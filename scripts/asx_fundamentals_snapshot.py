#!/usr/bin/env python3
"""
ASX Fundamentals Snapshot (Yahoo Finance)

This version is engineered specifically to stop the "0% success + 429 rate-limit" death spiral on GitHub Actions.

Key design changes vs earlier iterations:
- Tier B (quoteSummary) is the ONLY mandatory network layer.
  Tier A (bulk quote) is optional and OFF by default to avoid burning Yahoo rate limits.
- Global rate limiter for quoteSummary requests (default 8 req/min). Concurrency >1 is allowed but rate limiter
  enforces spacing so requests don't burst.
- 429 handling: honours Retry-After when present; otherwise cools down with exponential backoff and retries.
- Accurate success_rate denominator: based on attempted requests, not planned slice size.
- Cursor only advances when success_rate >= min_success_rate, and only by the number attempted in this run.
- Writes rich per-ticker telemetry so nulls are explainable.

Outputs:
- asx/fundamentals_latest.json
- asx/fundamentals_latest.csv
- asx/fundamentals_latest.xlsx (tabs: fundamentals, field_map)
- asx/fundamentals_cache.json (cursor + per-ticker cached deep fundamentals + telemetry)

Enrichment:
- name, sector, industry, asset_type, universe_source (from asx/universe.csv)
- canonical price + price timestamp from asx/prices_latest.json; recomputes price-derived fields for consistency
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pandas as pd


# ----------------- Yahoo endpoints -----------------
YF_QS_URLS = [
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}",
]
COOKIE_SEED_URLS = [
    "https://finance.yahoo.com/",
    "https://finance.yahoo.com/quote/AAPL",
]
CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"

DEFAULT_QS_MODULES = [
    "summaryDetail",
    "defaultKeyStatistics",
    "financialData",
    "calendarEvents",
    "summaryProfile",
    "recommendationTrend",
    "upgradeDowngradeHistory",
    "price",
]
STATEMENT_MODULES = [
    "incomeStatementHistory",
    "balanceSheetHistory",
    "cashflowStatementHistory",
]

FIELD_MAP: List[Dict[str, str]] = [
    {"field": "freeCashflow", "used_for": "DCF (FCF-based)", "source": "quoteSummary:financialData"},
    {"field": "operatingCashflow", "used_for": "Cashflow quality screens", "source": "quoteSummary:financialData"},
    {"field": "ebitda", "used_for": "EV/EBITDA; EPV", "source": "quoteSummary:financialData"},
    {"field": "totalDebt", "used_for": "Balance sheet risk; EV", "source": "quoteSummary:financialData"},
    {"field": "totalCash", "used_for": "Net debt; EV", "source": "quoteSummary:financialData"},
    {"field": "dividendRate", "used_for": "DDM; yield (with price)", "source": "quoteSummary:summaryDetail"},
    {"field": "bookValue", "used_for": "P/B; asset-based", "source": "quoteSummary:defaultKeyStatistics"},
    {"field": "earningsGrowth", "used_for": "PEG; growth assumptions", "source": "quoteSummary:financialData"},
    {"field": "revenueGrowth", "used_for": "Growth assumptions", "source": "quoteSummary:financialData"},
    {"field": "returnOnEquity", "used_for": "Residual income; quality", "source": "quoteSummary:financialData"},
    {"field": "sharesOutstanding", "used_for": "Market cap recompute", "source": "quoteSummary:defaultKeyStatistics"},
    {"field": "marketCap", "used_for": "Size; EV bridge", "source": "recomputed from prices_latest + shares"},
    {"field": "trailingPE", "used_for": "Multiples", "source": "recomputed from prices_latest + EPS"},
    {"field": "dividendYield", "used_for": "Yield", "source": "recomputed from prices_latest + dividendRate"},
]


# ----------------- Helpers -----------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, payload: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def read_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def yf_raw(v: Any) -> Any:
    if isinstance(v, dict) and "raw" in v:
        return v.get("raw")
    return v


def get_path(d: Any, path: List[Any], default=None):
    cur = d
    for key in path:
        if cur is None:
            return default
        if isinstance(key, int):
            if isinstance(cur, list) and 0 <= key < len(cur):
                cur = cur[key]
            else:
                return default
        else:
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return default
    return cur if cur is not None else default


def parse_bool_str(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


# ----------------- Universe / tickers / prices -----------------
def load_universe(repo_root: str) -> Dict[str, Dict[str, Any]]:
    uni_csv = os.path.join(repo_root, "asx", "universe.csv")
    uni_json = os.path.join(repo_root, "asx", "universe_latest.json")

    df = None
    if os.path.exists(uni_csv):
        df = pd.read_csv(uni_csv)
    elif os.path.exists(uni_json):
        data = load_json(uni_json)
        if isinstance(data, dict) and "data" in data:
            df = pd.DataFrame(data["data"])
        elif isinstance(data, list):
            df = pd.DataFrame(data)

    if df is None or df.empty:
        return {}

    for col in ["yahoo_symbol", "name", "sector", "industry", "asset_type", "source", "code"]:
        if col not in df.columns:
            df[col] = None

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        sym = str(r.get("yahoo_symbol") or "").strip()
        if not sym:
            continue
        out[sym] = {
            "code": r.get("code"),
            "name": r.get("name"),
            "sector": r.get("sector"),
            "industry": r.get("industry"),
            "asset_type": r.get("asset_type"),
            "universe_source": r.get("source"),
        }
    return out


def read_tickers(repo_root: str) -> List[str]:
    tickers_txt = os.path.join(repo_root, "asx", "tickers_asx.txt")
    syms = read_lines(tickers_txt)
    if syms:
        return sorted(set(syms))
    uni = load_universe(repo_root)
    if uni:
        return sorted(uni.keys())
    raise FileNotFoundError("Could not find asx/tickers_asx.txt and universe is missing/unreadable")


def load_prices_latest(repo_root: str) -> Dict[str, Dict[str, Any]]:
    p = os.path.join(repo_root, "asx", "prices_latest.json")
    data = load_json(p)
    if data is None:
        return {}
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        return data["data"]
    if isinstance(data, dict):
        return data
    return {}


# ----------------- Rate limiter -----------------
class AsyncRateLimiter:
    """
    Simple global rate limiter: enforces a minimum spacing between acquisitions.
    Good enough to avoid 429 bursts on GitHub runners.
    """
    def __init__(self, rate_per_min: float):
        self.rate_per_min = max(0.1, float(rate_per_min))
        self.min_interval = 60.0 / self.rate_per_min
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0  # monotonic time

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
            self._next_allowed = max(self._next_allowed, time.monotonic()) + self.min_interval


# ----------------- Yahoo session seeding -----------------
async def seed_yahoo_session(client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    notes = []
    headers = {
        "User-Agent": client.headers.get("User-Agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    for url in COOKIE_SEED_URLS:
        try:
            await client.get(url, headers=headers, timeout=20)
            notes.append(f"seed_ok:{url}")
        except Exception as e:
            notes.append(f"seed_fail:{url}:{type(e).__name__}")

    crumb = None
    try:
        r = await client.get(
            CRUMB_URL,
            headers={
                "User-Agent": client.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "*/*",
                "Referer": "https://finance.yahoo.com/",
            },
            timeout=20,
        )
        if r.status_code == 200:
            c = (r.text or "").strip()
            if c and "{" not in c and "<" not in c:
                crumb = c
                notes.append("crumb_ok")
            else:
                notes.append("crumb_bad_text")
        else:
            notes.append(f"crumb_http:{r.status_code}")
    except Exception as e:
        notes.append(f"crumb_fail:{type(e).__name__}")

    return crumb, ";".join(notes)


# ----------------- Networking -----------------
@dataclass
class FetchResult:
    ok: bool
    http_status: Optional[int]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]
    retry_after_s: Optional[int] = None


async def fetch_qs_one(
    client: httpx.AsyncClient,
    limiter: AsyncRateLimiter,
    symbol: str,
    modules: List[str],
    crumb: Optional[str],
    timeout_s: float,
    max_retries: int,
    jitter_ms: Tuple[int, int],
) -> FetchResult:
    params_base = {"modules": ",".join(modules), "ssl": "true"}
    if crumb:
        params_base["crumb"] = crumb

    headers = {
        "User-Agent": client.headers.get("User-Agent", "Mozilla/5.0"),
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://finance.yahoo.com/",
        "Origin": "https://finance.yahoo.com",
        "Connection": "keep-alive",
    }

    last_status = None
    last_err = None
    retry_after = None

    for attempt in range(max_retries + 1):
        # global rate limit spacing
        await limiter.acquire()
        # jitter to avoid lockstep
        await asyncio.sleep(random.uniform(jitter_ms[0], jitter_ms[1]) / 1000.0)

        for base in YF_QS_URLS:
            url = base.format(symbol=symbol)
            try:
                r = await client.get(url, params=params_base, headers=headers, timeout=timeout_s)
                last_status = r.status_code

                if r.status_code == 404:
                    return FetchResult(False, 404, "not_found", None)

                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    retry_after = int(ra) if ra and ra.isdigit() else None
                    last_err = "http_429"
                    continue

                if r.status_code in (401, 403):
                    last_err = f"http_{r.status_code}"
                    continue

                r.raise_for_status()
                data = r.json()
                qs = data.get("quoteSummary") or {}
                res = qs.get("result") or []
                if not res:
                    return FetchResult(False, r.status_code, "missing_modules", None)
                return FetchResult(True, r.status_code, None, res[0])

            except httpx.TimeoutException:
                last_err = "timeout"
                last_status = None
            except Exception as e:
                last_err = type(e).__name__

        # Backoff (especially for 429)
        if last_err == "http_429":
            # honour Retry-After if provided, else exponential with cap
            wait_s = retry_after if retry_after is not None else min(120, 10 * (attempt + 1))
        else:
            wait_s = min(30, 3 * (attempt + 1))
        await asyncio.sleep(wait_s)

    return FetchResult(False, last_status, last_err or "unknown_error", None, retry_after_s=retry_after)


# ----------------- Extraction -----------------
def extract_qs_fields(qs_result: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    summary_detail = qs_result.get("summaryDetail") or {}
    default_stats = qs_result.get("defaultKeyStatistics") or {}
    financial_data = qs_result.get("financialData") or {}
    profile = qs_result.get("summaryProfile") or {}
    cal = qs_result.get("calendarEvents") or {}
    price_mod = qs_result.get("price") or {}

    # summaryDetail
    for k in [
        "payoutRatio",
        "fiveYearAvgDividendYield",
        "exDividendDate",
        "trailingAnnualDividendRate",
        "trailingAnnualDividendYield",
        "dividendRate",
        "dividendYield",
        "beta",
        "marketCap",
    ]:
        out[k] = yf_raw(summary_detail.get(k))

    # defaultKeyStatistics
    for k in [
        "enterpriseValue",
        "enterpriseToEbitda",
        "pegRatio",
        "sharesOutstanding",
        "floatShares",
        "heldPercentInsiders",
        "heldPercentInstitutions",
        "shortPercentOfFloat",
        "shortRatio",
        "sharesShort",
        "sharesShortPriorMonth",
        "dateShortInterest",
        "bookValue",
        "priceToBook",
        "trailingEps",
        "forwardEps",
    ]:
        out[k] = yf_raw(default_stats.get(k))

    # financialData
    for k in [
        "freeCashflow",
        "operatingCashflow",
        "ebitda",
        "totalCash",
        "totalDebt",
        "debtToEquity",
        "currentRatio",
        "quickRatio",
        "grossMargins",
        "operatingMargins",
        "profitMargins",
        "returnOnAssets",
        "returnOnEquity",
        "revenueGrowth",
        "earningsGrowth",
        "totalRevenue",
        "targetMeanPrice",
        "targetHighPrice",
        "targetLowPrice",
        "recommendationMean",
        "recommendationKey",
        "numberOfAnalystOpinions",
    ]:
        out[k] = yf_raw(financial_data.get(k))

    # profile extras (Yahoo non-GICS)
    out["sector_yahoo"] = profile.get("sector")
    out["industry_yahoo"] = profile.get("industry")
    out["website"] = profile.get("website")
    out["country"] = profile.get("country")
    out["fullTimeEmployees"] = yf_raw(profile.get("fullTimeEmployees"))
    out["longBusinessSummary"] = profile.get("longBusinessSummary")

    # price module extras (often includes currency/market info)
    out["currency"] = price_mod.get("currency")
    out["exchangeName"] = price_mod.get("exchangeName")
    out["quoteType"] = price_mod.get("quoteType")

    # earnings date
    earnings = get_path(cal, ["earnings", "earningsDate"], default=None)
    if isinstance(earnings, list) and earnings:
        out["earningsDate"] = yf_raw(earnings[0])
    else:
        out["earningsDate"] = None

    # derived
    cash = safe_float(out.get("totalCash"))
    debt = safe_float(out.get("totalDebt"))
    out["netDebt"] = (debt - cash) if (cash is not None and debt is not None) else None

    return out


def recompute_price_derived_fields(row: Dict[str, Any], price: Optional[float]) -> None:
    if price is None:
        return

    row["regularMarketPrice"] = price

    shares = safe_float(row.get("sharesOutstanding"))
    if shares is not None:
        row["marketCap"] = shares * price

    div_rate = safe_float(row.get("dividendRate"))
    if div_rate is not None and price != 0:
        row["dividendYield"] = div_rate / price

    # EPS values can come from qs trailingEps/forwardEps
    eps_ttm = safe_float(row.get("trailingEps")) or safe_float(row.get("epsTrailingTwelveMonths"))
    if eps_ttm is not None and eps_ttm != 0:
        row["trailingPE"] = price / eps_ttm

    eps_fwd = safe_float(row.get("forwardEps")) or safe_float(row.get("epsForward"))
    if eps_fwd is not None and eps_fwd != 0:
        row["forwardPE"] = price / eps_fwd

    book = safe_float(row.get("bookValue"))
    if book is not None and book != 0:
        row["priceToBook"] = price / book

    total_debt = safe_float(row.get("totalDebt"))
    total_cash = safe_float(row.get("totalCash"))
    mcap = safe_float(row.get("marketCap"))
    if mcap is not None and total_debt is not None and total_cash is not None:
        ev = mcap + total_debt - total_cash
        row["enterpriseValue"] = ev
        ebitda = safe_float(row.get("ebitda"))
        if ebitda is not None and ebitda != 0:
            row["enterpriseToEbitda"] = ev / ebitda


# ----------------- Cache -----------------
def load_cache(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"_meta": {"schema_version": 2, "cursor": 0}, "bySymbol": {}}
    data = load_json(path)
    if not isinstance(data, dict):
        return {"_meta": {"schema_version": 2, "cursor": 0}, "bySymbol": {}}
    data.setdefault("_meta", {"schema_version": 2, "cursor": 0})
    data.setdefault("bySymbol", {})
    if "cursor" not in data["_meta"]:
        data["_meta"]["cursor"] = 0
    data["_meta"]["schema_version"] = 2
    return data


def choose_refresh_symbols(symbols: List[str], cache: Dict[str, Any], mode: str, n: int) -> List[str]:
    symbols = sorted(symbols)
    if mode == "full":
        return symbols
    cursor = int((cache.get("_meta") or {}).get("cursor", 0))
    n = max(0, min(n, len(symbols)))
    return [symbols[(cursor + i) % len(symbols)] for i in range(n)]


def advance_cursor(cache: Dict[str, Any], symbols_count: int, step: int) -> None:
    meta = cache.setdefault("_meta", {})
    cur = int(meta.get("cursor", 0))
    meta["cursor"] = (cur + step) % max(1, symbols_count)


# ----------------- Main -----------------
async def main_async(args: argparse.Namespace) -> int:
    repo_root = args.repo_root
    asx_dir = os.path.join(repo_root, "asx")
    os.makedirs(asx_dir, exist_ok=True)

    symbols = read_tickers(repo_root)
    universe = load_universe(repo_root)
    prices_latest = load_prices_latest(repo_root)

    cache_path = os.path.join(asx_dir, "fundamentals_cache.json")
    cache = load_cache(cache_path)
    meta = cache["_meta"]
    meta["lastRunUtc"] = utc_now_iso()

    modules = list(DEFAULT_QS_MODULES)
    if args.include_statements:
        modules += STATEMENT_MODULES

    planned_refresh = choose_refresh_symbols(symbols, cache, args.mode, args.summary_per_run)

    # IMPORTANT: Reduce burn. Only attempt deep fundamentals; no bulk quote by default.
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    limiter = AsyncRateLimiter(args.qs_rate_per_min)

    ok_count = 0
    attempted = 0
    fail_counts: Dict[str, int] = {}
    http_counts: Dict[str, int] = {}

    start = time.monotonic()
    max_run_s = args.max_run_minutes * 60

    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=None) as client:
        crumb, seed_note = await seed_yahoo_session(client)
        meta["yahooSeedNote"] = seed_note
        meta["yahooCrumbPresent"] = bool(crumb)

        # Work queue with bounded concurrency
        q: asyncio.Queue[str] = asyncio.Queue()
        for s in planned_refresh:
            q.put_nowait(s)

        sem = asyncio.Semaphore(args.concurrency)

        async def worker():
            nonlocal ok_count, attempted
            while True:
                if time.monotonic() - start > max_run_s:
                    return
                try:
                    sym = q.get_nowait()
                except asyncio.QueueEmpty:
                    return

                async with sem:
                    fr = await fetch_qs_one(
                        client=client,
                        limiter=limiter,
                        symbol=sym,
                        modules=modules,
                        crumb=crumb,
                        timeout_s=args.qs_timeout_s,
                        max_retries=args.max_retries,
                        jitter_ms=(args.jitter_min_ms, args.jitter_max_ms),
                    )

                attempted += 1
                bysym = cache.setdefault("bySymbol", {}).setdefault(sym, {})
                bysym["fundamentalsFetchedAtUtc"] = utc_now_iso()
                bysym["fundamentalsFetchHttpStatus"] = fr.http_status

                if fr.ok:
                    ok_count += 1
                    bysym["fundamentalsFetchStatus"] = "ok"
                    bysym["fundamentalsFetchError"] = None
                    bysym["summary"] = extract_qs_fields(fr.payload or {})
                else:
                    status = fr.error or "error"
                    bysym["fundamentalsFetchStatus"] = "http_error" if status.startswith("http_") else status
                    bysym["fundamentalsFetchError"] = status
                    fail_counts[status] = fail_counts.get(status, 0) + 1
                    if fr.http_status is not None:
                        http_counts[str(fr.http_status)] = http_counts.get(str(fr.http_status), 0) + 1

                # progress every 20 attempts
                if attempted % 20 == 0:
                    elapsed = time.monotonic() - start
                    print(f"[progress] attempted={attempted} ok={ok_count} elapsed={elapsed:.1f}s")

                q.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(max(1, args.concurrency))]
        await asyncio.gather(*workers)

    success_rate = (ok_count / attempted) if attempted else 0.0

    # Output rows by merging: universe + cached summary + canonical price recompute
    rows: List[Dict[str, Any]] = []
    bysym_all = cache.get("bySymbol") or {}

    for sym in symbols:
        row: Dict[str, Any] = {"yahoo_symbol": sym}

        u = universe.get(sym)
        if u:
            row.update(u)

        c = bysym_all.get(sym) or {}
        row["fundamentalsFetchedAtUtc"] = c.get("fundamentalsFetchedAtUtc")
        row["fundamentalsFetchStatus"] = c.get("fundamentalsFetchStatus")
        row["fundamentalsFetchHttpStatus"] = c.get("fundamentalsFetchHttpStatus")
        row["fundamentalsFetchError"] = c.get("fundamentalsFetchError")
        row.update(c.get("summary") or {})

        pinfo = prices_latest.get(sym) or {}
        price = pinfo.get("price")
        price_fetched = pinfo.get("fetchedAtUtc") or pinfo.get("fetched_at_utc") or pinfo.get("fetched_at")
        try:
            price_val = float(price) if price is not None else None
        except Exception:
            price_val = None

        row["priceSource"] = "prices_latest.json" if price_val is not None else None
        row["priceFetchedAtUtc"] = price_fetched

        recompute_price_derived_fields(row, price_val)

        rows.append(row)

    df = pd.DataFrame(rows)

    # JSON
    json_path = os.path.join(asx_dir, "fundamentals_latest.json")
    payload = {
        "asOfUtc": utc_now_iso(),
        "meta": {
            "mode": args.mode,
            "summaryPerRunPlanned": len(planned_refresh),
            "attempted": attempted,
            "successCount": ok_count,
            "successRate": round(success_rate, 6),
            "minSuccessRate": args.min_success_rate,
            "qsRatePerMin": args.qs_rate_per_min,
            "failCounts": fail_counts,
            "httpCounts": http_counts,
            "cursorBefore": int(meta.get("cursor", 0)),
        },
        "fieldMap": FIELD_MAP,
        "data": rows,
    }
    save_json(json_path, payload)

    # CSV/XLSX (drop long summary text for portability)
    df_csv = df.copy()
    if "longBusinessSummary" in df_csv.columns:
        df_csv = df_csv.drop(columns=["longBusinessSummary"])

    csv_path = os.path.join(asx_dir, "fundamentals_latest.csv")
    df_csv.to_csv(csv_path, index=False)

    xlsx_path = os.path.join(asx_dir, "fundamentals_latest.xlsx")
    df_map = pd.DataFrame(FIELD_MAP)
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        df_csv.to_excel(w, sheet_name="fundamentals", index=False)
        df_map.to_excel(w, sheet_name="field_map", index=False)

    # Cursor advance / failure policy
    should_advance = True
    if args.mode != "full" and attempted > 0 and success_rate < args.min_success_rate:
        should_advance = False

    if should_advance and args.mode != "full":
        before = int(meta.get("cursor", 0))
        advance_cursor(cache, symbols_count=len(symbols), step=attempted)
        meta["cursorAfter"] = int(meta.get("cursor", 0))
        meta["cursorAdvanced"] = True
        meta["cursorBefore"] = before
    else:
        meta["cursorAfter"] = int(meta.get("cursor", 0))
        meta["cursorAdvanced"] = False

    save_json(cache_path, cache)

    # Log summary
    print(f"Deep fundamentals ok={ok_count}/{attempted} success_rate={success_rate:.3f} (min={args.min_success_rate:.3f})")
    if http_counts:
        print(f"HTTP status counts: {http_counts}")
    if fail_counts:
        print(f"Failure counts: {fail_counts}")

    if attempted == 0:
        print("No tickers attempted (time budget too low or empty universe).", file=sys.stderr)
        return 1

    if not should_advance:
        msg = (
            f"Deep fundamentals success_rate={success_rate:.3f} below min_success_rate={args.min_success_rate:.3f}. "
            f"Not advancing cursor."
        )
        if args.fail_on_low_success:
            print(msg + " Failing run so you notice.", file=sys.stderr)
            return 1
        else:
            print(msg + " Continuing (fail_on_low_success=false).")
            return 0

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build ASX fundamentals snapshot (Yahoo Finance).")
    p.add_argument("--repo-root", default=".", help="Repo root path")
    p.add_argument("--mode", default="rotate", choices=["rotate", "full"], help="rotate or full")

    p.add_argument("--summary-per-run", "--summary_per_run", dest="summary_per_run", type=int, default=220)
    p.add_argument("--concurrency", type=int, default=1)

    p.add_argument("--qs-rate-per-min", "--qs_rate_per_min", dest="qs_rate_per_min", type=float, default=8.0,
                   help="Global quoteSummary request rate limit (req/min). Lower reduces 429s.")
    p.add_argument("--min-success-rate", "--min_success_rate", dest="min_success_rate", type=float, default=0.30)
    p.add_argument("--fail-on-low-success", "--fail_on_low_success", dest="fail_on_low_success", default="true")

    p.add_argument("--include-statements", "--include_statements", dest="include_statements", action="store_true")

    # network tuning
    p.add_argument("--qs-timeout-s", "--qs_timeout_s", dest="qs_timeout_s", type=float, default=20.0)
    p.add_argument("--max-retries", "--max_retries", dest="max_retries", type=int, default=4)
    p.add_argument("--jitter-min-ms", "--jitter_min_ms", dest="jitter_min_ms", type=int, default=250)
    p.add_argument("--jitter-max-ms", "--jitter_max_ms", dest="jitter_max_ms", type=int, default=1200)

    # safety budget
    p.add_argument("--max-run-minutes", "--max_run_minutes", dest="max_run_minutes", type=int, default=80,
                   help="Stop attempting new tickers after this many minutes.")

    # workflow compatibility (accepted but unused in this hardened build)
    p.add_argument("--quote-chunk", "--quote_chunk", dest="quote_chunk", type=int, default=120)

    return p


def parse_args() -> argparse.Namespace:
    p = build_arg_parser()
    a = p.parse_args()
    a.fail_on_low_success = parse_bool_str(a.fail_on_low_success)
    return a


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(asyncio.run(main_async(args)))
