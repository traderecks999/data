#!/usr/bin/env python3
"""
ASX Fundamentals Snapshot (Yahoo Finance) - hardened for GitHub Actions reliability

This patch focuses on:
- Avoiding 40+ minute "silent timeout" runs that end with 0.000 success_rate
- Capturing WHY fetches fail (http status / timeout / missing_modules)
- Seeding Yahoo cookies + crumb (helps on GitHub runner IPs)
- Endpoint fallback (query2 -> query1)
- Fail-fast after N consecutive failures with 0 successes

Outputs:
- asx/fundamentals_latest.{json,csv,xlsx}
- asx/fundamentals_cache.json (cursor + per-ticker cached deep fundamentals + telemetry)

Also enriches each ticker with:
- name, sector, industry, asset_type, universe_source (from asx/universe.csv/json)
- canonical price from asx/prices_latest.json and recomputes price-derived ratios for consistency
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pandas as pd


YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
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

QUOTE_FIELDS = [
    "symbol",
    "quoteType",
    "shortName",
    "longName",
    "currency",
    "exchange",
    "fullExchangeName",
    "marketState",
    "marketCap",
    "sharesOutstanding",
    "floatShares",
    "trailingPE",
    "forwardPE",
    "epsTrailingTwelveMonths",
    "epsForward",
    "dividendRate",
    "dividendYield",
    "beta",
    "bookValue",
    "priceToBook",
]

FIELD_MAP: List[Dict[str, str]] = [
    {"field": "freeCashflow", "used_for": "DCF (FCF-based)", "source": "quoteSummary:financialData"},
    {"field": "dividendRate", "used_for": "DDM; Dividend yield (with price)", "source": "quote/quoteSummary"},
    {"field": "totalRevenue", "used_for": "EPV; Screens", "source": "quoteSummary:financialData/statements"},
    {"field": "ebitda", "used_for": "EPV; EV/EBITDA", "source": "quoteSummary:financialData"},
    {"field": "bookValue", "used_for": "Residual income; Asset based", "source": "quote/quoteSummary"},
    {"field": "returnOnEquity", "used_for": "Residual income; Quality", "source": "quoteSummary:financialData"},
    {"field": "earningsGrowth", "used_for": "PEG; Growth assumptions", "source": "quoteSummary:financialData"},
    {"field": "marketCap", "used_for": "Size; EV bridge; Screens", "source": "quote/recomputed from prices_latest"},
    {"field": "trailingPE", "used_for": "Multiples; Screens", "source": "quote/recomputed from prices_latest"},
    {"field": "dividendYield", "used_for": "Dividend yield", "source": "quote/recomputed from prices_latest"},
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_lines(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


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


def chunks(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


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


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def parse_bool_str(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


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


async def seed_yahoo_session(client: httpx.AsyncClient) -> Tuple[Optional[str], str]:
    headers = {
        "User-Agent": client.headers.get("User-Agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    notes = []
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


@dataclass
class FetchResult:
    ok: bool
    http_status: Optional[int]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]


async def fetch_quote_batch(client: httpx.AsyncClient, symbols: List[str], timeout_s: float) -> List[Dict[str, Any]]:
    params = {"symbols": ",".join(symbols)}
    r = await client.get(YF_QUOTE_URL, params=params, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    return (data.get("quoteResponse") or {}).get("result") or []


async def fetch_qs_one(
    client: httpx.AsyncClient,
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

    last_err = None
    last_status = None

    for attempt in range(max_retries + 1):
        await asyncio.sleep(random.uniform(jitter_ms[0], jitter_ms[1]) / 1000.0)

        for base in YF_QS_URLS:
            url = base.format(symbol=symbol)
            try:
                r = await client.get(url, params=params_base, headers=headers, timeout=timeout_s)
                last_status = r.status_code

                if r.status_code == 404:
                    return FetchResult(False, 404, "not_found", None)

                if r.status_code in (401, 403, 429):
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

        await asyncio.sleep(min(8.0, 1.5 ** attempt))

    return FetchResult(False, last_status, last_err or "unknown_error", None)


def extract_quote_fields(q: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for f in QUOTE_FIELDS:
        row[f] = q.get(f)
    row["yahoo_symbol"] = q.get("symbol")
    row["quoteFetchedAtUtc"] = utc_now_iso()
    return row


def extract_qs_fields(qs_result: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    summary_detail = qs_result.get("summaryDetail") or {}
    default_stats = qs_result.get("defaultKeyStatistics") or {}
    financial_data = qs_result.get("financialData") or {}
    profile = qs_result.get("summaryProfile") or {}
    cal = qs_result.get("calendarEvents") or {}

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
    ]:
        out[k] = yf_raw(default_stats.get(k))

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

    out["sector_yahoo"] = profile.get("sector")
    out["industry_yahoo"] = profile.get("industry")
    out["website"] = profile.get("website")
    out["country"] = profile.get("country")
    out["fullTimeEmployees"] = yf_raw(profile.get("fullTimeEmployees"))
    out["longBusinessSummary"] = profile.get("longBusinessSummary")

    earnings = get_path(cal, ["earnings", "earningsDate"], default=None)
    if isinstance(earnings, list) and earnings:
        out["earningsDate"] = yf_raw(earnings[0])
    else:
        out["earningsDate"] = None

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

    eps_ttm = safe_float(row.get("epsTrailingTwelveMonths"))
    if eps_ttm is not None and eps_ttm != 0:
        row["trailingPE"] = price / eps_ttm

    eps_fwd = safe_float(row.get("epsForward"))
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
    out = []
    for i in range(min(n, len(symbols))):
        out.append(symbols[(cursor + i) % len(symbols)])
    return out


def advance_cursor(cache: Dict[str, Any], symbols_count: int, step: int) -> None:
    meta = cache.setdefault("_meta", {})
    cur = int(meta.get("cursor", 0))
    meta["cursor"] = (cur + step) % max(1, symbols_count)


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

    refresh_syms = choose_refresh_symbols(symbols, cache, args.mode, args.summary_per_run)

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }

    quote_results: Dict[str, Dict[str, Any]] = {}

    ok_count = 0
    fail_counts: Dict[str, int] = {}
    http_counts: Dict[str, int] = {}
    consecutive_fail = 0

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        crumb, seed_note = await seed_yahoo_session(client)
        meta["yahooSeedNote"] = seed_note
        meta["yahooCrumbPresent"] = bool(crumb)

        # Tier A quote
        try:
            for batch in chunks(symbols, args.quote_chunk):
                lst = await fetch_quote_batch(client, batch, timeout_s=args.quote_timeout_s)
                for q in lst:
                    sym = q.get("symbol")
                    if sym:
                        quote_results[sym] = q
        except Exception as e:
            meta["tierAQuoteError"] = f"{type(e).__name__}:{e}"

        # Tier B quoteSummary
        sem = asyncio.Semaphore(args.concurrency)

        async def run_one(sym: str) -> Tuple[str, FetchResult]:
            async with sem:
                fr = await fetch_qs_one(
                    client,
                    sym,
                    modules,
                    crumb=crumb,
                    timeout_s=args.qs_timeout_s,
                    max_retries=args.max_retries,
                    jitter_ms=(args.jitter_min_ms, args.jitter_max_ms),
                )
                return sym, fr

        tasks = [run_one(sym) for sym in refresh_syms]
        results = await asyncio.gather(*tasks)

        for sym, fr in results:
            bysym = cache.setdefault("bySymbol", {}).setdefault(sym, {})
            bysym["fundamentalsFetchedAtUtc"] = utc_now_iso()
            bysym["fundamentalsFetchHttpStatus"] = fr.http_status

            if fr.ok:
                bysym["fundamentalsFetchStatus"] = "ok"
                bysym["fundamentalsFetchError"] = None
                bysym["summary"] = extract_qs_fields(fr.payload or {})
                ok_count += 1
                consecutive_fail = 0
            else:
                status = fr.error or "error"
                bysym["fundamentalsFetchStatus"] = "http_error" if status.startswith("http_") else status
                bysym["fundamentalsFetchError"] = status
                consecutive_fail += 1
                fail_counts[status] = fail_counts.get(status, 0) + 1
                if fr.http_status is not None:
                    http_counts[str(fr.http_status)] = http_counts.get(str(fr.http_status), 0) + 1

            if ok_count == 0 and consecutive_fail >= args.fail_fast_after:
                meta["failFastTriggered"] = True
                meta["failFastReason"] = f"{consecutive_fail} consecutive failures with 0 successes"
                break

    requested = len(refresh_syms)
    success_rate = (ok_count / requested) if requested else 1.0

    rows: List[Dict[str, Any]] = []
    for sym in symbols:
        row: Dict[str, Any] = {"yahoo_symbol": sym}

        u = universe.get(sym)
        if u:
            row.update(u)

        q = quote_results.get(sym)
        if q:
            row.update(extract_quote_fields(q))
        else:
            row["quoteFetchedAtUtc"] = utc_now_iso()

        bysym = (cache.get("bySymbol") or {}).get(sym) or {}
        row["fundamentalsFetchedAtUtc"] = bysym.get("fundamentalsFetchedAtUtc")
        row["fundamentalsFetchStatus"] = bysym.get("fundamentalsFetchStatus")
        row["fundamentalsFetchHttpStatus"] = bysym.get("fundamentalsFetchHttpStatus")
        row["fundamentalsFetchError"] = bysym.get("fundamentalsFetchError")

        summary = bysym.get("summary") or {}
        row.update(summary)

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

    json_path = os.path.join(asx_dir, "fundamentals_latest.json")
    payload = {
        "asOfUtc": utc_now_iso(),
        "meta": {
            "mode": args.mode,
            "summaryPerRun": args.summary_per_run,
            "refreshedTickers": requested,
            "successCount": ok_count,
            "successRate": round(success_rate, 6),
            "minSuccessRate": args.min_success_rate,
            "failCounts": fail_counts,
            "httpCounts": http_counts,
            "cursorBefore": int(meta.get("cursor", 0)),
            "cursorAdvanced": False,
        },
        "fieldMap": FIELD_MAP,
        "data": rows,
    }
    save_json(json_path, payload)

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

    # cursor advance decision
    should_advance = True
    if args.mode != "full" and requested > 0 and success_rate < args.min_success_rate:
        should_advance = False

    if should_advance and args.mode != "full":
        before = int(meta.get("cursor", 0))
        advance_cursor(cache, symbols_count=len(symbols), step=requested)
        meta["cursorAfter"] = int(meta.get("cursor", 0))
        payload["meta"]["cursorBefore"] = before
        payload["meta"]["cursorAfter"] = meta["cursorAfter"]
        payload["meta"]["cursorAdvanced"] = True
        save_json(json_path, payload)
    else:
        payload["meta"]["cursorAfter"] = int(meta.get("cursor", 0))
        save_json(json_path, payload)

    save_json(cache_path, cache)

    print(f"Deep fundamentals ok={ok_count}/{requested} success_rate={success_rate:.3f} (min={args.min_success_rate:.3f})")
    if http_counts:
        print(f"HTTP status counts: {http_counts}")
    if fail_counts:
        print(f"Failure counts: {fail_counts}")

    if not should_advance:
        msg = (
            f"Deep fundamentals success_rate={success_rate:.3f} below min_success_rate={args.min_success_rate:.3f}. "
            f"Not advancing cursor."
        )
        if args.fail_on_low_success:
            print(msg + " Failing run so you notice.", file=sys.stderr)
            return 1
        print(msg + " Continuing (fail_on_low_success=false).")
        return 0

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build ASX fundamentals snapshot (Yahoo Finance).")
    p.add_argument("--repo-root", default=".", help="Repo root path")
    p.add_argument("--mode", default="rotate", choices=["rotate", "full"], help="rotate or full")

    p.add_argument("--summary-per-run", "--summary_per_run", dest="summary_per_run", type=int, default=220)
    p.add_argument("--quote-chunk", "--quote_chunk", dest="quote_chunk", type=int, default=120)
    p.add_argument("--concurrency", type=int, default=4)

    p.add_argument("--min-success-rate", "--min_success_rate", dest="min_success_rate", type=float, default=0.30)
    p.add_argument("--fail-on-low-success", "--fail_on_low_success", dest="fail_on_low_success", default="true")
    p.add_argument("--include-statements", "--include_statements", dest="include_statements", action="store_true")

    p.add_argument("--qs-timeout-s", "--qs_timeout_s", dest="qs_timeout_s", type=float, default=18.0)
    p.add_argument("--quote-timeout-s", "--quote_timeout_s", dest="quote_timeout_s", type=float, default=18.0)
    p.add_argument("--max-retries", "--max_retries", dest="max_retries", type=int, default=1)
    p.add_argument("--jitter-min-ms", "--jitter_min_ms", dest="jitter_min_ms", type=int, default=60)
    p.add_argument("--jitter-max-ms", "--jitter_max_ms", dest="jitter_max_ms", type=int, default=220)
    p.add_argument("--fail-fast-after", "--fail_fast_after", dest="fail_fast_after", type=int, default=25)
    return p


def parse_args() -> argparse.Namespace:
    p = build_arg_parser()
    a = p.parse_args()
    a.fail_on_low_success = parse_bool_str(a.fail_on_low_success)
    return a


if __name__ == "__main__":
    args = parse_args()
    rc = asyncio.run(main_async(args))
    raise SystemExit(rc)
