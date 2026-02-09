#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from common import write_json, utc_now_iso

# -----------------------------------------------------------------------------
# Yahoo endpoints (unofficial; may rate-limit)
# -----------------------------------------------------------------------------
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YF_QS_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YF_CRUMB_URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"
YF_COOKIE_SEED = "https://fc.yahoo.com"

DEFAULT_TICKERS_FILE = "asx/tickers_asx.txt"
DEFAULT_UNIVERSE_CSV = "asx/universe.csv"
DEFAULT_PRICES_LATEST = "asx/prices_latest.json"

DEFAULT_OUT_JSON = "asx/fundamentals_latest.json"
DEFAULT_OUT_CSV = "asx/fundamentals_latest.csv"
DEFAULT_OUT_XLSX = "asx/fundamentals_latest.xlsx"
DEFAULT_CACHE = "asx/fundamentals_cache.json"

DEFAULT_MODE = "rotate"  # rotate | full
DEFAULT_SUMMARY_PER_RUN = 220
DEFAULT_QUOTE_CHUNK = 120
DEFAULT_CONCURRENCY = 4
DEFAULT_INCLUDE_STATEMENTS = False

# Guardrails
DEFAULT_MIN_SUCCESS_RATE = 0.30  # if deep fetch ok rate < this, cursor won't advance (and you can choose to fail)
DEFAULT_FAIL_ON_LOW_SUCCESS = True

# quoteSummary modules we pull in one hit per symbol
QS_MODULES = [
    "summaryDetail",
    "defaultKeyStatistics",
    "financialData",
    "summaryProfile",
    "price",
    "calendarEvents",
]

# Optional annual statements (heavier payloads)
STATEMENT_MODULES = [
    "incomeStatementHistory",
    "balanceSheetHistory",
    "cashflowStatementHistory",
]

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None

def yf_raw(v: Any) -> Any:
    """Yahoo fields often look like {"raw": 123, "fmt": "123"}."""
    if isinstance(v, dict):
        if "raw" in v:
            return v.get("raw")
        if "fmt" in v and len(v) == 1:
            return v.get("fmt")
    return v

def read_json(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))

def load_universe(path: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by yahoo_symbol (e.g. BHP.AX).
    Expects columns at least: yahoo_symbol, name, sector, industry, asset_type
    """
    p = Path(path)
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        ys = str(r.get("yahoo_symbol") or "").strip()
        if not ys:
            continue
        out[ys] = {
            "name": (r.get("name") if pd.notna(r.get("name")) else None),
            "sector": (r.get("sector") if pd.notna(r.get("sector")) else None),
            "industry": (r.get("industry") if pd.notna(r.get("industry")) else None),
            "asset_type": (r.get("asset_type") if pd.notna(r.get("asset_type")) else None),
            "source": (r.get("source") if pd.notna(r.get("source")) else None),
            "code": (r.get("code") if pd.notna(r.get("code")) else None),
        }
    return out

def load_prices_latest(path: str) -> Dict[str, Dict[str, Any]]:
    d = read_json(path)
    return d.get("prices") or {}

def read_tickers(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tickers file not found: {path}")
    tickers: List[str] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        tickers.append(t)
    # de-dupe preserving order
    seen = set()
    out: List[str] = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def build_yahoo_session() -> Tuple[requests.Session, Optional[str]]:
    sess = requests.Session()
    # A normal-ish browser UA helps reduce 403s.
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-AU,en;q=0.9",
        "Connection": "keep-alive",
    })

    crumb: Optional[str] = None
    try:
        # Seed cookies
        sess.get(YF_COOKIE_SEED, timeout=20, allow_redirects=True)
        # Try to get a crumb (some endpoints ignore it; some require it)
        r = sess.get(YF_CRUMB_URL, timeout=20)
        if r.status_code == 200:
            c = (r.text or "").strip()
            if c and len(c) < 200 and "html" not in c.lower():
                crumb = c
    except Exception:
        crumb = None
    return sess, crumb

def request_json(
    sess: requests.Session,
    url: str,
    params: Dict[str, Any],
    *,
    timeout: int = 30,
    retries: int = 3,
    jitter_ms: Tuple[int, int] = (50, 250),
    crumb: Optional[str] = None,
) -> Tuple[Optional[dict], int, Optional[str]]:
    """
    Returns (json, http_status, error_string)
    Adds a tiny jitter to avoid thundering herds in concurrency.
    """
    if crumb is not None and "crumb" not in params:
        params = dict(params)
        params["crumb"] = crumb

    last_err: Optional[str] = None
    last_status: int = 0

    for attempt in range(retries):
        # Jitter
        time.sleep(random.uniform(jitter_ms[0], jitter_ms[1]) / 1000.0)

        try:
            r = sess.get(url, params=params, timeout=timeout)
            last_status = r.status_code

            # Rate-limit/backoff
            if r.status_code in (429, 503, 502, 500):
                last_err = f"http_{r.status_code}"
                time.sleep(1.5 + attempt * 2.0 + random.random())
                continue

            if r.status_code == 404:
                return None, 404, "not_found"

            if r.status_code == 401 or r.status_code == 403:
                # Often fixed by refreshing cookies/crumb; try once per attempt
                last_err = f"http_{r.status_code}"
                try:
                    sess.get(YF_COOKIE_SEED, timeout=20, allow_redirects=True)
                except Exception:
                    pass
                time.sleep(1.0 + attempt * 1.5 + random.random())
                continue

            r.raise_for_status()
            return r.json(), r.status_code, None
        except Exception as e:
            last_err = type(e).__name__
            time.sleep(1.0 + attempt * 1.5 + random.random())

    return None, last_status, last_err

# -----------------------------------------------------------------------------
# Fetches
# -----------------------------------------------------------------------------
def fetch_bulk_quote(sess: requests.Session, symbols: List[str], crumb: Optional[str], quote_chunk: int) -> Dict[str, Dict[str, Any]]:
    """
    Uses v7 quote endpoint which is batch-friendly.
    Returns dict keyed by symbol.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(symbols), quote_chunk):
        chunk = symbols[i:i + quote_chunk]
        params = {"symbols": ",".join(chunk), "region": "AU", "lang": "en-AU"}
        js, status, err = request_json(sess, YF_QUOTE_URL, params, timeout=30, retries=3, crumb=crumb)
        if not js:
            # If a whole chunk fails, we keep going; per-symbol status will reflect missing quote fields
            continue
        res = (js.get("quoteResponse") or {}).get("result") or []
        for q in res:
            sym = q.get("symbol")
            if sym:
                out[sym] = q
    return out

def fetch_quote_summary_for_symbol(
    sess: requests.Session,
    symbol: str,
    crumb: Optional[str],
    modules: List[str],
) -> Dict[str, Any]:
    url = YF_QS_URL.format(symbol=symbol)
    params = {"modules": ",".join(modules), "region": "AU", "lang": "en-AU"}
    js, status, err = request_json(sess, url, params, timeout=30, retries=4, crumb=crumb)
    row: Dict[str, Any] = {
        "fundamentalsFetchHttpStatus": status if status else None,
        "fundamentalsFetchError": err,
    }
    if not js:
        row["fundamentalsFetchStatus"] = "http_error" if status else "exception"
        return row

    qs = js.get("quoteSummary") or {}
    err_obj = qs.get("error")
    if err_obj:
        row["fundamentalsFetchStatus"] = "missing_modules"
        row["fundamentalsFetchError"] = err_obj.get("description") or err_obj.get("code") or "qs_error"
        return row

    res = qs.get("result") or []
    if not res:
        row["fundamentalsFetchStatus"] = "missing_modules"
        row["fundamentalsFetchError"] = "empty_result"
        return row

    row["fundamentalsFetchStatus"] = "ok"
    row["fundamentalsFetchError"] = None

    qs0 = res[0] or {}

    # Extract key fields from modules
    sd = qs0.get("summaryDetail") or {}
    ks = qs0.get("defaultKeyStatistics") or {}
    fd = qs0.get("financialData") or {}
    prof = qs0.get("summaryProfile") or {}
    price = qs0.get("price") or {}
    cal = qs0.get("calendarEvents") or {}

    def put(d: dict, key: str, out_key: str | None = None):
        row[out_key or key] = yf_raw(d.get(key))

    # Profile / classification (Yahoo's own; we'll also enrich from universe)
    put(prof, "sector", "sector_yahoo")
    put(prof, "industry", "industry_yahoo")
    put(prof, "longBusinessSummary", "businessSummary")
    put(prof, "website", "website")
    put(prof, "country", "country")
    put(prof, "fullTimeEmployees", "fullTimeEmployees")

    # FinancialData
    for k in [
        "freeCashflow", "operatingCashflow",
        "ebitda", "totalRevenue", "revenueGrowth",
        "grossMargins", "ebitdaMargins", "operatingMargins", "profitMargins",
        "returnOnAssets", "returnOnEquity",
        "totalCash", "totalDebt", "debtToEquity",
        "currentRatio", "quickRatio",
        "earningsGrowth",
        "targetMeanPrice",
        "targetHighPrice",
        "targetLowPrice",
        "recommendationMean",
        "recommendationKey",
        "numberOfAnalystOpinions",
    ]:
        put(fd, k)

    # Key statistics
    for k in [
        "sharesOutstanding", "floatShares",
        "bookValue",
        "enterpriseValue",
        "pegRatio",
        "heldPercentInsiders", "heldPercentInstitutions",
        "beta",
        "shortPercentOfFloat",
        "shortRatio",
        "sharesShort",
        "sharesShortPriorMonth",
        "dateShortInterest",
        "heldPercentInsiders",
        "heldPercentInstitutions",
    ]:
        put(ks, k)

    # Summary detail
    for k in [
        "dividendRate", "dividendYield",
        "payoutRatio",
        "exDividendDate",
        "trailingAnnualDividendRate", "trailingAnnualDividendYield",
        "fiveYearAvgDividendYield",
    ]:
        put(sd, k)

    # Price module sometimes has company names
    put(price, "shortName")
    put(price, "longName")

    # Calendar events (earnings dates)
    earnings = (cal.get("earnings") or {})
    row["earningsDateStart"] = yf_raw(((earnings.get("earningsDate") or [{}])[0]).get("raw") if isinstance(earnings.get("earningsDate"), list) else None)
    if isinstance(earnings.get("earningsDate"), list) and len(earnings.get("earningsDate") or []) > 1:
        row["earningsDateEnd"] = yf_raw((earnings.get("earningsDate")[1] or {}).get("raw"))
    else:
        row["earningsDateEnd"] = None

    # Optional annual statements (only present if modules requested)
    bsh = qs0.get('balanceSheetHistory') or {}
    bal_list = bsh.get('balanceSheetStatements') or []
    if bal_list:
        bs0 = bal_list[0] or {}
        row['bs_totalAssets'] = yf_raw(bs0.get('totalAssets'))
        row['bs_totalLiab'] = yf_raw(bs0.get('totalLiab'))
        row['bs_totalStockholderEquity'] = yf_raw(bs0.get('totalStockholderEquity'))
        row['bs_netTangibleAssets'] = yf_raw(bs0.get('netTangibleAssets'))

    ish = qs0.get('incomeStatementHistory') or {}
    inc_list = ish.get('incomeStatementHistory') or []
    if inc_list:
        is0 = inc_list[0] or {}
        row['is_totalRevenue'] = yf_raw(is0.get('totalRevenue'))
        row['is_operatingIncome'] = yf_raw(is0.get('operatingIncome'))
        row['is_netIncome'] = yf_raw(is0.get('netIncome'))

    cfh = qs0.get('cashflowStatementHistory') or {}
    cf_list = cfh.get('cashflowStatements') or []
    if cf_list:
        cf0 = cf_list[0] or {}
        row['cf_operatingCashflow'] = yf_raw(cf0.get('totalCashFromOperatingActivities'))
        row['cf_capex'] = yf_raw(cf0.get('capitalExpenditures'))

    return row

# -----------------------------------------------------------------------------
# Cache
# -----------------------------------------------------------------------------
def load_cache(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"_meta": {"schema_version": 2, "cursor": 0}, "bySymbol": {}}
    d = json.loads(p.read_text(encoding="utf-8"))
    if "_meta" not in d:
        d["_meta"] = {"schema_version": 2, "cursor": 0}
    if "bySymbol" not in d:
        d["bySymbol"] = {}
    # upgrade schema
    d["_meta"]["schema_version"] = 2
    if "cursor" not in d["_meta"]:
        d["_meta"]["cursor"] = 0
    return d

def save_cache(path: str, cache: Dict[str, Any]) -> None:
    write_json(path, cache)

def eligible_symbols_for_deep(tickers: List[str], universe: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Prefer universe asset_type to exclude ETFs/ETPs from deep fundamentals.
    """
    out: List[str] = []
    for t in tickers:
        info = universe.get(t) or {}
        asset_type = (info.get("asset_type") or "").upper()
        if asset_type in ("ETF/ETP", "ETF", "ETP"):
            continue
        out.append(t)
    return out

def slice_rotate(symbols: List[str], cursor: int, n: int) -> Tuple[List[str], int, str, str]:
    if not symbols:
        return [], 0, "", ""
    n = max(1, min(n, len(symbols)))
    cursor = cursor % len(symbols)
    end = cursor + n
    if end <= len(symbols):
        batch = symbols[cursor:end]
    else:
        batch = symbols[cursor:] + symbols[: end - len(symbols)]
    next_cursor = (cursor + n) % len(symbols)
    return batch, next_cursor, batch[0], batch[-1]

# -----------------------------------------------------------------------------
# Derived price-consistent fields
# -----------------------------------------------------------------------------
def apply_price_consistency(row: Dict[str, Any], price_info: Optional[Dict[str, Any]]) -> None:
    """
    Overwrite/derive price-derived fields using prices_latest.json price when possible.
    """
    if not price_info:
        return
    px = price_info.get("price")
    px = safe_float(px)
    if px is None:
        return

    row["regularMarketPrice"] = px
    row["priceFetchedAtUtc"] = price_info.get("fetchedAtUtc")
    row["priceMarketDate"] = price_info.get("marketDate")
    row["priceSource"] = "prices_latest.json"

    shares = safe_float(row.get("sharesOutstanding"))
    if shares is not None and shares > 0 and row.get("marketCap") is None:
        row["marketCap"] = shares * px

    # Dividend yield can be derived if dividendRate exists
    div_rate = safe_float(row.get("dividendRate"))
    if div_rate is not None and px > 0:
        row["dividendYield"] = div_rate / px

    eps_ttm = safe_float(row.get("epsTrailingTwelveMonths"))
    if eps_ttm is not None and eps_ttm != 0:
        row["trailingPE"] = px / eps_ttm

    eps_fwd = safe_float(row.get("epsForward"))
    if eps_fwd is not None and eps_fwd != 0:
        row["forwardPE"] = px / eps_fwd

    book = safe_float(row.get("bookValue"))
    if book is not None and book != 0:
        row["priceToBook"] = px / book

    # EV = mkt cap + debt - cash
    mc = safe_float(row.get("marketCap"))
    debt = safe_float(row.get("totalDebt"))
    cash = safe_float(row.get("totalCash"))
    if mc is not None and debt is not None and cash is not None:
        row["enterpriseValue"] = mc + debt - cash

    ev = safe_float(row.get("enterpriseValue"))
    ebitda = safe_float(row.get("ebitda"))
    if ev is not None and ebitda is not None and ebitda != 0:
        row["enterpriseToEbitda"] = ev / ebitda

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="ASX fundamentals snapshot (Yahoo + cache + universe enrichment).")
    ap.add_argument("--repo-root", "--repo_root", dest="repo_root", default=".", help="Repo root to resolve default paths.")
    ap.add_argument("--tickers", default=DEFAULT_TICKERS_FILE)
    ap.add_argument("--universe-csv", "--universe_csv", dest="universe_csv", default=DEFAULT_UNIVERSE_CSV)
    ap.add_argument("--prices-latest", "--prices_latest", dest="prices_latest", default=DEFAULT_PRICES_LATEST)
    ap.add_argument("--out-json", "--out_json", dest="out_json", default=DEFAULT_OUT_JSON)
    ap.add_argument("--out-csv", "--out_csv", dest="out_csv", default=DEFAULT_OUT_CSV)
    ap.add_argument("--out-xlsx", "--out_xlsx", dest="out_xlsx", default=DEFAULT_OUT_XLSX)
    ap.add_argument("--cache", default=DEFAULT_CACHE)

    ap.add_argument("--mode", choices=["rotate", "full"], default=DEFAULT_MODE)
    ap.add_argument("--summary-per-run", "--summary_per_run", dest="summary_per_run", type=int, default=DEFAULT_SUMMARY_PER_RUN)
    ap.add_argument("--quote-chunk", "--quote_chunk", dest="quote_chunk", type=int, default=DEFAULT_QUOTE_CHUNK)
    ap.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    ap.add_argument("--min-success-rate", "--min_success_rate", dest="min_success_rate", type=float, default=DEFAULT_MIN_SUCCESS_RATE)
    ap.add_argument("--fail-on-low-success", "--fail_on_low_success", dest="fail_on_low_success", type=str, default=str(DEFAULT_FAIL_ON_LOW_SUCCESS).lower())
    ap.add_argument("--include-statements", "--include_statements", dest="include_statements", action="store_true", default=DEFAULT_INCLUDE_STATEMENTS)

    args = ap.parse_args()

    rr = Path(args.repo_root).resolve()
    def _rp(p: str) -> str:
        pp = Path(p)
        return str((rr / pp).resolve()) if not pp.is_absolute() else str(pp)
    args.tickers = _rp(args.tickers)
    args.universe_csv = _rp(args.universe_csv)
    args.prices_latest = _rp(args.prices_latest)
    args.out_json = _rp(args.out_json)
    args.out_csv = _rp(args.out_csv)
    args.out_xlsx = _rp(args.out_xlsx)
    args.cache = _rp(args.cache)

    tickers = read_tickers(args.tickers)
    universe = load_universe(args.universe_csv)
    prices = load_prices_latest(args.prices_latest)

    sess, crumb = build_yahoo_session()

    # Bulk quote for everyone (cheap-ish). Note: some fields are still price-derived, but useful.
    quote_map = fetch_bulk_quote(sess, tickers, crumb=crumb, quote_chunk=int(args.quote_chunk))

    # Cache (deep fundamentals)
    cache = load_cache(args.cache)
    meta = cache.get("_meta") or {}
    by_symbol: Dict[str, Any] = cache.get("bySymbol") or {}
    cache["bySymbol"] = by_symbol
    cache["_meta"] = meta

    eligible = eligible_symbols_for_deep(tickers, universe)
    eligible_hash = sha1_text("|".join(eligible))
    if meta.get("eligible_hash") != eligible_hash:
        # Universe/tickers changed: reset cursor so we don't skip weirdly
        meta["cursor"] = 0
        meta["eligible_hash"] = eligible_hash
        meta["eligible_count"] = len(eligible)

    cursor = int(meta.get("cursor") or 0)

    if args.mode == "full":
        batch = eligible
        next_cursor = cursor
        first_sym = batch[0] if batch else ""
        last_sym = batch[-1] if batch else ""
    else:
        batch, next_cursor, first_sym, last_sym = slice_rotate(eligible, cursor, int(args.summary_per_run))

    # Deep fetch for batch (skip ETFs already filtered out)
    attempted = 0
    ok = 0
    status_counts: Dict[str, int] = {}
    http_counts: Dict[str, int] = {}

    modules = QS_MODULES + (STATEMENT_MODULES if args.include_statements else [])
    def worker(sym: str) -> Tuple[str, Dict[str, Any]]:
        # Always include timestamp & symbol
        res = fetch_quote_summary_for_symbol(sess, sym, crumb=crumb, modules=modules)
        res["fundamentalsFetchedAtUtc"] = utc_now_iso()
        res["symbol"] = sym
        return sym, res

    # Concurrency guard
    max_workers = max(1, int(args.concurrency))

    # If Yahoo is unhappy, lower concurrency helps a lot. We still do it in parallel, just gently.
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for sym in batch:
            futures.append(ex.submit(worker, sym))

        for fut in as_completed(futures):
            sym, res = fut.result()
            attempted += 1
            st = res.get("fundamentalsFetchStatus") or "unknown"
            status_counts[st] = status_counts.get(st, 0) + 1

            hs = res.get("fundamentalsFetchHttpStatus")
            if hs is not None:
                http_counts[str(hs)] = http_counts.get(str(hs), 0) + 1

            if st == "ok":
                ok += 1
                # merge into cache for that symbol (keep last good values even if later runs fail)
                prev = by_symbol.get(sym) or {}
                merged = dict(prev)
                merged.update(res)
                by_symbol[sym] = merged
            else:
                # still record a small telemetry record so you can see failure reasons in output/cached meta
                prev = by_symbol.get(sym) or {}
                merged = dict(prev)
                merged.update({
                    "fundamentalsFetchStatus": st,
                    "fundamentalsFetchHttpStatus": res.get("fundamentalsFetchHttpStatus"),
                    "fundamentalsFetchError": res.get("fundamentalsFetchError"),
                    "fundamentalsFetchedAtUtc": res.get("fundamentalsFetchedAtUtc"),
                })
                by_symbol[sym] = merged

    success_rate = (ok / attempted) if attempted else 0.0

    now = utc_now_iso()
    meta["lastRunUtc"] = now
    meta["cursorBatchSize"] = len(batch)
    meta["cursorLastBatchFirstSymbol"] = first_sym
    meta["cursorLastBatchLastSymbol"] = last_sym
    meta["deepAttempted"] = attempted
    meta["deepOk"] = ok
    meta["deepSuccessRate"] = round(success_rate, 4)
    meta["deepStatusCounts"] = status_counts
    meta["deepHttpCounts"] = http_counts

    fail_on_low_success = (str(args.fail_on_low_success).lower() in ("1","true","yes","y","on"))

    # Advance cursor only if deep fetch looks healthy
    if args.mode == "rotate" and attempted > 0:
        if success_rate >= float(args.min_success_rate):
            meta["cursor"] = next_cursor
            meta["cursorUpdatedUtc"] = now
        else:
            # don't advance; keep cursor and force visibility
            meta["cursorNotAdvancedUtc"] = now
            meta["cursorNotAdvancedReason"] = f"success_rate_{success_rate:.3f}_lt_{float(args.min_success_rate):.3f}"

            if fail_on_low_success:
                # Write cache/meta for debugging before failing
                save_cache(args.cache, cache)
                raise SystemExit(
                    f"Deep fundamentals success_rate={success_rate:.3f} below min_success_rate={float(args.min_success_rate):.3f}. "
                    f"Not advancing cursor; failing run so you notice."
                )

    # Persist cache
    save_cache(args.cache, cache)

    # -------------------------------------------------------------------------
    # Build output rows (one row per ticker, always)
    # -------------------------------------------------------------------------
    rows: List[Dict[str, Any]] = []
    for sym in tickers:
        uni = universe.get(sym) or {}
        q = quote_map.get(sym) or {}

        row: Dict[str, Any] = {
            "symbol": sym,
            "name": uni.get("name"),
            "sector": uni.get("sector"),
            "industry": uni.get("industry"),
            "asset_type": uni.get("asset_type"),
            "universe_source": uni.get("source"),
        }

        # Quote fields (wide-ish)
        for k in [
            "quoteType",
            "currency",
            "exchange",
            "fullExchangeName",
            "shortName",
            "longName",
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
            "fiftyTwoWeekHigh",
            "fiftyTwoWeekLow",
            "fiftyDayAverage",
            "twoHundredDayAverage",
            "averageDailyVolume3Month",
            "averageDailyVolume10Day",
            "regularMarketPreviousClose",
            "regularMarketOpen",
            "regularMarketDayHigh",
            "regularMarketDayLow",
            "regularMarketVolume",
            "regularMarketChange",
            "regularMarketChangePercent",
        ]:
            row[k] = q.get(k)

        # Prefer universe name if Yahoo names are missing
        if not row.get("shortName") and uni.get("name"):
            row["shortName"] = uni.get("name")
        if not row.get("longName") and uni.get("name"):
            row["longName"] = uni.get("name")

        # Overlay deep fields from cache (if any)
        deep = by_symbol.get(sym) or {}
        # Always include deep telemetry fields
        row["fundamentalsFetchStatus"] = deep.get("fundamentalsFetchStatus")
        row["fundamentalsFetchHttpStatus"] = deep.get("fundamentalsFetchHttpStatus")
        row["fundamentalsFetchError"] = deep.get("fundamentalsFetchError")
        row["fundamentalsFetchedAtUtc"] = deep.get("fundamentalsFetchedAtUtc")

        # Add extracted deep values if present
        for k in [
            "freeCashflow", "operatingCashflow",
            "ebitda", "totalRevenue", "revenueGrowth",
            "grossMargins", "ebitdaMargins", "operatingMargins", "profitMargins",
            "returnOnAssets", "returnOnEquity",
            "totalCash", "totalDebt", "debtToEquity",
            "currentRatio", "quickRatio",
            "enterpriseValue", "pegRatio",
            "bookValue",
            "heldPercentInsiders", "heldPercentInstitutions",
            "payoutRatio", "exDividendDate",
            "trailingAnnualDividendRate", "trailingAnnualDividendYield",
            "fiveYearAvgDividendYield",
            "sector_yahoo", "industry_yahoo", "businessSummary",
            "earningsDateStart", "earningsDateEnd",
            "earningsGrowth",
            "targetMeanPrice",
            "targetHighPrice",
            "targetLowPrice",
            "recommendationMean",
            "recommendationKey",
            "numberOfAnalystOpinions",
            "shortPercentOfFloat",
            "shortRatio",
            "sharesShort",
            "sharesShortPriorMonth",
            "dateShortInterest",
            "website",
            "country",
            "fullTimeEmployees",
            "bs_totalAssets",
            "bs_totalLiab",
            "bs_totalStockholderEquity",
            "bs_netTangibleAssets",
            "is_totalRevenue",
            "is_operatingIncome",
            "is_netIncome",
            "cf_operatingCashflow",
            "cf_capex",
        ]:
            if k in deep and deep.get(k) is not None:
                row[k] = deep.get(k)
            else:
                row.setdefault(k, None)

        # Mark ETFs as not_applicable if they have no deep status
        asset_type = (row.get("asset_type") or "").upper()
        if asset_type in ("ETF/ETP", "ETF", "ETP") and not row.get("fundamentalsFetchStatus"):
            row["fundamentalsFetchStatus"] = "not_applicable"

        # Price consistency (from prices_latest.json)
        apply_price_consistency(row, prices.get(sym))

        rows.append(row)

    # Dataset meta + field map lives in JSON and Excel tab
    field_map_rows = [
        {"field": "freeCashflow", "used_for": "DCF / intrinsic valuation (FCF projection)"},
        {"field": "operatingCashflow", "used_for": "FCF sanity checks / quality screens"},
        {"field": "ebitda", "used_for": "EV/EBITDA, margin quality"},
        {"field": "totalRevenue", "used_for": "growth, sizing, valuation multiples"},
        {"field": "revenueGrowth", "used_for": "DCF growth assumption, PEG context"},
        {"field": "dividendRate", "used_for": "DDM inputs, yield calculation"},
        {"field": "payoutRatio", "used_for": "dividend sustainability"},
        {"field": "sharesOutstanding", "used_for": "market cap, per-share metrics"},
        {"field": "bookValue", "used_for": "P/B, asset-based valuation context"},
        {"field": "totalDebt", "used_for": "EV, leverage, risk"},
        {"field": "totalCash", "used_for": "EV, liquidity buffers"},
        {"field": "enterpriseValue", "used_for": "EV multiples"},
        {"field": "pegRatio", "used_for": "growth-adjusted valuation screen"},
    ]

    out_json = {
        "dataset": "asx_fundamentals_latest",
        "asOfUtc": now,
        "source": "Yahoo Finance (quote + quoteSummary) + universe.csv enrichment + prices_latest.json for price consistency",
        "mode": args.mode,
        "summaryPerRun": int(args.summary_per_run),
        "includeStatements": False,
        "tickersCount": len(tickers),
        "fieldMap": field_map_rows,
        "cacheMeta": meta,
        "fundamentals": rows,
    }

    write_json(args.out_json, out_json)

    # CSV mirror: keep it flat (drop businessSummary which can be huge)
    df = pd.DataFrame(rows)
    if "businessSummary" in df.columns:
        df = df.drop(columns=["businessSummary"])
    df.to_csv(args.out_csv, index=False)

    # Excel with 2 sheets
    with pd.ExcelWriter(args.out_xlsx, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="fundamentals")
        pd.DataFrame(field_map_rows).to_excel(xw, index=False, sheet_name="field_map")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())