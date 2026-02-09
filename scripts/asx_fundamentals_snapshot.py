#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from common import write_json, utc_now_iso

# -----------------------------------------------------------------------------
# Yahoo endpoints (unofficial)
# -----------------------------------------------------------------------------
YF_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YF_QS_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"

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
DEFAULT_CONCURRENCY = 12  # used as max_workers for quoteSummary requests
DEFAULT_INCLUDE_STATEMENTS = False

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

# Quote endpoint fields (cheap, batched). We still fetch this for broad coverage.
QUOTE_FIELDS = [
    # identity
    "symbol", "quoteType", "shortName", "longName", "currency", "exchange", "fullExchangeName",
    # price/volume (we override price from prices_latest.json to keep consistency)
    "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent",
    "regularMarketOpen", "regularMarketPreviousClose",
    "regularMarketDayHigh", "regularMarketDayLow",
    "regularMarketVolume",
    "averageDailyVolume3Month", "averageDailyVolume10Day",
    # ranges/averages
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyDayAverage", "twoHundredDayAverage",
    # common ratios/metrics
    "marketCap", "sharesOutstanding", "floatShares",
    "trailingPE", "forwardPE",
    "epsTrailingTwelveMonths", "epsForward",
    "dividendRate", "dividendYield",
    "beta",
    "bookValue", "priceToBook",
]

# Field map (goes into XLSX tab + JSON). No separate CSV file is generated.
FIELD_MAP: List[Dict[str, str]] = [
    {"field": "regularMarketPrice", "used_for": "Join anchor; recompute price-derived ratios consistently with prices_latest.json", "source": "prices_latest.json"},
    {"field": "marketCap", "used_for": "Size filters; EV bridge; FCF yield", "source": "derived (sharesOutstanding * price) or Yahoo"},
    {"field": "sharesOutstanding", "used_for": "Per-share outputs (DCF/DDM/Residual Income)", "source": "quote/defaultKeyStatistics"},
    {"field": "dividendRate", "used_for": "DDM; Yield (derived)", "source": "quote/summaryDetail"},
    {"field": "dividendYield", "used_for": "Yield (derived from dividendRate/price)", "source": "derived or Yahoo"},
    {"field": "freeCashflow", "used_for": "DCF (FCF-based)", "source": "financialData"},
    {"field": "operatingCashflow", "used_for": "DCF cross-check / fallback", "source": "financialData"},
    {"field": "totalCash", "used_for": "Net debt; EV bridge", "source": "financialData"},
    {"field": "totalDebt", "used_for": "Net debt; EV bridge", "source": "financialData"},
    {"field": "enterpriseValue", "used_for": "EV multiples; EPV variants", "source": "derived (mcap + debt - cash) or Yahoo"},
    {"field": "enterpriseToEbitda", "used_for": "EV/EBITDA", "source": "derived or Yahoo"},
    {"field": "ebitda", "used_for": "EPV; EV/EBITDA", "source": "financialData"},
    {"field": "totalRevenue", "used_for": "EPV; revenue screens", "source": "financialData / statements"},
    {"field": "revenueGrowth", "used_for": "Growth assumptions; screens", "source": "financialData"},
    {"field": "earningsGrowth", "used_for": "PEG; growth assumptions", "source": "financialData"},
    {"field": "bookValue", "used_for": "Residual Income; asset-based", "source": "quote/defaultKeyStatistics"},
    {"field": "returnOnEquity", "used_for": "Residual Income; quality", "source": "financialData"},
    {"field": "heldPercentInsiders", "used_for": "Ownership context", "source": "defaultKeyStatistics"},
    {"field": "heldPercentInstitutions", "used_for": "Ownership context", "source": "defaultKeyStatistics"},
    {"field": "shortPercentOfFloat", "used_for": "Short pressure context", "source": "defaultKeyStatistics"},
    {"field": "targetMeanPrice", "used_for": "Analyst overlay (not intrinsic)", "source": "financialData"},
    {"field": "recommendationMean", "used_for": "Analyst overlay (not intrinsic)", "source": "financialData"},
    {"field": "sector_yahoo", "used_for": "Extra metadata (not GICS)", "source": "summaryProfile"},
    {"field": "industry_yahoo", "used_for": "Extra metadata (not GICS)", "source": "summaryProfile"},
    {"field": "fundamentalsFetchedAtUtc", "used_for": "Per-ticker freshness of deep fundamentals", "source": "cache"},
]

# -----------------------------------------------------------------------------
# Utility
# -----------------------------------------------------------------------------
def read_tickers(path: str) -> List[str]:
    """Read tickers from tickers file + optional tickers_extra + union in universe.csv to prevent drift."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tickers file not found: {path}")

    out: List[str] = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)

    # Optional extra tickers
    extra_path = Path("asx/tickers_extra.txt")
    if extra_path.exists():
        for line in extra_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            s2 = line.strip()
            if not s2 or s2.startswith("#"):
                continue
            out.append(s2)

    # Union in universe.csv too (prevents drift)
    uni_path = Path(DEFAULT_UNIVERSE_CSV)
    if uni_path.exists():
        try:
            dfu = pd.read_csv(uni_path)
            col = "yahoo_symbol" if "yahoo_symbol" in dfu.columns else ("code" if "code" in dfu.columns else None)
            if col:
                for v in dfu[col].astype(str).tolist():
                    s3 = v.strip().upper()
                    if not s3 or s3 == "NAN":
                        continue
                    if not s3.endswith(".AX") and re.match(r"^[A-Z0-9]+$", s3):
                        s3 = f"{s3}.AX"
                    out.append(s3)
        except Exception:
            pass

    # dedupe, preserve order
    seen = set()
    final: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            final.append(t)
    return final

def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

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

def is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x)))

# -----------------------------------------------------------------------------
# Prices_latest integration (canonical price)
# -----------------------------------------------------------------------------
def load_prices_latest(path: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns map: symbol -> {price, fetchedAtUtc, marketDate, currency, source}
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        prices = j.get("prices") or {}
        out: Dict[str, Dict[str, Any]] = {}
        for sym, obj in prices.items():
            if not isinstance(obj, dict):
                continue
            out[sym] = {
                "price": obj.get("price"),
                "currency": obj.get("currency"),
                "marketDate": obj.get("marketDate"),
                "fetchedAtUtc": obj.get("fetchedAtUtc"),
                "source": obj.get("source"),
            }
        return out
    except Exception:
        return {}

# -----------------------------------------------------------------------------
# Cache (for deep fundamentals)
# -----------------------------------------------------------------------------
def load_cache(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"_meta": {"schema_version": 1, "cursor": 0}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"_meta": {"schema_version": 1, "cursor": 0}}

def save_cache(path: str, cache: Dict[str, Any]) -> None:
    # atomic write
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(p)

# -----------------------------------------------------------------------------
# Yahoo fetching
# -----------------------------------------------------------------------------
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def fetch_quote_batch(sess: requests.Session, symbols: List[str], retry: int = 3) -> List[Dict[str, Any]]:
    params = {"symbols": ",".join(symbols)}
    for attempt in range(retry):
        try:
            r = sess.get(YF_QUOTE_URL, params=params, timeout=30)
            if r.status_code == 429:
                # backoff
                time.sleep(2.0 + attempt * 2.0)
                continue
            r.raise_for_status()
            data = r.json()
            return (data.get("quoteResponse") or {}).get("result") or []
        except Exception:
            time.sleep(1.0 + attempt * 1.5)
    return []

def fetch_quote_summary(sess: requests.Session, symbol: str, modules: List[str], retry: int = 3) -> Optional[Dict[str, Any]]:
    url = YF_QS_URL.format(symbol=symbol)
    params = {"modules": ",".join(modules)}
    for attempt in range(retry):
        try:
            r = sess.get(url, params=params, timeout=30)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2.0 + attempt * 2.0 + random.random())
                continue
            r.raise_for_status()
            data = r.json()
            qs = data.get("quoteSummary") or {}
            res = qs.get("result") or []
            if not res:
                return None
            return res[0]
        except Exception:
            time.sleep(1.0 + attempt * 2.0 + random.random())
    return None

# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------
def extract_quote_fields(q: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for f in QUOTE_FIELDS:
        row[f] = q.get(f)
    row["yahoo_symbol"] = q.get("symbol")
    return row

def extract_qs_fields(qs_result: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    summary_detail = qs_result.get("summaryDetail") or {}
    default_stats = qs_result.get("defaultKeyStatistics") or {}
    financial_data = qs_result.get("financialData") or {}
    profile = qs_result.get("summaryProfile") or {}
    price_mod = qs_result.get("price") or {}
    cal = qs_result.get("calendarEvents") or {}

    # SummaryDetail
    for k in [
        "payoutRatio", "fiveYearAvgDividendYield", "exDividendDate",
        "trailingAnnualDividendRate", "trailingAnnualDividendYield",
        "dividendRate", "dividendYield",
        "beta", "marketCap",
    ]:
        out[k] = yf_raw(summary_detail.get(k))

    # DefaultKeyStatistics
    for k in [
        "enterpriseValue", "enterpriseToEbitda", "pegRatio",
        "sharesOutstanding", "floatShares",
        "heldPercentInsiders", "heldPercentInstitutions",
        "shortPercentOfFloat", "shortRatio",
        "sharesShort", "sharesShortPriorMonth",
        "dateShortInterest",
        "lastSplitFactor", "lastSplitDate",
        "bookValue", "priceToBook",
    ]:
        out[k] = yf_raw(default_stats.get(k))

    # FinancialData (big hitters)
    for k in [
        "freeCashflow", "operatingCashflow",
        "ebitda",
        "totalCash", "totalDebt", "debtToEquity",
        "currentRatio", "quickRatio",
        "grossMargins", "operatingMargins", "profitMargins",
        "returnOnAssets", "returnOnEquity",
        "revenueGrowth", "earningsGrowth",
        "totalRevenue",
        "targetMeanPrice", "targetHighPrice", "targetLowPrice",
        "recommendationMean", "recommendationKey", "numberOfAnalystOpinions",
    ]:
        out[k] = yf_raw(financial_data.get(k))

    # Profile (extra; not GICS)
    out["sector_yahoo"] = profile.get("sector")
    out["industry_yahoo"] = profile.get("industry")
    out["website"] = profile.get("website")
    out["country"] = profile.get("country")
    out["fullTimeEmployees"] = yf_raw(profile.get("fullTimeEmployees"))
    out["longBusinessSummary"] = profile.get("longBusinessSummary")

    # Price module (sometimes duplicates / helps)
    out["exchangeName"] = price_mod.get("exchangeName")
    out["marketCap_price_module"] = yf_raw(price_mod.get("marketCap"))

    # Calendar events
    earnings = get_path(cal, ["earnings", "earningsDate"], default=None)
    if isinstance(earnings, list) and earnings:
        out["earningsDate"] = yf_raw(earnings[0])
    else:
        out["earningsDate"] = None

    # Derived convenience (net debt)
    cash = out.get("totalCash")
    debt = out.get("totalDebt")
    if is_number(cash) and is_number(debt):
        out["netDebt"] = debt - cash
    else:
        out["netDebt"] = None

    return out

def extract_statements(qs_result: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    ish = qs_result.get("incomeStatementHistory") or {}
    bsh = qs_result.get("balanceSheetHistory") or {}
    cfh = qs_result.get("cashflowStatementHistory") or {}

    inc_list = get_path(ish, ["incomeStatementHistory"], default=[]) or []
    bal_list = get_path(bsh, ["balanceSheetStatements"], default=[]) or []
    cf_list = get_path(cfh, ["cashflowStatements"], default=[]) or []

    # nested (JSON only)
    out["statements_income_annual"] = inc_list
    out["statements_balance_annual"] = bal_list
    out["statements_cashflow_annual"] = cf_list

    # flatten latest annual line items for CSV
    if inc_list:
        latest = inc_list[0]
        out["is_totalRevenue_annual"] = yf_raw(latest.get("totalRevenue"))
        out["is_netIncome_annual"] = yf_raw(latest.get("netIncome"))
        out["is_operatingIncome_annual"] = yf_raw(latest.get("operatingIncome"))
    if bal_list:
        latest = bal_list[0]
        out["bs_totalAssets_annual"] = yf_raw(latest.get("totalAssets"))
        out["bs_totalLiab_annual"] = yf_raw(latest.get("totalLiab"))
        out["bs_totalStockholderEquity_annual"] = yf_raw(latest.get("totalStockholderEquity"))
    if cf_list:
        latest = cf_list[0]
        out["cf_operatingCashflow_annual"] = yf_raw(latest.get("totalCashFromOperatingActivities"))
        out["cf_capex_annual"] = yf_raw(latest.get("capitalExpenditures"))

    return out

# -----------------------------------------------------------------------------
# Price-derived consistency overrides (use prices_latest.json as canonical)
# -----------------------------------------------------------------------------
def apply_price_overrides(row: Dict[str, Any], price_info: Optional[Dict[str, Any]]) -> None:
    """
    Overwrite/derive price-dependent fields using canonical price_latest.json.
    Falls back to Yahoo values if inputs missing.
    """
    if not price_info:
        return

    price = price_info.get("price")
    if not is_number(price):
        return

    # store canonical price + provenance
    row["regularMarketPrice"] = float(price)
    row["priceFetchedAtUtc"] = price_info.get("fetchedAtUtc")
    row["priceMarketDate"] = price_info.get("marketDate")
    row["priceSource"] = "prices_latest.json"

    # marketCap (prefer derive if shares outstanding available)
    shares = row.get("sharesOutstanding")
    if is_number(shares):
        row["marketCap"] = float(shares) * float(price)

    # dividendYield (prefer derive if dividendRate present)
    div_rate = row.get("dividendRate")
    if is_number(div_rate) and float(price) != 0.0:
        row["dividendYield"] = float(div_rate) / float(price)

    # trailingPE / forwardPE (derive from EPS)
    eps_ttm = row.get("epsTrailingTwelveMonths")
    if is_number(eps_ttm) and float(eps_ttm) != 0.0:
        row["trailingPE"] = float(price) / float(eps_ttm)

    eps_fwd = row.get("epsForward")
    if is_number(eps_fwd) and float(eps_fwd) != 0.0:
        row["forwardPE"] = float(price) / float(eps_fwd)

    # priceToBook
    bv = row.get("bookValue")
    if is_number(bv) and float(bv) != 0.0:
        row["priceToBook"] = float(price) / float(bv)

    # enterpriseValue / enterpriseToEbitda (EV = mcap + debt - cash)
    mcap = row.get("marketCap")
    debt = row.get("totalDebt")
    cash = row.get("totalCash")
    if is_number(mcap) and is_number(debt) and is_number(cash):
        ev = float(mcap) + float(debt) - float(cash)
        row["enterpriseValue"] = ev
        ebitda = row.get("ebitda")
        if is_number(ebitda) and float(ebitda) != 0.0:
            row["enterpriseToEbitda"] = ev / float(ebitda)

# -----------------------------------------------------------------------------
# Rotation selection (stateful cursor)
# -----------------------------------------------------------------------------
def select_symbols(mode: str, symbols: List[str], take: int, cache_meta: Dict[str, Any]) -> Tuple[List[str], Dict[str, Any]]:
    symbols_sorted = sorted(symbols)
    n = len(symbols_sorted)
    if mode == "full" or take >= n:
        cache_meta["cursor"] = 0
        cache_meta["cursorUpdatedUtc"] = utc_now_iso()
        return symbols_sorted, cache_meta

    cursor = int(cache_meta.get("cursor", 0) or 0) % n
    out: List[str] = []
    for i in range(take):
        out.append(symbols_sorted[(cursor + i) % n])

    cache_meta["cursor"] = (cursor + take) % n
    cache_meta["cursorUpdatedUtc"] = utc_now_iso()
    cache_meta["cursorLastBatchFirstSymbol"] = out[0] if out else None
    cache_meta["cursorLastBatchLastSymbol"] = out[-1] if out else None
    cache_meta["cursorBatchSize"] = take
    return out, cache_meta

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--tickers", default=DEFAULT_TICKERS_FILE)
    p.add_argument("--prices-latest", default=DEFAULT_PRICES_LATEST)
    p.add_argument("--out-json", default=DEFAULT_OUT_JSON)
    p.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    p.add_argument("--out-xlsx", default=DEFAULT_OUT_XLSX)
    p.add_argument("--cache", default=DEFAULT_CACHE)
    p.add_argument("--mode", choices=["rotate", "full"], default=DEFAULT_MODE)
    p.add_argument("--summary-per-run", type=int, default=DEFAULT_SUMMARY_PER_RUN, help="How many tickers to refresh deep fundamentals this run")
    p.add_argument("--quote-chunk", type=int, default=DEFAULT_QUOTE_CHUNK)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--include-statements", action="store_true", default=DEFAULT_INCLUDE_STATEMENTS)
    return p.parse_args()

def main() -> None:
    args = parse_args()

    symbols = read_tickers(args.tickers)
    if not symbols:
        raise RuntimeError("No tickers found.")

    # Ensure output dirs exist
    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    Path(args.cache).parent.mkdir(parents=True, exist_ok=True)

    prices_map = load_prices_latest(args.prices_latest)

    cache = load_cache(args.cache)
    cache["_meta"] = cache.get("_meta") or {"schema_version": 1, "cursor": 0}
    cache["_meta"]["schema_version"] = cache["_meta"].get("schema_version", 1)
    cache["_meta"]["lastRunUtc"] = utc_now_iso()

    # Tier A: quote endpoint for everyone (batched)
    quote_fetched_at = utc_now_iso()
    quote_results: Dict[str, Dict[str, Any]] = {}

    with _session() as sess:
        for batch in chunked(symbols, args.quote_chunk):
            res = fetch_quote_batch(sess, batch)
            for q in res:
                sym = q.get("symbol")
                if sym:
                    quote_results[sym] = q

        # Tier B: deep quoteSummary (rotate or full)
        modules = list(DEFAULT_QS_MODULES)
        if args.include_statements:
            modules += STATEMENT_MODULES

        refresh_syms, meta = select_symbols(args.mode, symbols, args.summary_per_run, cache["_meta"])
        cache["_meta"] = meta

        # ThreadPool for quoteSummary
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _one(sym: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            qs = fetch_quote_summary(sess, sym, modules)
            return sym, qs

        if refresh_syms:
            with ThreadPoolExecutor(max_workers=max(1, int(args.concurrency))) as ex:
                futs = [ex.submit(_one, sym) for sym in refresh_syms]
                for fut in as_completed(futs):
                    sym, qs = fut.result()
                    if qs is None:
                        continue
                    entry = cache.get(sym) or {}
                    entry["fundamentalsFetchedAtUtc"] = utc_now_iso()
                    entry["summary"] = extract_qs_fields(qs)
                    if args.include_statements:
                        entry["statementsFetchedAtUtc"] = entry["fundamentalsFetchedAtUtc"]
                        entry["statements"] = extract_statements(qs)
                    cache[sym] = entry

    # Merge per-symbol rows (always include all tickers in output)
    rows: List[Dict[str, Any]] = []
    now_utc = utc_now_iso()

    for sym in symbols:
        q = quote_results.get(sym, {"symbol": sym})
        row = extract_quote_fields(q)

        entry = cache.get(sym) or {}
        summary = entry.get("summary") or {}
        statements = entry.get("statements") or {}

        row.update(summary)
        row.update(statements)

        # freshness fields
        row["quoteFetchedAtUtc"] = quote_fetched_at
        row["fundamentalsFetchedAtUtc"] = entry.get("fundamentalsFetchedAtUtc")  # per ticker
        row["statementsFetchedAtUtc"] = entry.get("statementsFetchedAtUtc")

        # Apply canonical price overrides + derived price-based metrics
        apply_price_overrides(row, prices_map.get(sym))

        rows.append(row)

    # DataFrames
    df = pd.DataFrame(rows)

    # CSV/XLSX: drop huge nested/text fields; keep scalars + flattened statement items
    drop_cols = []
    for c in df.columns:
        if c in ("longBusinessSummary", "statements_income_annual", "statements_balance_annual", "statements_cashflow_annual"):
            drop_cols.append(c)
    df_csv = df.drop(columns=drop_cols, errors="ignore").sort_values(by=["yahoo_symbol"])

    # Field map sheet
    df_map = pd.DataFrame(FIELD_MAP).sort_values(by=["field"])

    # Write CSV
    df_csv.to_csv(args.out_csv, index=False)

    # Write XLSX with two sheets
    with pd.ExcelWriter(args.out_xlsx, engine="openpyxl") as w:
        df_csv.to_excel(w, sheet_name="fundamentals", index=False)
        df_map.to_excel(w, sheet_name="field_map", index=False)

    # Write JSON (includes nested statements + longBusinessSummary)
    payload = {
        "dataset": "asx_fundamentals_latest",
        "asOfUtc": now_utc,
        "source": "Yahoo Finance (quote + quoteSummary) + prices_latest.json for price consistency",
        "mode": args.mode,
        "summaryPerRun": int(args.summary_per_run),
        "includeStatements": bool(args.include_statements),
        "tickersCount": len(symbols),
        "fieldMap": FIELD_MAP,
        "cacheMeta": cache.get("_meta", {}),
        "fundamentals": rows,
    }
    write_json(args.out_json, payload)

    # Persist cache
    save_cache(args.cache, cache)

if __name__ == "__main__":
    main()
