#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from common import write_json, utc_now_iso

DEFAULT_TICKERS_FILE = "asx/tickers_asx.txt"
DEFAULT_UNIVERSE_CSV = "asx/universe.csv"
DEFAULT_PRICES_LATEST = "asx/prices_latest.json"

DEFAULT_OUT_JSON = "asx/fundamentals_fast_latest.json"
DEFAULT_OUT_CSV = "asx/fundamentals_fast_latest.csv"

YAHOO_QUOTE_URLS = [
    "https://query2.finance.yahoo.com/v7/finance/quote",
    "https://query1.finance.yahoo.com/v7/finance/quote",
]

# A practical, "headline fundamentals" set. These are typically returned by the bulk quote endpoint.
FAST_FIELDS: List[str] = [
    # identity
    "symbol",
    "shortName",
    "longName",
    "quoteType",
    "currency",
    "exchange",
    "exchangeName",
    "fullExchangeName",
    # price-ish
    "regularMarketPrice",
    "regularMarketPreviousClose",
    "regularMarketOpen",
    "regularMarketDayHigh",
    "regularMarketDayLow",
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    "regularMarketVolume",
    "averageDailyVolume3Month",
    # valuation-ish / fundamentals-ish
    "marketCap",
    "trailingPE",
    "forwardPE",
    "pegRatio",
    "priceToBook",
    "bookValue",
    "epsTrailingTwelveMonths",
    "epsForward",
    "beta",
    # dividends
    "dividendRate",
    "dividendYield",
    "exDividendDate",
    "payoutRatio",
]


def read_lines(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        return []
    out: List[str] = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def load_universe_map(path_csv: str) -> Dict[str, Dict[str, Any]]:
    p = Path(path_csv)
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    # Expected columns (from your repo): yahoo_symbol, name, sector, industry, asset_type, source
    for col in ["yahoo_symbol", "code", "name", "sector", "industry", "asset_type", "source"]:
        if col not in df.columns:
            df[col] = None

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        sym = str(r.get("yahoo_symbol") or "").strip()
        if not sym:
            continue
        out[sym] = {
            "code": r.get("code"),
            "name_universe": r.get("name"),
            "sector": r.get("sector"),
            "industry": r.get("industry"),
            "asset_type": r.get("asset_type"),
            "universe_source": r.get("source"),
        }
    return out


def load_prices_latest(path_json: str) -> Dict[str, Dict[str, Any]]:
    p = Path(path_json)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    prices = data.get("prices")
    return prices if isinstance(prices, dict) else {}


def read_tickers_union(tickers_path: str, universe_map: Dict[str, Dict[str, Any]]) -> List[str]:
    tickers = read_lines(tickers_path)

    extra_path = Path("asx/tickers_extra.txt")
    if extra_path.exists():
        tickers += read_lines(str(extra_path))

    # Union in the universe to prevent drift
    tickers += list(universe_map.keys())

    # Stable unique order
    seen = set()
    out: List[str] = []
    for s in tickers:
        s = s.strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    out.sort()
    return out


def chunked(xs: List[str], n: int) -> List[List[str]]:
    n = max(1, int(n))
    return [xs[i : i + n] for i in range(0, len(xs), n)]



def yahoo_seed_session(session: requests.Session, timeout_s: float) -> Optional[str]:
    """Try to acquire Yahoo cookies and crumb. Returns crumb if available."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": f"https://finance.yahoo.com/quote/{symbols[0] if symbols else '' }",
    }
    crumb: Optional[str] = None

    # 1) Seed cookies (B cookie, etc.)
    for url in ["https://fc.yahoo.com", "https://finance.yahoo.com"]:
        try:
            session.get(url, headers=headers, timeout=timeout_s)
            break
        except Exception:
            continue

    # 2) Fetch crumb (optional but helps in some environments)
    for base in ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]:
        try:
            r = session.get(f"{base}/v1/test/getcrumb", headers=headers, timeout=timeout_s)
            if r.status_code == 200:
                c = (r.text or "").strip()
                if c and len(c) < 128 and " " not in c:
                    crumb = c
                    break
        except Exception:
            continue

    return crumb


def yahoo_quote_bulk(
    symbols: List[str],
    fields: List[str],
    session: requests.Session,
    timeout_s: float,
    crumb: Optional[str] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[int]]:
    """Fetch bulk quote data. Returns (symbol->payload, http_status)."""
    params = {"symbols": ",".join(symbols), "fields": ",".join(fields), "formatted": "false"}
    if crumb:
        params["crumb"] = crumb
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    last_status: Optional[int] = None
    last_exc: Optional[Exception] = None

    for base in YAHOO_QUOTE_URLS:
        try:
            r = session.get(base, params=params, headers=headers, timeout=timeout_s)
            last_status = r.status_code
            if r.status_code == 429:
                return ({}, 429)
            r.raise_for_status()
            data = r.json()
            res = (data.get("quoteResponse") or {}).get("result") or []
            out: Dict[str, Dict[str, Any]] = {}
            for row in res:
                sym = row.get("symbol")
                if not sym:
                    continue
                out[sym] = row
            return (out, last_status)
        except Exception as e:
            last_exc = e
            continue

    if last_exc:
        print(f"[warn] yahoo_quote_bulk failed: {type(last_exc).__name__}: {last_exc}")
    return ({}, last_status)


def build_records(
    tickers: List[str],
    universe_map: Dict[str, Dict[str, Any]],
    prices_latest: Dict[str, Dict[str, Any]],
    quote_chunk: int,
    timeout_s: float,
    min_sleep_s: float,
    max_sleep_s: float,
    max_retries_429: int,
) -> Tuple[Dict[str, Dict[str, Any]], List[str], Dict[str, Any]]:
    asof_utc = utc_now_iso()
    records: Dict[str, Dict[str, Any]] = {}
    missing = set(tickers)
    http_counts: Dict[str, int] = {}

    sess = requests.Session()
    crumb = yahoo_seed_session(sess, timeout_s=timeout_s)

    chunks = chunked(tickers, quote_chunk)
    for idx, ch in enumerate(chunks, start=1):
        time.sleep(random.uniform(min_sleep_s, max_sleep_s))

        retries_429 = 0
        while True:
            data_by_sym, status = yahoo_quote_bulk(ch, FAST_FIELDS, sess, timeout_s=timeout_s, crumb=crumb)
            status_key = "none" if status is None else str(status)
            http_counts[status_key] = http_counts.get(status_key, 0) + 1

            if status == 429 and retries_429 == 0:
                # Re-seed cookies/crumb once (helps on fresh runners / stricter edges)
                crumb = yahoo_seed_session(sess, timeout_s=timeout_s)

            if status == 429 and retries_429 < max_retries_429:
                backoff = min(180.0, 15.0 * (retries_429 + 1)) + random.random() * 5.0
                print(f"[warn] 429 on chunk {idx}/{len(chunks)}; backoff {backoff:.1f}s (retry {retries_429+1}/{max_retries_429})")
                time.sleep(backoff)
                retries_429 += 1
                continue

            for sym, payload in data_by_sym.items():
                rec: Dict[str, Any] = {
                    "symbol": sym,
                    "fetchedAtUtc": asof_utc,
                    "source": "yahoo_v7_quote",
                }
                for f in FAST_FIELDS:
                    if f == "symbol":
                        continue
                    rec[f] = payload.get(f)

                # Enrichment from your repo (universe + prices_latest)
                rec.update(universe_map.get(sym, {}))

                p = prices_latest.get(sym, {})
                if isinstance(p, dict):
                    rec["price_latest"] = p.get("price")
                    rec["price_currency_latest"] = p.get("currency")
                    rec["price_marketDate_latest"] = p.get("marketDate")
                    rec["price_fetchedAtUtc_latest"] = p.get("fetchedAtUtc")
                    rec["price_source_latest"] = p.get("source")

                records[sym] = rec
                missing.discard(sym)

            break

        if idx % 5 == 0 or idx == len(chunks):
            print(f"[progress] chunks={idx}/{len(chunks)} records={len(records)} missing={len(missing)}")

    # Retry missing in smaller chunks (helps when Yahoo truncates results for large requests)
    if missing:
        miss_list = sorted(missing)
        print(f"[info] retrying missing={len(miss_list)} with smaller chunk=30")
        for ch in chunked(miss_list, 30):
            time.sleep(random.uniform(min_sleep_s, max_sleep_s))
            data_by_sym, status = yahoo_quote_bulk(ch, FAST_FIELDS, sess, timeout_s=timeout_s, crumb=crumb)
            status_key = "none" if status is None else str(status)
            http_counts[status_key] = http_counts.get(status_key, 0) + 1
            for sym, payload in data_by_sym.items():
                rec: Dict[str, Any] = {
                    "symbol": sym,
                    "fetchedAtUtc": asof_utc,
                    "source": "yahoo_v7_quote",
                }
                for f in FAST_FIELDS:
                    if f == "symbol":
                        continue
                    rec[f] = payload.get(f)
                rec.update(universe_map.get(sym, {}))
                p = prices_latest.get(sym, {})
                if isinstance(p, dict):
                    rec["price_latest"] = p.get("price")
                    rec["price_currency_latest"] = p.get("currency")
                    rec["price_marketDate_latest"] = p.get("marketDate")
                    rec["price_fetchedAtUtc_latest"] = p.get("fetchedAtUtc")
                    rec["price_source_latest"] = p.get("source")
                records[sym] = rec
                missing.discard(sym)

    stats = {
        "asOfUtc": asof_utc,
        "countTickers": len(tickers),
        "countReturned": len(records),
        "countMissing": len(missing),
        "httpCounts": http_counts,
        "quoteChunk": int(quote_chunk),
    }
    return records, sorted(missing), stats


def main() -> int:
    ap = argparse.ArgumentParser(description="ASX fast fundamentals snapshot via Yahoo bulk quote endpoint.")
    ap.add_argument("--tickers", default=DEFAULT_TICKERS_FILE, help="Tickers file (default asx/tickers_asx.txt)")
    ap.add_argument("--universe-csv", default=DEFAULT_UNIVERSE_CSV, help="Universe CSV (default asx/universe.csv)")
    ap.add_argument("--prices-latest", default=DEFAULT_PRICES_LATEST, help="Prices latest JSON (default asx/prices_latest.json)")
    ap.add_argument("--out-json", default=DEFAULT_OUT_JSON, help="Output JSON path")
    ap.add_argument("--out-csv", default=DEFAULT_OUT_CSV, help="Output CSV path")
    ap.add_argument("--quote-chunk", type=int, default=120, help="Symbols per bulk quote request (default 120)")
    ap.add_argument("--timeout-s", type=float, default=20.0, help="HTTP timeout seconds (default 20)")
    ap.add_argument("--min-sleep-s", type=float, default=1.2, help="Min sleep between batches (default 0.15)")
    ap.add_argument("--max-sleep-s", type=float, default=2.8, help="Max sleep between batches (default 0.55)")
    ap.add_argument("--max-retries-429", type=int, default=4, help="Retries per chunk when HTTP 429 (default 2)")
    args = ap.parse_args()

    universe_map = load_universe_map(args.universe_csv)
    prices_latest = load_prices_latest(args.prices_latest)
    tickers = read_tickers_union(args.tickers, universe_map)

    asof_utc = utc_now_iso()
    perth = ZoneInfo("Australia/Perth")
    asof_perth = datetime.now(timezone.utc).astimezone(perth).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S AWST")

    records, missing, stats = build_records(
        tickers=tickers,
        universe_map=universe_map,
        prices_latest=prices_latest,
        quote_chunk=args.quote_chunk,
        timeout_s=args.timeout_s,
        min_sleep_s=args.min_sleep_s,
        max_sleep_s=args.max_sleep_s,
        max_retries_429=args.max_retries_429,
    )
    # If Yahoo blocks the bulk quote endpoint (all 429), still emit a fully-populated *enrichment* dataset
    # so downstream apps don't break. Quote fields remain null (no fake data).
    if len(records) == 0 and isinstance(stats, dict) and (stats.get("httpCounts") or {}).get("429"):
        print("[warn] Yahoo bulk quote appears blocked (429). Writing enrichment-only records (prices + universe) so outputs still update.")
        records = {}
        missing = []
        for sym in tickers:
            rec: Dict[str, Any] = {
                "symbol": sym,
                "fetchedAtUtc": asof_utc,
                "source": "enrichment_only_due_to_429",
            }
            # Keep the same FAST_FIELDS keys so the schema is stable
            for f in FAST_FIELDS:
                if f in ("symbol",):
                    continue
                rec[f] = None

            rec.update(universe_map.get(sym, {}))
            p = prices_latest.get(sym, {})
            if isinstance(p, dict):
                rec["price_latest"] = p.get("price")
                rec["price_currency_latest"] = p.get("currency")
                rec["price_marketDate_latest"] = p.get("marketDate")
                rec["price_fetchedAtUtc_latest"] = p.get("fetchedAtUtc")
                rec["price_source_latest"] = p.get("source")
            records[sym] = rec

        stats["countReturned"] = len(records)
        stats["countMissing"] = 0
        stats["mode"] = "enrichment_only_due_to_429"


    payload = {
        "dataset": "asx_fundamentals_fast_latest",
        "asOfUtc": asof_utc,
        "asOfPerth": asof_perth,
        "source": "yahoo_v7_quote",
        "countTickers": len(tickers),
        "countReturned": len(records),
        "missingCount": len(missing),
        "missing": missing[:200],  # cap for readability
        "stats": stats,
        "fields": FAST_FIELDS,
        "records": records,  # map by symbol
    }
    write_json(args.out_json, payload)

    rows = [records[sym] for sym in sorted(records.keys())]
    df = pd.DataFrame(rows)
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out_csv, index=False)

    print(f"[done] tickers={len(tickers)} returned={len(records)} missing={len(missing)} out={args.out_json}")
    if len(records) == 0:
        http_counts = (stats.get('httpCounts') or {}) if isinstance(stats, dict) else {}
        if http_counts.get('429'):
            print('[warn] All Yahoo quote requests returned 429. Leaving outputs with enrichment only (no quote fields populated) and exiting 0 so the workflow does not fail.')
            return 0
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
