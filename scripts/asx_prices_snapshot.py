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
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import exchange_calendars as xcals
import pandas as pd
import yfinance as yf

from common import write_json, utc_now_iso

DEFAULT_TICKERS_FILE = "asx/tickers_asx.txt"
DEFAULT_OUT = "asx/prices_latest.json"
DEFAULT_HISTORY_DIR = "asx/history"

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

    # Optional extra tickers (hand-curated additions)
    extra_path = Path("asx/tickers_extra.txt")
    if extra_path.exists():
        for line in extra_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            s2 = line.strip()
            if not s2 or s2.startswith("#"):
                continue
            out.append(s2)

    # Union in universe.csv symbols too (prevents drift between universe and tickers list)
    uni_path = Path("asx/universe.csv")
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

def is_asx_trading_day(dt_utc: datetime) -> bool:
    cal = xcals.get_calendar("XASX")
    d = dt_utc.date()
    return cal.is_session(pd.Timestamp(d))

def within_window_sydney(dt_utc: datetime) -> Optional[str]:
    """Classify the snapshot window in Australia/Sydney time.

    This is *metadata*, not a gate.
    We rely on GitHub Actions cron for timing, but reruns can happen at any time.

    Windows are generous to tolerate cron drift and DST differences.
    """
    sydney = dt_utc.astimezone(ZoneInfo("Australia/Sydney"))
    hhmm = sydney.strftime("%H:%M")
    # Mid-session (~13:00 Sydney)
    if "12:45" <= hhmm <= "13:15":
        return "mid_session"
    # End-of-session / close (~16:25 Sydney)
    if "16:10" <= hhmm <= "16:40":
        return "close"
    return None

def recent_enough(latest_path: str, max_age_minutes: int) -> bool:
    p = Path(latest_path)
    if not p.exists():
        return False
    try:
        j = json.loads(p.read_text(encoding="utf-8"))
        asof = j.get("asOfUtc") or j.get("as_of") or j.get("asOf")
        if not asof:
            return False
        # parse ISO-ish
        if asof.endswith("Z"):
            asof = asof.replace("Z", "+00:00")
        dt = datetime.fromisoformat(asof).astimezone(timezone.utc)
        age = (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
        return age <= max_age_minutes
    except Exception:
        return False

def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

def _extract_latest_close_and_date(df: pd.DataFrame, sym: str) -> tuple[Optional[float], Optional[str]]:
    """Return (latest_close, market_date_yyyy_mm_dd) for sym from yfinance download dataframe."""
    try:
        if isinstance(df.columns, pd.MultiIndex):
            sub = df[sym]
            close = pd.to_numeric(sub.get("Close"), errors="coerce").dropna()
        else:
            close = pd.to_numeric(df.get("Close"), errors="coerce").dropna()
        if close is None or close.empty:
            return None, None
        px = float(close.iloc[-1])
        if not (math.isfinite(px) and px > 0):
            return None, None
        # market date = index of last close row
        idx = close.index[-1]
        try:
            # pandas Timestamp or datetime-like
            dt = pd.to_datetime(idx).to_pydatetime()
            market_date = dt.date().isoformat()
        except Exception:
            market_date = None
        return px, market_date
    except Exception:
        return None, None


def fetch_prices_bulk(
    tickers: List[str],
    period: str = "7d",
    interval: str = "1d",
    chunk_size: int = 200,
) -> Dict[str, Tuple[float, str, Optional[str]]]:
    """
    Bulk fetch latest close prices for tickers using yfinance.download.

    Returns mapping: sym -> (price, currency, market_date_yyyy_mm_dd)
    NOTE: Bulk calls can return partial results; caller should retry missing tickers.
    """
    prices: Dict[str, Tuple[float, str, Optional[str]]] = {}
    chunks = chunked(tickers, chunk_size)
    for idx, ch in enumerate(chunks, start=1):
        tickers_str = " ".join(ch)
        # retry per chunk
        for attempt in range(1, 4):
            try:
                df = yf.download(
                    tickers=tickers_str,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    threads=True,
                    auto_adjust=True,
                    progress=False,
                )
                for sym in ch:
                    px, mdate = _extract_latest_close_and_date(df, sym)
                    if px is not None:
                        # For ASX .AX tickers, currency is AUD in almost all cases.
                        prices[sym] = (px, "AUD", mdate)
                break
            except Exception as e:
                # mild exponential backoff with jitter
                sleep_s = min(6.0, (attempt ** 2) * 0.6) + random.random() * 0.8
                print(f"[warn] chunk {idx}/{len(chunks)} attempt {attempt}/3 failed: {e}; sleeping {sleep_s:.1f}s")
                time.sleep(sleep_s)
    return prices


def fetch_price_single(sym: str, period: str = "10d", interval: str = "1d") -> Optional[Tuple[float, str, Optional[str]]]:
    """Last-resort fetch for a single symbol."""
    # Try download first (sometimes succeeds when bulk fails)
    try:
        df = yf.download(
            tickers=sym,
            period=period,
            interval=interval,
            threads=False,
            auto_adjust=True,
            progress=False,
        )
        px, mdate = _extract_latest_close_and_date(df, sym)
        if px is not None:
            return (px, "AUD", mdate)
    except Exception:
        pass

    # Then try Ticker().history()
    try:
        hist = yf.Ticker(sym).history(period=period, interval=interval, auto_adjust=True)
        # history() returns single-ticker columns
        px, mdate = _extract_latest_close_and_date(hist, sym)
        if px is not None:
            return (px, "AUD", mdate)
    except Exception:
        pass

    return None


def fetch_prices_resilient(
    tickers: List[str],
    chunk_size: int,
    asof_utc: str,
) -> Tuple[Dict[str, dict], List[str], dict]:
    """
    Fetch prices with retries and produce per-ticker records including fetchedAtUtc.
    Returns: (records, missing, stats)
    """
    # Keep stable order for output
    tickers = list(dict.fromkeys(tickers))

    # records: sym -> {"price":..., "currency":..., "marketDate":..., "fetchedAtUtc":..., "source":...}
    records: Dict[str, dict] = {}
    missing = set(tickers)

    def _merge(found: Dict[str, Tuple[float, str, Optional[str]]], source: str) -> None:
        for sym, (px, ccy, mdate) in found.items():
            records[sym] = {
                "price": float(px),
                "currency": ccy,
                "marketDate": mdate,
                "fetchedAtUtc": asof_utc,
                "source": source,
            }
            missing.discard(sym)

    # Pass 1: bulk
    _merge(fetch_prices_bulk(tickers, chunk_size=chunk_size), "bulk")

    # Pass 2: retry missing with smaller chunks
    if missing:
        retry_chunk = max(40, min(120, chunk_size // 2))
        _merge(fetch_prices_bulk(sorted(missing), chunk_size=retry_chunk), "retry_bulk")

    # Pass 3: retry missing with even smaller chunks
    if missing:
        retry_chunk2 = max(20, min(60, max(20, (chunk_size // 4))))
        _merge(fetch_prices_bulk(sorted(missing), chunk_size=retry_chunk2), "retry_bulk_small")

    # Pass 4: per-symbol fallback
    single_ok = 0
    if missing:
        # keep it bounded so we don't hammer Yahoo; but we still try all if force-run manually.
        for i, sym in enumerate(sorted(missing), start=1):
            rec = fetch_price_single(sym)
            if rec is not None:
                px, ccy, mdate = rec
                records[sym] = {
                    "price": float(px),
                    "currency": ccy,
                    "marketDate": mdate,
                    "fetchedAtUtc": asof_utc,
                    "source": "single",
                }
                single_ok += 1
                missing.discard(sym)
            # tiny jitter to avoid bursts
            time.sleep(0.12 + random.random() * 0.18)

    stats = {
        "requested": len(tickers),
        "bulk_ok": sum(1 for r in records.values() if r.get("source") == "bulk"),
        "retry_ok": sum(1 for r in records.values() if r.get("source") in ("retry_bulk", "retry_bulk_small")),
        "single_ok": single_ok,
    }

    return records, sorted(missing), stats


def prune_history(history_dir: str, keep_days: int=45) -> None:
    p = Path(history_dir)
    if not p.exists():
        return
    cutoff = datetime.now(timezone.utc).timestamp() - keep_days * 86400
    for f in p.glob("prices_*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
        except Exception:
            pass

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default=DEFAULT_TICKERS_FILE)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--history-dir", default=DEFAULT_HISTORY_DIR)
    ap.add_argument("--keep-history", action="store_true", help="Also write dated snapshots into history/")
    ap.add_argument("--max-age-minutes", type=int, default=20, help="Skip if latest snapshot is newer than this")
    ap.add_argument("--chunk-size", type=int, default=200)
    ap.add_argument("--force", action="store_true", help="Run anytime: ignore trading-day, time-window, and recency checks")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)

    if (not args.force) and (not is_asx_trading_day(now)):
        print("[skip] not an ASX trading day (use --force to snapshot the last available close anyway)")
        return

    window = within_window_sydney(now)

    if not args.force and recent_enough(args.out, max_age_minutes=args.max_age_minutes):
        print("[skip] latest snapshot is recent; avoiding duplicate run")
        return

    tickers = read_tickers(args.tickers)
    if not tickers:
        raise RuntimeError("No tickers found")
    asof = utc_now_iso()
    asof_perth = datetime.now(ZoneInfo('Australia/Perth')).strftime('%Y-%m-%d %H:%M:%S %Z')

    # Fetch fresh prices now (with retries). NOTE: market can be closed; we still fetch the latest available close.
    fresh_records, missing_after_fetch, stats = fetch_prices_resilient(
        tickers=tickers,
        chunk_size=args.chunk_size,
        asof_utc=asof,
    )

    # Load previous snapshot to backfill any remaining missing symbols (keeps dataset dense).
    prev_prices: dict = {}
    prev_asof: Optional[str] = None
    out_p = Path(args.out)
    if out_p.exists():
        try:
            prev_j = json.loads(out_p.read_text(encoding="utf-8"))
            prev_prices = prev_j.get("prices") or {}
            prev_asof = prev_j.get("asOfUtc") or prev_j.get("asOf") or prev_j.get("as_of")
            if isinstance(prev_asof, str) and prev_asof.endswith("Z"):
                prev_asof = prev_asof.replace("Z", "+00:00")
        except Exception:
            prev_prices = {}
            prev_asof = None

    filled_from_prev = 0
    full_prices: Dict[str, dict] = {}

    for sym in tickers:
        if sym in fresh_records:
            full_prices[sym] = fresh_records[sym]
            continue

        # Backfill from previous snapshot if available
        prev = prev_prices.get(sym)
        if isinstance(prev, dict) and prev.get("price") is not None:
            rec = dict(prev)
            rec.setdefault("currency", "AUD")
            rec.setdefault("marketDate", None)
            rec.setdefault("fetchedAtUtc", prev_asof)
            rec["source"] = "previous"
            rec["stale"] = True
            full_prices[sym] = rec
            filled_from_prev += 1
        else:
            full_prices[sym] = {
                "price": None,
                "currency": "AUD",
                "marketDate": None,
                "fetchedAtUtc": None,
                "source": "missing",
            }

    missing_final = [s for s, rec in full_prices.items() if rec.get("price") is None]

    payload = {
        "dataset": "asx/prices",
        "asOfUtc": asof,
        "asOfPerth": asof_perth,
        "window": window or "manual",
        "source": "yfinance_bulk_history_resilient",
        "countTickers": len(tickers),
        # How many we fetched fresh in this run (not counting backfilled)
        "countFetchedNow": len(fresh_records),
        "countFilledFromPrevious": filled_from_prev,
        "countMissing": len(missing_final),
        "missing": missing_final[:500],  # cap to keep file readable
        "stats": stats,
        "prices": full_prices,
    }

    write_json(args.out, payload)
    print(f"[ok] wrote {args.out} with fresh={len(fresh_records)} backfill={filled_from_prev} missing={len(missing_final)} total={len(tickers)}")


    if args.keep_history:
        Path(args.history_dir).mkdir(parents=True, exist_ok=True)
        stamp = asof.replace(":", "").replace("-", "")
        hist_path = str(Path(args.history_dir) / f"prices_{stamp}.json")
        write_json(hist_path, payload)
        prune_history(args.history_dir, keep_days=45)
        print(f"[ok] wrote history snapshot {hist_path}")

if __name__ == "__main__":
    main()
