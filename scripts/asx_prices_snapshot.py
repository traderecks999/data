#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import random
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
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tickers file not found: {path}")
    out = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    # dedupe, preserve order
    seen=set()
    final=[]
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
    """Decide whether this run should do an "open+1h" or "close" snapshot.

    Windows are generous because cron timing differs across DST.
    We compute the window in Australia/Sydney time.
    """
    sydney = dt_utc.astimezone(ZoneInfo("Australia/Sydney"))
    hhmm = sydney.strftime("%H:%M")
    # open+1h window: 10:55–11:20
    if "10:55" <= hhmm <= "11:20":
        return "open_plus_1h"
    # close window: 15:55–16:20
    if "15:55" <= hhmm <= "16:20":
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

def fetch_prices_bulk(
    tickers: List[str],
    period: str = "5d",
    interval: str = "1d",
    chunk_size: int = 150,
    second_pass_chunk: int = 60,
    max_individual: int = 200,
    sleep_s: float = 0.15,
) -> Dict[str, Tuple[float, str]]:
    """Fetch latest close prices for many Yahoo symbols.

    Uses yfinance bulk download first (stable), then a smaller-chunk second pass,
    then (optionally) an individual-ticker fallback for the remaining misses.

    Returns: {symbol: (price, currency)}. Currency is set to AUD by default.
    """
    prices: Dict[str, Tuple[float, str]] = {}

    def latest_close_for(df: pd.DataFrame, sym: str) -> Optional[float]:
        try:
            if df is None or getattr(df, "empty", True):
                return None
            if isinstance(df.columns, pd.MultiIndex):
                if sym not in df.columns.get_level_values(0):
                    return None
                sub = df[sym]
                if sub is None or getattr(sub, "empty", True):
                    return None
                if "Close" not in sub.columns:
                    return None
                s = sub["Close"].dropna()
            else:
                if "Close" not in df.columns:
                    return None
                s = df["Close"].dropna()
            if s is None or len(s) == 0:
                return None
            return float(s.iloc[-1])
        except Exception:
            return None

    def bulk_download(batch: List[str]) -> Dict[str, float]:
        tickers_str = " ".join(batch)
        # retry per chunk
        for attempt in range(1, 4):
            try:
                df = yf.download(
                    tickers=tickers_str,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=True,
                    threads=True,
                    progress=False,
                )
                if df is None or getattr(df, "empty", True):
                    raise RuntimeError("empty dataframe")
                out: Dict[str, float] = {}
                for sym in batch:
                    v = latest_close_for(df, sym)
                    if v is not None:
                        out[sym] = v
                return out
            except Exception as e:
                if attempt == 3:
                    print(f"[warn] chunk download failed after 3 attempts: {e}")
                else:
                    time.sleep(0.5 * attempt + random.random())
        return {}

    # 1) First pass: bulk download in medium chunks
    missing: List[str] = []
    for idx, ch in enumerate(chunked(tickers, chunk_size), start=1):
        got = bulk_download(ch)
        for sym, v in got.items():
            prices[sym] = (float(v), "AUD")
        for sym in ch:
            if sym not in prices:
                missing.append(sym)
        if idx % 5 == 0:
            print(f"[info] fetched {len(prices)}/{idx*chunk_size} so far...")

    # 2) Second pass: retry missing in smaller chunks
    if missing:
        print(f"[info] second pass for {len(missing)} missing tickers (smaller chunks)")
        missing2: List[str] = []
        for ch in chunked(missing, second_pass_chunk):
            got = bulk_download(ch)
            for sym, v in got.items():
                prices[sym] = (float(v), "AUD")
            for sym in ch:
                if sym not in prices:
                    missing2.append(sym)
        missing = missing2

    # 3) Individual fallback for a bounded number of tickers
    if missing:
        todo = missing[:max_individual]
        print(f"[info] individual fallback for {len(todo)} still-missing tickers (cap={max_individual})")
        for sym in todo:
            try:
                df = yf.Ticker(sym).history(period=period, interval=interval, auto_adjust=True)
                v = latest_close_for(df, sym)
                if v is not None:
                    prices[sym] = (float(v), "AUD")
            except Exception:
                pass
            time.sleep(sleep_s + random.random() * sleep_s)

    return prices


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
    ap.add_argument("--force", action="store_true", help="Ignore time window checks")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)

    if not is_asx_trading_day(now):
        print("[skip] not an ASX trading day")
        return

    window = within_window_sydney(now)
    if not args.force and window is None:
        print("[skip] outside snapshot windows (Sydney time)")
        return

    if not args.force and recent_enough(args.out, max_age_minutes=args.max_age_minutes):
        print("[skip] latest snapshot is recent; avoiding duplicate run")
        return

    tickers = read_tickers(args.tickers)
    if not tickers:
        raise RuntimeError("No tickers found")

    prices = fetch_prices_bulk(tickers, chunk_size=args.chunk_size)
    asof = utc_now_iso()
    asof_perth = datetime.now(ZoneInfo('Australia/Perth')).strftime('%Y-%m-%d %H:%M:%S %Z')

    payload = {
        "dataset": "asx/prices",
        "asOfUtc": asof,
        "asOfPerth": asof_perth,
        "window": window or "manual",
        "source": "yfinance_bulk_history",
        "countTickers": len(tickers),
        "countPrices": len(prices),
        "prices": {sym: {"price": float(px), "currency": ccy} for sym, (px, ccy) in prices.items()},
    }

    write_json(args.out, payload)
    print(f"[ok] wrote {args.out} with {len(prices)}/{len(tickers)} prices")

    if args.keep_history:
        Path(args.history_dir).mkdir(parents=True, exist_ok=True)
        stamp = asof.replace(":", "").replace("-", "")
        hist_path = str(Path(args.history_dir) / f"prices_{stamp}.json")
        write_json(hist_path, payload)
        prune_history(args.history_dir, keep_days=45)
        print(f"[ok] wrote history snapshot {hist_path}")

if __name__ == "__main__":
    main()
