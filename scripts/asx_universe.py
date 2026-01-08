#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv as pycsv
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List, Optional

import requests

try:
    import pandas as pd
except Exception:
    pd = None

ASX_CSV_URLS = [
    "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
]

@dataclass
class CompanyRow:
    code: str
    name: str
    sector: str = ""
    industry: str = ""

def normalize_code(code: str) -> str:
    code = (code or "").strip().upper()
    code = re.sub(r"[^A-Z0-9]", "", code)
    return code

def yahoo_symbol(code: str) -> str:
    c = normalize_code(code)
    return f"{c}.AX" if c and not c.endswith(".AX") else c

def _find_header_start(lines: list[str]) -> int:
    # Some ASX CSVs include a short preamble; find the real header row.
    for i, line in enumerate(lines):
        ll = line.strip().lower()
        if ("company name" in ll) and ("asx code" in ll) and ("gics" in ll):
            return i
    return 0

def read_asx_listed_companies(timeout: float = 20.0) -> List[CompanyRow]:
    last_exc: Optional[Exception] = None
    for url in ASX_CSV_URLS:
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            raw_lines = resp.text.splitlines()
            header_idx = _find_header_start(raw_lines)
            normalized = "\n".join(raw_lines[header_idx:])

            if pd is not None:
                from io import StringIO
                df = pd.read_csv(StringIO(normalized))
                rows_iter = df.to_dict(orient="records")
                col_map = {str(c).strip().lower(): c for c in df.columns}
            else:
                rows_iter = list(pycsv.DictReader(normalized.splitlines()))
                col_map = {str(c).strip().lower(): c for c in (rows_iter[0].keys() if rows_iter else [])}

            def find_col(cands: List[str]) -> Optional[str]:
                for cand in cands:
                    if cand in col_map:
                        return col_map[cand]
                return None

            code_col = find_col(["asx code", "asx code.", "asx", "code", "ticker"]) or "ASX code"
            name_col = find_col(["company name", "name", "company"]) or "Company name"
            sector_col = find_col(["gics industry group", "gics industry group."]) or "GICS industry group"
            industry_col = find_col(["industry group", "industry"]) or "Industry group"

            out: List[CompanyRow] = []
            for r in rows_iter:
                code = normalize_code(str(r.get(code_col, "")))
                name = str(r.get(name_col, "")).strip()
                sector = str(r.get(sector_col, "")).strip()
                industry = str(r.get(industry_col, "")).strip()
                if code and name:
                    out.append(CompanyRow(code=code, name=name, sector=sector, industry=industry))
            if out:
                return out
        except Exception as e:
            last_exc = e
            continue

    raise RuntimeError(f"Failed to fetch ASX listed companies CSV. Last error: {last_exc}")

def write_universe_csv(rows: List[CompanyRow], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    now_utc = now_dt.isoformat().replace("+00:00", "Z")
    awst = now_dt.astimezone(ZoneInfo("Australia/Perth")).strftime("%Y-%m-%d %H:%M %Z")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = pycsv.writer(f)
        w.writerow(["code", "yahoo_symbol", "name", "sector", "industry", "last_extracted_utc", "last_extracted_awst"])
        for r in rows:
            w.writerow([r.code, yahoo_symbol(r.code), r.name, r.sector, r.industry, now_utc, awst])

def write_tickers_txt(rows: List[CompanyRow], out_txt: Path) -> None:
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    now_utc = now_dt.isoformat().replace("+00:00", "Z")
    awst = now_dt.astimezone(ZoneInfo("Australia/Perth")).strftime("%Y-%m-%d %H:%M %Z")
    tickers = sorted({yahoo_symbol(r.code) for r in rows if r.code})
    lines = [
        "# ASX universe tickers (Yahoo symbols, .AX)",
        f"# last_extracted_utc: {now_utc}",
        f"# last_extracted_awst: {awst}",
        "# one symbol per line",
        *tickers,
    ]
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-csv", default="asx/universe.csv")
    ap.add_argument("--out-tickers", default="asx/tickers_asx.txt")
    args = ap.parse_args()

    rows = read_asx_listed_companies()
    # sort stable
    rows.sort(key=lambda r: r.code)
    write_universe_csv(rows, Path(args.out_csv))
    write_tickers_txt(rows, Path(args.out_tickers))
    print(f"[ok] wrote {args.out_csv} and {args.out_tickers} ({len(rows)} tickers)")

if __name__ == "__main__":
    main()
