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
from typing import List, Optional, Tuple
from io import BytesIO
from urllib.parse import urljoin

import requests

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import openpyxl
except Exception:
    openpyxl = None

ASX_CSV_URLS = [
    "https://www.asx.com.au/asx/research/ASXListedCompanies.csv",
]

INVESTMENT_PRODUCTS_INDEX_URL = "https://www.asx.com.au/issuers/investment-products/asx-investment-products-monthly-report"

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



_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}

def _pick_latest_investment_products_xlsx(links: List[str]) -> Optional[str]:
    """Pick the latest ASX investment products XLSX link from a list of hrefs."""
    best = None
    best_key = None
    rx = re.compile(r"/asx-investment-products-([a-z]{3})-(\d{4})-abs\.xlsx", re.I)
    for href in links:
        m = rx.search(href)
        if not m:
            continue
        mon = _MONTHS.get(m.group(1).lower())
        yr = int(m.group(2))
        if not mon:
            continue
        key = (yr, mon)
        if best_key is None or key > best_key:
            best_key = key
            best = href
    return best

def fetch_latest_investment_products_xlsx(timeout: float = 20.0) -> Optional[str]:
    """Fetch the latest ASX investment products XLSX URL from the ASX monthly report page."""
    try:
        r = requests.get(INVESTMENT_PRODUCTS_INDEX_URL, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400:
            return None
        html = r.text or ""
        # We only want the excel XLSX links (not the PDFs)
        links = re.findall(r'href="([^"]+/excel/asx-investment-products-[^"]+-abs\.xlsx)"', html, flags=re.I)
        if not links:
            # fallback: any xlsx in the page that matches the known pattern
            links = re.findall(r'href="([^"]+asx-investment-products-[^"]+-abs\.xlsx)"', html, flags=re.I)
        href = _pick_latest_investment_products_xlsx(links) if links else None
        if not href:
            return None
        return urljoin(INVESTMENT_PRODUCTS_INDEX_URL, href)
    except Exception:
        return None

def read_asx_investment_products_etps(timeout: float = 30.0) -> List[CompanyRow]:
    """Return ASX ETP/ETF codes from the official ASX investment products monthly XLSX.

    This is used to expand the universe beyond ASXListedCompanies.csv (which excludes ETP issuers).
    """
    if openpyxl is None:
        print("[warn] openpyxl not available; skipping ETP expansion", file=sys.stderr)
        return []

    url = fetch_latest_investment_products_xlsx(timeout=timeout)
    if not url:
        print("[warn] could not locate ASX investment products XLSX; skipping ETP expansion", file=sys.stderr)
        return []

    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 400:
            print(f"[warn] failed to download investment products XLSX: {r.status_code}", file=sys.stderr)
            return []
        wb = openpyxl.load_workbook(BytesIO(r.content), read_only=True, data_only=True)
        # Prefer the detailed ETP list sheet
        sheet_name = None
        for cand in ["Spotlight ETP List", "Spotlight ETPs", "ETP List"]:
            if cand in wb.sheetnames:
                sheet_name = cand
                break
        if not sheet_name:
            # best-effort: any sheet containing both 'ETP' and 'List'
            for sn in wb.sheetnames:
                low = sn.lower()
                if "etp" in low and "list" in low:
                    sheet_name = sn
                    break
        if not sheet_name:
            return []
        ws = wb[sheet_name]

        rows: List[CompanyRow] = []

        # Find header row + column indexes
        header_row_idx = None
        col_code = col_name = col_type = None

        for i, row in enumerate(ws.iter_rows(values_only=True), start=1):
            cells = [str(c).strip().lower() for c in row if c is not None]
            joined = " ".join(cells)
            if "asx" in joined and "code" in joined and "fund" in joined:
                header_row_idx = i
                # map headers by position
                for j, c in enumerate(row):
                    if c is None:
                        continue
                    h = str(c).strip().lower()
                    if "asx" in h and "code" in h:
                        col_code = j
                    elif "fund" in h and "name" in h:
                        col_name = j
                    elif h == "type" or ("type" in h and "code" not in h):
                        col_type = j
                break

        if header_row_idx is None or col_code is None:
            return []

        for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
            code = row[col_code] if col_code < len(row) else None
            if code is None:
                continue
            code = str(code).strip().upper()
            # Stop if we hit the end of table
            if not re.match(r"^[A-Z0-9]{1,6}$", code):
                continue
            name = ""
            if col_name is not None and col_name < len(row) and row[col_name] is not None:
                name = str(row[col_name]).strip()
            itype = ""
            if col_type is not None and col_type < len(row) and row[col_type] is not None:
                itype = str(row[col_type]).strip()
            rows.append(CompanyRow(code=code, name=name or code, sector="ETF/ETP", industry=itype))
        return rows
    except Exception as e:
        print(f"[warn] error parsing investment products XLSX: {e}", file=sys.stderr)
        return []

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
    etps = read_asx_investment_products_etps()

    # Merge + dedupe by code (prefer company record if duplicates)
    merged = {r.code: r for r in rows if r.code}
    for r in etps:
        if r.code and r.code not in merged:
            merged[r.code] = r

    rows_all = list(merged.values())
    rows_all.sort(key=lambda r: r.code)

    write_universe_csv(rows_all, Path(args.out_csv))
    write_tickers_txt(rows_all, Path(args.out_tickers))

    print(f"[ok] wrote {args.out_csv} and {args.out_tickers} ({len(rows_all)} tickers) | companies={len(rows)} etps={len(etps)}")

if __name__ == "__main__":
    main()
