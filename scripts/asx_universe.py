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
from typing import List, Optional, Dict, Tuple

import requests

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore


ASX_LISTED_COMPANIES_CSV = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
ASX_INVESTMENT_PRODUCTS_MONTHLY_REPORT_PAGE = (
    "https://www.asx.com.au/issuers/investment-products/asx-investment-products-monthly-report"
)

MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


@dataclass
class CompanyRow:
    code: str
    name: str
    sector: str = ""
    industry: str = ""


def normalize_code(code: str) -> str:
    code = (code or "").strip().upper()
    # Keep alnum only (ASX codes are alnum; strip spaces, punctuation)
    code = re.sub(r"[^A-Z0-9]", "", code)
    return code


def yahoo_symbol(code: str) -> str:
    c = normalize_code(code)
    return f"{c}.AX" if c and not c.endswith(".AX") else c


def _http_get(url: str, timeout: int = 30) -> requests.Response:
    resp = requests.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": "howmuch-data-repo/0.1 (+github-actions)",
            "Accept": "*/*",
        },
    )
    resp.raise_for_status()
    return resp


def _find_header_start(lines: List[str]) -> int:
    # ASX CSV sometimes has metadata lines before the real header.
    for i, line in enumerate(lines):
        if line.lower().startswith("company name,"):
            return i
    return 0


def read_asx_listed_companies() -> List[CompanyRow]:
    """Equity universe from ASX 'ASXListedCompanies.csv' (companies only)."""
    resp = _http_get(ASX_LISTED_COMPANIES_CSV)
    raw_lines = resp.text.splitlines()
    header_idx = _find_header_start(raw_lines)
    normalized = "\n".join(raw_lines[header_idx:])

    rows: List[CompanyRow] = []

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

    col_name = find_col(["company name", "name"])
    col_code = find_col(["asx code", "asxcode", "code"])
    col_sector = find_col(["gics industry group", "gics sector", "sector"])
    col_industry = find_col(["gics industry", "industry"])

    for r in rows_iter:
        code = normalize_code(str(r.get(col_code, "") if col_code else ""))
        name = str(r.get(col_name, "") if col_name else "").strip()
        sector = str(r.get(col_sector, "") if col_sector else "").strip()
        industry = str(r.get(col_industry, "") if col_industry else "").strip()
        if not code:
            continue
        rows.append(CompanyRow(code=code, name=name, sector=sector, industry=industry))

    return rows


def _score_xlsx_url(url: str) -> Tuple[int, int, int]:
    """Heuristic score (year, month, day) to pick the latest XLSX link."""
    u = url.lower()
    year = 0
    month = 0
    day = 0

    m = re.search(r"/(20\d{2})/", u)
    if m:
        year = int(m.group(1))

    m2 = re.search(r"-(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*-(20\d{2})", u)
    if m2:
        month = MONTHS.get(m2.group(1), 0)
        year = int(m2.group(2))
    else:
        m3 = re.search(r"(20\d{2})[-_/](\d{1,2})", u)
        if m3:
            year = int(m3.group(1))
            month = int(m3.group(2))

    mday = re.search(r"-(\d{1,2})-(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*-(20\d{2})", u)
    if mday:
        day = int(mday.group(1))
        month = MONTHS.get(mday.group(2), month)
        year = int(mday.group(3))

    return (year, month, day)


def _extract_latest_investment_products_xlsx_url(page_html: str) -> Optional[str]:
    # Find all XLSX links
    hrefs = re.findall(r'href="([^"]+\.xlsx)"', page_html, flags=re.IGNORECASE)
    if not hrefs:
        hrefs = re.findall(r"href='([^']+\.xlsx)'", page_html, flags=re.IGNORECASE)
    if not hrefs:
        return None

    abs_candidates: List[str] = []
    all_candidates: List[str] = []
    for h in hrefs:
        if h.startswith("//"):
            url = "https:" + h
        elif h.startswith("http"):
            url = h
        else:
            url = "https://www.asx.com.au" + (h if h.startswith("/") else "/" + h)
        all_candidates.append(url)
        if url.lower().endswith("abs.xlsx"):
            abs_candidates.append(url)

    candidates = abs_candidates or all_candidates
    candidates.sort(key=_score_xlsx_url, reverse=True)
    return candidates[0] if candidates else None


def read_asx_investment_products() -> List[CompanyRow]:
    """ETF/ETP universe from the ASX 'Investment Products Monthly Report'."""
    if pd is None:
        print("[warn] pandas not available; skipping investment products XLSX parsing", file=sys.stderr)
        return []

    try:
        page = _http_get(ASX_INVESTMENT_PRODUCTS_MONTHLY_REPORT_PAGE).text
        xlsx_url = _extract_latest_investment_products_xlsx_url(page)
        if not xlsx_url:
            print("[warn] Could not find XLSX link on monthly report page; skipping ETP expansion", file=sys.stderr)
            return []

        xlsx_bytes = _http_get(xlsx_url, timeout=60).content
        tmp = Path(".tmp_investment_products.xlsx")
        tmp.write_bytes(xlsx_bytes)

        dfs = pd.read_excel(tmp, sheet_name=None, engine="openpyxl")
        tmp.unlink(missing_ok=True)

        rows: List[CompanyRow] = []

        def find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
            lower = {str(c).strip().lower(): c for c in cols}
            for cand in candidates:
                if cand in lower:
                    return lower[cand]
            return None

        code_candidates = [
            "asx code",
            "asx code / apir code",
            "asx code/apir code",
            "asx code/apir",
            "asxcode",
            "code",
        ]
        name_candidates = [
            "investment name",
            "product name",
            "fund name",
            "security name",
            "name",
            "description",
        ]
        type_candidates = [
            "product type",
            "product",
            "structure",
            "category",
            "asset class",
        ]

        for sheet_name, df in dfs.items():
            if df is None or getattr(df, "empty", True):
                continue
            code_col = find_col(list(df.columns), code_candidates)
            if not code_col:
                continue
            name_col = find_col(list(df.columns), name_candidates)
            type_col = find_col(list(df.columns), type_candidates)

            for _, r in df.iterrows():
                raw_code = r.get(code_col)
                code = normalize_code("" if raw_code is None else str(raw_code))
                if not code or len(code) > 7:
                    continue
                name = ("" if name_col is None else str(r.get(name_col) or "")).strip() or "ASX investment product"
                ptype = ("" if type_col is None else str(r.get(type_col) or "")).strip()
                sector = "ETF/ETP"
                industry = (ptype or sheet_name).strip()
                rows.append(CompanyRow(code=code, name=name, sector=sector, industry=industry))

        out: Dict[str, CompanyRow] = {}
        for r in rows:
            if r.code not in out:
                out[r.code] = r
        return list(out.values())

    except Exception as e:
        print(f"[warn] investment products expansion failed: {e}", file=sys.stderr)
        return []


def _read_extra_tickers(extra_path: Path) -> List[str]:
    if not extra_path.exists():
        return []
    out: List[str] = []
    for line in extra_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.upper().endswith(".AX"):
            out.append(s.upper())
        else:
            out.append(yahoo_symbol(s))
    return out


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

    tickers = {yahoo_symbol(r.code) for r in rows if r.code}
    tickers |= set(_read_extra_tickers(out_txt.parent / "tickers_extra.txt"))

    tickers_sorted = sorted({t for t in tickers if t})

    lines = [
        "# ASX universe tickers (Yahoo symbols, .AX)",
        f"# last_extracted_utc: {now_utc}",
        f"# last_extracted_awst: {awst}",
        "# one symbol per line",
        *tickers_sorted,
    ]
    out_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-csv", default="asx/universe.csv")
    ap.add_argument("--out-tickers", default="asx/tickers_asx.txt")
    args = ap.parse_args()

    rows = read_asx_listed_companies()
    etps = read_asx_investment_products()

    merged: Dict[str, CompanyRow] = {r.code: r for r in rows if r.code}
    for r in etps:
        if r.code and r.code not in merged:
            merged[r.code] = r

    final = list(merged.values())
    final.sort(key=lambda r: r.code)

    write_universe_csv(final, Path(args.out_csv))
    write_tickers_txt(final, Path(args.out_tickers))
    print(f"[ok] wrote {args.out_csv} and {args.out_tickers} ({len(final)} tickers)")


if __name__ == "__main__":
    main()
