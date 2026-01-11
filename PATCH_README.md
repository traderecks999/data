# Patch #2 — data repo: expand universe (ETFs/ETPs) + improve snapshot coverage

This patch targets your **public data cache repo** (the one producing `asx/prices_latest.json`).

It fixes:
1) **Universe too small (missing ETFs/ETPs like `IVV.AX`)**
- The universe generator now merges:
  - Official ASX `ASXListedCompanies.csv`
  - **ETP/ETF codes from the official ASX Investment Products Monthly Report (XLSX)**
- Output tickers should jump from ~1500–1800 to **~2000+**, depending on delistings.

2) **Snapshot missing many tickers**
- The price snapshot script now uses:
  - bulk yfinance pass (medium chunks)
  - second bulk pass (smaller chunks)
  - bounded individual fallback for stubborn misses
This generally increases coverage without blowing up runtime.

## Files overwritten
- `scripts/asx_universe.py`
- `scripts/asx_prices_snapshot.py`
- `requirements.txt` (adds `openpyxl`)
- `README.md`

## Apply patch
1) Unzip this patch into your data repo root (the folder that contains `scripts/`, `asx/`, `.github/`), and allow overwrite.
2) Commit + push to `main`.

## Run workflows (recommended order)
1) **ASX Universe – Weekly Refresh** (workflow_dispatch)  
   Generates:
   - `asx/universe.csv`
   - `asx/tickers_asx.txt`  ✅ should now include `IVV.AX`

2) **ASX Prices – Snapshot** (workflow_dispatch)  
   Generates:
   - `asx/prices_latest.json` ✅ should now contain an entry for `IVV.AX`

## Local run (optional)
```bash
python -m pip install -r requirements.txt
python scripts/asx_universe.py --out-csv asx/universe.csv --out-tickers asx/tickers_asx.txt
python scripts/asx_prices_snapshot.py --tickers asx/tickers_asx.txt --out asx/prices_latest.json --force
```

## Quick verification checks
- `grep IVV.AX asx/tickers_asx.txt` returns a match.
- `jq '.prices["IVV.AX"]' asx/prices_latest.json` returns a record with a `price`.
