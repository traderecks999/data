# traderecks999/data

A public, modular **data cache repo** for market/public datasets.

Design goals:
- Keep **private** stuff (your holdings, cash, etc.) on your NAS.
- Store **public** market data snapshots here (prices, universes, etc.).
- Use GitHub Actions to refresh snapshots on a schedule (within free minutes).

## Current datasets

### ASX: universe + price snapshots
Files:
- `asx/universe.csv` — ASX listed companies **plus ETP/ETF codes** (official ASX sources, with fallback).
- `asx/universe_latest.json` — JSON mirror of `asx/universe.csv` (same rows; easier for apps to consume).
- `asx/tickers_asx.txt` — tickers list used for snapshots (Yahoo format like `BHP.AX`).
- `asx/prices_latest.json` — latest snapshot of prices (bulk).
- `asx/history/` — optional archived snapshots (pruned automatically).

Workflows:
- **Universe (weekly):** updates `universe.csv` + `universe_latest.json` + `tickers_asx.txt`
- **Prices (twice daily):** updates `prices_latest.json` on ASX trading days



### ASX: fundamentals snapshots
Files:
- `asx/fundamentals_latest.json` — latest fundamentals snapshot (wide; includes a `fieldMap` and per-ticker `fundamentalsFetchedAtUtc`).
- `asx/fundamentals_latest.csv` — CSV mirror of the snapshot (nested/huge text fields removed).
- `asx/fundamentals_latest.xlsx` — Excel workbook with two tabs: `fundamentals` and `field_map`.
- `asx/fundamentals_cache.json` — cache used to refresh deep fundamentals in **rotate** mode without hammering Yahoo.

Workflow:
- **Fundamentals (daily):** refreshes fundamentals on a daily schedule.
  - Default mode is `rotate` (refreshes a slice each run and advances a cursor stored in `fundamentals_cache.json`).
  - Manual runs can override `summary_per_run` on the Actions page.
  - Price-derived fields (e.g. market cap, yield, PE) are overwritten/derived using `asx/prices_latest.json` for consistency.


### Universe fields
`asx/universe.csv` keeps the legacy column names (`sector`, `industry`) but with upgraded meanings:

For **equities**:
- `sector` = **GICS Sector** (tier-1, derived from industry group)
- `industry` = **GICS Industry Group** (tier-2, from ASXListedCompanies.csv)
- `asset_type` = `EQUITY`
- `source` = `ASXListedCompanies.csv`

For **ETP/ETF codes**:
- `asset_type` = `ETF/ETP`
- `sector` = blank (by design)
- `industry` = product type / sheet name (best available descriptor)
- `source` = `ASXInvestmentProductsMonthlyReport`

## How Much / FortuneValley integration
Point your apps to:
- `https://raw.githubusercontent.com/traderecks999/data/main/asx/prices_latest.json`
- `https://raw.githubusercontent.com/traderecks999/data/main/asx/universe_latest.json`

## Notes
- Universe combines the official ASX "ASXListedCompanies.csv" **plus ETP/ETF codes** from the ASX Investment Products Monthly Report (XLSX).
- Price snapshots use `yfinance` bulk download (history), not quote endpoints.

## Import note
When running scripts via `python scripts/<name>.py`, imports should be `from common import ...` (not `from scripts.common ...`) because Python sets the script directory on `sys.path`.
