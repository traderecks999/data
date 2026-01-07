# traderecks999/data

A public, modular **data cache repo** for market/public datasets.

Design goals:
- Keep **private** stuff (your holdings, cash, etc.) on your NAS.
- Store **public** market data snapshots here (prices, universes, etc.).
- Use GitHub Actions to refresh snapshots on a schedule (within free minutes).

## Current datasets

### ASX: universe + price snapshots
Files:
- `asx/universe.csv` — ASX listed companies (official ASX CSV, with fallback).
- `asx/tickers_asx.txt` — tickers list used for snapshots (Yahoo format like `BHP.AX`).
- `asx/prices_latest.json` — latest snapshot of prices (bulk).
- `asx/history/` — optional archived snapshots (pruned automatically).

Workflows:
- **Universe (weekly):** updates `universe.csv` + `tickers_asx.txt`
- **Prices (twice daily):** updates `prices_latest.json` on ASX trading days

## How Much integration (private app)
Point your app to:
`https://raw.githubusercontent.com/traderecks999/data/main/asx/prices_latest.json`

## Notes
- The universe is sourced from the official ASX "ASXListedCompanies.csv" (free).
- Price snapshots use `yfinance` bulk download (history), not quote endpoints.



## Import note
When running scripts via `python scripts/<name>.py`, imports should be `from common import ...` (not `from scripts.common ...`) because Python sets the script directory on `sys.path`.
