# Data repo hotfix patch (v2)

## What this fixes
1) Manual workflow runs on weekends now work:
   - Previously `scripts/asx_prices_snapshot.py` always skipped on non-trading days.
   - Now `--force` bypasses the trading-day guard (it will snapshot the latest available Yahoo prices).

2) Workflow YAMLs are made complete + safe for manual dispatch:
   - `asx_prices_snapshot.yml`
   - `asx_universe_weekly.yml`

## How to apply
1) Unzip into the *root of your data repo* (the one that contains `.github/`, `scripts/`, `asx/`).
2) Overwrite files.
3) Commit + push to `main`.
4) In GitHub Actions:
   - Run **ASX Prices Snapshot (twice daily)** manually with `force=true`.
   - Confirm `asx/prices_latest.json` updates (and includes `IVV.AX` if it exists in `asx/tickers_asx.txt`).

## Quick verification
- Check workflow logs for:
  - `[info] forced run on a non-trading day`
  - `[ok] wrote asx/prices_latest.json ...`


### Optional: force-include a ticker
If a specific ETF/ETP is missing from the universe, create `asx/tickers_extra.txt` and add one symbol per line, e.g.:
- IVV.AX

The snapshot script will automatically merge this file if it exists.
