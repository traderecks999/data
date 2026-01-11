# Data Repo Hotfix v0.03.1 – Snapshot action crash fix

## What this fixes
Your GitHub Actions run was **writing `asx/prices_latest.json` successfully** but then crashed with:

`NameError: name 'prices' is not defined`

This happened because `scripts/asx_prices_snapshot.py` printed a second summary line referencing a variable that no longer exists.

## Changes
- `scripts/asx_prices_snapshot.py`
  - Removed the redundant print statement that referenced `prices`.
- `.github/workflows/asx_prices_snapshot.yml`
  - Adds an extra log line printing CHUNK/MAXAGE for easier debugging (no behavior change).

## How to apply
1) Unzip this patch into the root of your data repo (overwrite files).
2) Commit + push.

## Run
GitHub → Actions → **ASX Prices Snapshot (twice daily)** → Run workflow.

If you still see many `YFRateLimitError` or delisted symbols:
- The job should still complete successfully and write the JSON.
- Delisted/unsupported tickers will remain in `missing`, but the file will be produced and kept aligned.
