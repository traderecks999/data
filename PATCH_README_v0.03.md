# Patch: Align prices_latest.json to universe + add per-ticker timestamps (v0.03)

## What this patch fixes
1) **Discrepancy between universe/tickers and prices_latest.json**
   - Yahoo (via yfinance) can return **partial results** in bulk downloads.
   - Previously, tickers missing from the returned dataframe were silently omitted from `prices_latest.json`.
   - This patch adds multi-pass retries (bulk → smaller bulk → per-symbol) and then backfills remaining misses from the previous snapshot.

2) **Per-ticker extraction timestamp**
   - Each ticker now carries `fetchedAtUtc` (when we fetched or backfilled the price).
   - Backfilled prices retain their earlier `fetchedAtUtc` (or the previous snapshot `asOfUtc`) and are marked `stale: true`.

3) **Prevents drift**
   - Snapshot tickers are now the union of:
     - `asx/tickers_asx.txt`
     - `asx/tickers_extra.txt` (optional)
     - `asx/universe.csv` (if present)

4) **Workflow reliability**
   - Fixes the `asx_prices_snapshot.yml` workflow (ensures it calls `scripts/asx_universe.py` correctly).
   - Adds workflow inputs for `chunk_size`, `force`, and `max_age_minutes`.

## Output schema changes (backwards compatible)
`asx/prices_latest.json` still has a top-level `prices` object. Each ticker entry is a dict:

- `price`: float or null
- `currency`: e.g. "AUD"
- `marketDate`: last trading day date for the bar used (YYYY-MM-DD) or null
- `fetchedAtUtc`: ISO timestamp when fetched/backfilled (or null)
- `source`: bulk / retry_bulk / retry_bulk_small / single / previous / missing
- `stale`: true only when filled from previous snapshot

New top-level fields:
- `countFetchedNow`, `countFilledFromPrevious`, `countMissing`
- `missing` (capped list)
- `stats`

## Recommended chunk_size
- Start at **120** (good balance).
- If you see lots of missing tickers: try **80**.
- If GitHub Actions runtime is too long and missing is low: try **150–200**.

Trade-off:
- Larger chunk_size = fewer HTTP calls but **higher chance of partial responses**.
- Smaller chunk_size = more calls but more reliable completeness.

## How to apply
1) Unzip into the **data repo root** (same level as `.github/`, `scripts/`, `asx/`), overwrite files.
2) Commit + push.
3) GitHub Actions → run:
   - (optional) Universe workflow
   - Prices Snapshot workflow (manual runs default `force=true`)

