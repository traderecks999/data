# Patch v0.04 — Fix “outside snapshot windows” skips + align schedule

This patch fixes the situation where the **ASX prices snapshot workflow appears to run, but no data updates**, because the script exits early with:

- `[skip] outside snapshot windows (Sydney time)`

That check was useful as a guard, but it breaks common real-world cases:
- **Re-running** a scheduled job later in the day to recover from a transient Yahoo/rate-limit failure.
- Having a cron time that differs slightly from what the script considers “in-window”.

## What this patch changes

### 1) `scripts/asx_prices_snapshot.py`
- **Window is now metadata only** (still written to JSON as `window: mid_session/close/manual`).
- The script **no longer exits** just because it’s outside a Sydney “window”.
- The `window` classification ranges are updated to match the agreed timing:
  - `mid_session`: **12:45–13:15 Sydney**
  - `close`: **16:10–16:40 Sydney**

The script still:
- Skips **non‑trading days** automatically using the XASX exchange calendar (weekends + ASX public holidays).
- Skips if the existing snapshot is **newer than `--max-age-minutes`**, unless `--force` is used.

### 2) `.github/workflows/asx_prices_snapshot.yml`
- Schedule is aligned to your agreed AEDT times:
  - **02:00 UTC** (≈13:00 Sydney)
  - **05:25 UTC** (≈16:25 Sydney)
- Default `CHUNK` in the workflow is set to **120** (less likely to trigger Yahoo rate-limits / partial failures).

## Install (apply patch)

1) Download and unzip this patch.
2) Copy the two paths into your **data** repo (overwrite existing):
   - `scripts/asx_prices_snapshot.py`
   - `.github/workflows/asx_prices_snapshot.yml`
3) Commit and push:
   ```bash
   git add scripts/asx_prices_snapshot.py .github/workflows/asx_prices_snapshot.yml
   git commit -m "Fix snapshot window skips + align schedule"
   git push
   ```

## How to run it (important)

- **Manual run (recommended):** In GitHub → Actions → **ASX Prices Snapshot (twice daily)** → **Run workflow**.
  - This uses `workflow_dispatch` and, by default, runs with `force=true`.

- **Don’t confuse this with “Re-run jobs”:** If you click **Re-run jobs** on an old scheduled run, GitHub keeps the event as `schedule`.
  - That’s fine now (no window skip), but it won’t let you change inputs.

## Verify success

After a run, open `asx/prices_latest.json` in the repo and check:
- `asOfUtc` is today’s UTC timestamp
- `countTickers` matches your universe
- `countFetchedNow` is high (some missing is normal)

