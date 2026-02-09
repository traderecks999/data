# Patch: Fundamentals hardening + universe enrichment

This patch updates the ASX fundamentals pipeline to:

- Enrich each fundamentals row with `name`, `sector`, `industry`, `asset_type` from `asx/universe.csv`
- Add per-ticker telemetry fields:
  - `fundamentalsFetchStatus`, `fundamentalsFetchHttpStatus`, `fundamentalsFetchError`, `fundamentalsFetchedAtUtc`
- Store deep fundamentals into `asx/fundamentals_cache.json` under `bySymbol` (previously your cache meta was updating but no per-symbol rows were persisted)
- Only advance the rotate cursor when the deep-success-rate is healthy (configurable). Otherwise, cursor stays put and the workflow can fail so you notice.
- Lower default concurrency to 4 and add jitter/backoff + cookie seeding for Yahoo.
- Keep price consistency by overwriting/deriving price-driven fields using `asx/prices_latest.json`.

Workflow: `.github/workflows/asx_fundamentals_daily.yml`
- scheduled daily 01:30 UTC (09:30 Perth)
- manual runs let you change `summary_per_run`, `concurrency`, `min_success_rate` from the Actions UI.
