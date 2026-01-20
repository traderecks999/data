DATA REPO PATCH — Investment Products (ETF/ETP/Managed Funds) Universe Expansion

What this fixes
- Your ASX universe generation now reliably pulls investment products (ETFs/ETPs/quoted funds) from the ASX “Investment Products Monthly Report” XLSX.
- It uses a browser-like User-Agent + Referer (ASX sometimes blocks non-browser agents).
- It parses the XLSX robustly using openpyxl by scanning for the real header row (the report often has note rows above the header).
- If the latest month publishes multiple XLSX variants (e.g., ABS vs non-ABS), it downloads/parses ALL variants for that same month and unions the tickers.

Files changed
- scripts/asx_universe.py

How to apply (GitHub repo: traderecks999/data)
1) Download and unzip this patch.
2) Copy the patched file into your data repo (overwrite):
   - scripts/asx_universe.py
3) Commit and push to GitHub.

How to run
A) Run from GitHub Actions (recommended)
- In GitHub → Actions → run the workflow that builds the ASX universe (whatever you named it; usually “Universe” / “ASX Universe Refresh”).
- After it finishes, confirm these files in the repo updated:
  - asx/universe.csv
  - asx/tickers_asx.txt

B) Run locally (optional)
- From repo root:
  python scripts/asx_universe.py --out-csv asx/universe.csv --out-tickers asx/tickers_asx.txt --extra-tickers asx/tickers_extra.txt

How to verify the fix
- After the universe workflow completes, check if your known investment products appear:
  - grep '^CETF,' asx/universe.csv
  - grep '^CNEW,' asx/universe.csv
  - grep '^IZZ,'  asx/universe.csv
  - grep '^GOLD,' asx/universe.csv
  - grep '^ETPMAG,' asx/universe.csv

Then ensure the price snapshot picks them up
- The daily price snapshot reads asx/tickers_asx.txt, so once the universe is updated, the next scheduled/manual price snapshot should include those codes automatically.

Notes
- If ASX changes the XLSX column names again, the header scan still usually works because it keys off finding a row containing “ASX code”.
- APIR-only rows (long codes) are intentionally skipped because Yahoo Finance won’t price them.
