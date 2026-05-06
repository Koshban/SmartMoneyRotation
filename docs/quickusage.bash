#═══════════════════════════════════════════════════════════════════════════
# Usage.bash — CASH Strategy System: Operations & Execution Guide
#═══════════════════════════════════════════════════════════════════════════

;;   data/
;;   ├── universe_ohlcv.parquet          ← combined cash (all markets)
;;   ├── us_cash.parquet                 ← US equities OHLCV
;;   ├── hk_cash.parquet                 ← HK equities OHLCV
;;   ├── india_cash.parquet              ← India equities OHLCV
;;   ├── us_options.parquet              ← US options (consolidated)
;;   ├── hk_options.parquet              ← HK options (consolidated)
;;   ├── india_options.parquet           ← India options (consolidated)
;;   └── options/
;;       ├── us/
;;       │   ├── AAPL_2026-05-15.csv
;;       │   ├── MSFT_2026-06-19.csv
;;       │   └── ...
;;       ├── hk/
;;       │   └── ...
;;       └── india/
;;           └── ...

ingest_cash.py [-h] [--market {us,hk,in,all}] [--period PERIOD] [--days DAYS] [--source {yfinance,ibkr}] [--full] [--backfill]

python ingest/ingest_cash.py --market all --period 20y 
python ingest/ingest_cash.py --market all --period 3d --source ibkr
python ingest/db/load_db.py --market all --type cash

python ingest/ingest_options.py --market us      
python ingest/ingest_options.py --market us    --consolidate                                                                                                                                                 
python ingestdb/load_db.py --market all --type options 

python ingestdb/load_db.py --market all --type all
python ingestdb/load_db.py --market us  --type cash
python ingestdb/load_db.py --market us  --type options
python ingestdb/load_db.py --market hk  --type options
python ingestdb/load_db.py --status

# Quickest way — sector rotation with quality filter + your holdings
python -m scripts.run_strategy top-down --market US \
    --quality --holdings NVDA,CRWD,CEG,LMT,VST

# Highest conviction — full combined pipeline
python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG

# HK or India (RS ranking only — no sector rotation)
python -m scripts.run_strategy top-down --market HK
python -m scripts.run_strategy top-down --market IN

# Everything, exported to JSON
python -m scripts.run_strategy full --market ALL -o results/report.json

# Generate HTML report and open in browser
python -m scripts.run_market -m US --days 365 --holdings NVDA,CRWD,PANW --open

# HK report
python -m scripts.run_market -m HK --days 180 --open

# Over Exhausted Sell
python run_bounce_scan.py --market US


# Default: US market, auto dates, minimal output
python cash/phase2/runner_v2.py

# US market with explicit date range
python cash/phase2/runner_v2.py --market US --start-date 2024-01-02 --end-date 2025-04-25

# ── Reports & Logging ────────────────────────────────────────────

# Print the plain-text report to terminal
python cash/phase2/runner_v2.py --market US --print-report

# Verbose logging + report (recommended first smoke test)
python cash/phase2/runner_v2.py --market US --print-report -v

# Full flags: date range + verbose + report
python cash/phase2/runner_v2.py --market US --start-date 2024-06-01 --end-date 2025-04-25 --print-report -v

# ── Custom Data Path ─────────────────────────────────────────────

# Point to a specific parquet file
python cash/phase2/runner_v2.py --market US --parquet-path data/us_cash.parquet

# ── Other Markets ────────────────────────────────────────────────

# Hong Kong
python cash/phase2/runner_v2.py --market HK --print-report -v

# India with date range
python cash/phase2/runner_v2.py --market IN --parquet-path data/in_cash.parquet --print-report -v

# ── Single Day Run ───────────────────────────────────────────────

# Run for a single trading day
python cash/phase2/runner_v2.py --market US --start 2025-04-25 --end 2025-04-25 --print-report -v


#So which should you use?
# If you want quick terminal output with flexibility over which analysis mode to run, use run_strategy.py. 
# If you want a polished HTML report you can save, share, or open in a browser, use run_market.py. 
# Under the hood they call the same pipeline — run_market.py is essentially run_strategy.py full with an HTML rendering step on top.

# For maximum conviction recommendations, this is the command:

python -m scripts.run_strategy full --market US \
    --holdings NVDA,CRWD,CEG,LMT,VST -v

# Backtesting The backtest/ module enables testing any strategy variant over up to 20 years of historical data.

Usage:
    python run_bounce_scan.py                    # default market (US)
    python run_bounce_scan.py --market IN        # Indian market
    python run_bounce_scan.py --market US --top 15
    python run_bounce_scan.py --csv              # also save to CSV
    python run_bounce_scan.py --relaxed          # relax filters for more hits


python scripts/run_bounce_scan.py --market US --csv

Usage:
    python ingest/db/schema.py create          # Create all tables
    python ingest/db/schema.py drop --yes      # Drop all tables (confirm required)
    python ingest/db/schema.py recreate --yes  # Drop + Create
    python ingest/db/schema.py status          # Show which tables exist
    python ingest/db/schema.py drop-options --yes  # Drop only options tables