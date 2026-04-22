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

ingest_cash.py [-h] [--market {us,hk,india,all}] [--period PERIOD] [--days DAYS] [--source {yfinance,ibkr}] [--full] [--backfill]

python src/ingest_cash.py --market all --period 20y
python src/ingest_cash.py --market in --period 5d
python src/db/load_db.py --market all --type cash

python src/ingest_options.py --market us      
python src/ingest_options.py --market us    --consolidate                                                                                                                                                 
python src/db/load_db.py --market all --type options 

python src/db/load_db.py --market all --type all
python src/db/load_db.py --market us  --type cash
python src/db/load_db.py --market us  --type options
python src/db/load_db.py --market hk  --type options
python src/db/load_db.py --status

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

#So which should you use?
# If you want quick terminal output with flexibility over which analysis mode to run, use run_strategy.py. 
# If you want a polished HTML report you can save, share, or open in a browser, use run_market.py. 
# Under the hood they call the same pipeline — run_market.py is essentially run_strategy.py full with an HTML rendering step on top.

# For maximum conviction recommendations, this is the command:

python -m scripts.run_strategy full --market US \
    --holdings NVDA,CRWD,CEG,LMT,VST -v