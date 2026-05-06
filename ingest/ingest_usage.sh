# Options Data Smoke test on 5 symbols, 30 days
python -m ingest.underlying.build_underlying_daily --market us --lookback 30d --limit 5

# Once happy, full backfill (will take ~2-5 min for 260 symbols)
python -m ingest.underlying.build_underlying_daily --market us --lookback 2y
python -m ingest.underlying.build_underlying_daily --market hk --lookback 2y

# Daily cron / manual run going forward
python -m ingest.underlying.build_underlying_daily --market us --lookback 10d
python -m ingest.underlying.build_underlying_daily --market hk --lookback 10d


python -m ingest.iv.build_iv_history --market us
python -m ingest.iv.build_iv_history --market us --symbols AAPL,AMZN,SPY,QQQ,NVDA,MSTR,TSLA,JMIA --verbose --dry-run