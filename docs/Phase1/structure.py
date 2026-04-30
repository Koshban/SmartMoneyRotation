"""
smartmoneyrotation/
├── backtest/
    ├── phase1/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── engine.py
│   ├── metrics.py
│   ├── strategies.py
│   ├── comparison.py
│   ├── runner.py
    ├── phase2/
        ├── __init__.py
        ├── data_source.py        # Date-aware data wrapper
        ├── engine.py             # Day-by-day replay loop
        ├── tracker.py            # Virtual portfolio
        ├── metrics.py            # Performance calculations
        ├── compare.py            # Side-by-side comparison + Rich output
        └── run_backtest.py       # CLI entry point
        └── diagnostics.py
├── backtest_results/
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── credentials.py
│   ├── universe.py
│   ├── sector_map.py
│   └── expiry.py
│
├── compute/
│   ├── __init__.py
│   ├── breadth.py
│   ├── indicators.py
│   ├── relative_strength.py
│   ├── scoring.py
│   └── sector_rs.py
│
├── data/
|   ├── backtest_results/
│   ├── us_cash.parquet
│   ├── us_options.parquet
│   ├── hk_cash.parquet
│   ├── in_cash.parquet
│   ├── universe_cash.parquet
│   ├── staging.json
│   └── options/
|
├── logs/
│
├── pipeline/
│   ├── __init__.py
│   ├── runner.py
│   └── orchestrator.py
│
├── portfolio/
│   ├── __init__.py
│   ├── backtest.py
│   ├── rebalance.py
│   ├── risk.py
│   └── sizing.py
│
refactor/
|    └── common/
        ├── __init__.py
        ├── config_refactor.py
        ├── market_config_v2.py
        ├── universe_loader_v2.py
├── pipeline_v2.py
├── report_v2.py 
└── runner_v2.py         
└── strategy/
    ├── __init__.py
    ├── adapters_v2.py
    ├── breadth_v2.py
    ├── enrich_v2.py
    ├── portfolio_v2.py
    ├── regime_v2.py
    ├── rotation_v2.py
    ├── rs_v2.py
    ├── scoring_v2.py
    └── signals_v2.py
│   ├── tests/
│   │   └── test_refactor_smoke.py
│
├── reports/
│   ├── __init__.py
│   ├── portfolio_view.py
│   ├── recommendations.py
│   ├── weekly_report.py
│   └── html_report.py
│
├── results/
│   ├── comparison_report.txt
│   └── run_logger.py
│   └── signals/
│
├── scripts/
│   ├── run_market.py
│   ├── run_bounce_scan.py
│   ├── run_combined.py
│   ├── signal_writer.py
│   ├── run_strategy.py
│   └── run_bounce_scan.py
│
├── src/
│   ├── __init__.py
│   ├── db.py
│   ├── schema.py
│   ├── load_db.py
│   ├── loader.py
│   ├── ingest_cash.py
│   └── ingest_options.py
│
├── strategy_phase1/
│   ├── __init__.py
│   ├── signals.py
│   ├── portfolio.py
│   ├── rotation.py
│   ├── rotation_filters.py
│   └── convergence.py
│
├── output/
│   ├── __init__.py
│   ├── rankings.py
│   ├── reports.py
│   └── signals.py
│
├── tests/
│   ├── __init__.py
│   └── test_connections.py
│
├── utils/
│   ├── __init__.py
│   └── run_logger.py
│   └── display_results.py
│
├── docs/
├── logs/
├── main.py
├── pyproject.toml
├── requirements.txt
└── .gitignore

"""
"""
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── credentials.py
│   ├── universe.py
│   ├── sector_map.py
│   └── expiry.py
├── data/
|   ├── backtest_results/
│   ├── us_cash.parquet
│   ├── us_options.parquet
│   ├── hk_cash.parquet
│   ├── in_cash.parquet
│   ├── universe_cash.parquet
│   ├── staging.json
│   └── options/
├── refactor/
|    └── common/
        ├── __init__.py
        ├── config_refactor.py
        ├── market_config_v2.py
        ├── universe_loader_v2.py
├── pipeline_v2.py
├── report_v2.py 
└── runner_v2.py         
└── strategy/
    ├── __init__.py
    ├── adapters_v2.py
    ├── breadth_v2.py
    ├── enrich_v2.py
    ├── portfolio_v2.py
    ├── regime_v2.py
    ├── rotation_v2.py
    ├── rs_v2.py
    ├── scoring_v2.py
    └── signals_v2.py

"""