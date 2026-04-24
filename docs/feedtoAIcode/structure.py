"""
smartmoneyrotation/
└── backtest/
    ├── __init__.py
    ├── data_loader.py        # Download & cache 20 years of OHLCV
    ├── engine.py             # Run pipeline → backtest for any period
    ├── metrics.py            # CAGR, Sharpe, drawdown, annual returns
    ├── strategies.py         # Predefined strategy parameter variants
    ├── comparison.py         # Multi-strategy comparison framework
    └── runner.py             # CLI: python -m backtest.runner
└──common/
   └──  __init__.py
   └── config.py
   └── credentials.py
   └── universe.py
   └── sector_map.py
   └── expiry.py
└──compute/
     └──  __init__.py
     └── breadth.py
     └── indicators.py
     └── relative_strength.py
     └── scoring.py
     └── sector_rs.py
└──data/
     └──india_cash.parquet
     └──universe_cash.parquet
     └── staging.json
     └── options/
└──docs/
    └── DataArchitecture.clj
    └── DataFlow.clj
└──logs/
└──output/
    └──  __init__.py
    └── rankings.py
    └── reports.py
    └── signals.py
└──pipeline/
    └──  __init__.py
    └──  runner.py
    └──  orchestrator.py
└──portfolio/
    └──  __init__.py
    └── backtest.py
    └── rebalance.py
    └── risk.py
    └── sizing.py
└──reports/
    └──  __init__.py
    └──  portfolio_view.py
    └──  recommendations.py
    └──  weekly_report.py
    └──  html_report.py
└──results/
    └──  comparison_report.txt
    └──  run_logger.py
└──scripts/
    └──  run_market.py
    └──  run_strategy.py
    └──  run_bounce_scan.py
└── src/
    └──db.py
	└── __init__.py
	└──  db.py
	└──  schema.py
	└──  load_db.py
	└──  loader.py
    └──  ingest_cash.py
    └──  ingest_options.py
    └──  __init__.py
└──strategy/
    ├── __init__.py
    ├── signals.py          ← per-ticker quality gates (unchanged)
    ├── portfolio.py        ← portfolio construction (unchanged)
    ├── rotation.py         ← top-down rotation engine (unchanged)
    ├── rotation_filters.py
    └── convergence.py      ← NEW: dual-list merge + market dispatcher
└── tests/
    └──  __init__.py
    └── test_connections.py ( and varios other tests )
 └──utils/
    └──  __init__.py
    └──  run_logger.py
 └── main.py  
 └── pyproject.toml         
 └── requirements.txt 
 └── .gitignore
 
"""

"""
smartmoneyrotation/
├── backtest/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── engine.py
│   ├── metrics.py
│   ├── strategies.py
│   ├── comparison.py
│   ├── runner.py
│   └── phase2/
│       ├── __init__.py
│       ├── README.md
│       ├── signal_study.py
│       ├── portfolio_sim.py
│       ├── reversal_study.py
│       └── run_backtests.py
│
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
│   ├── us_cash.parquet
│   ├── hk_cash.parquet
│   ├── india_cash.parquet
│   ├── universe_cash.parquet
│   ├── staging.json
│   └── options/
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
├── refactor/
│   ├── __init__.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_refactor.py
│   │   ├── market_config_v2.py
│   │   └── universe_loader_v2.py
│   ├── strategy/
│   │   ├── __init__.py
│   │   ├── adapters_v2.py
│   │   ├── portfolio_v2.py
│   │   ├── regime_v2.py
│   │   ├── scoring_v2.py
│   │   └── signals_v2.py
│   ├── tests/
│   │   └── test_refactor_smoke.py
│   ├── demo_runner.py
│   ├── pipeline_v2.py
│   ├── report_v2.py
│   └── runner_v2.py
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
│
├── scripts/
│   ├── run_market.py
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
├── strategy/
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
│
├── docs/
├── logs/
├── main.py
├── pyproject.toml
├── requirements.txt
└── .gitignore

"""