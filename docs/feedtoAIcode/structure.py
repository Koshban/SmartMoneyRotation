"""
Let me provide the complete codebase again, and you check if CASH is complete or anything else needs rejigging.
If all completed now then We need to write a module for backtesting the entire project we have created for CASH smartmoneyrotattion strategies. 
We will need to load 20 years worth of Data , and then be able to backtest for any given period. We should also be able to backtest by default for the 20 years worth of data.

We can load data using the src/*/* components for the last 20 years.
We can create a separate backtest folder to then Test the strategies.
We will also need a method to calculate CAGR so that we can confirm which strategies ( if we need to tweak them ) gives best returns.
"""
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