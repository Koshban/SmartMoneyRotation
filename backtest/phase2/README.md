# Phase 2 Backtest Scripts for Sandbox SmartMoneyRotation Logic

These scripts are standalone validators for the sandbox strategy logic created in this session. They are designed for you to copy into your repo and adapt to your own PostgreSQL loader.

## Files

- `signal_study.py` — Cross-sectional forward-return study by recommendation bucket and score percentile.
- `portfolio_sim.py` — Portfolio simulation using sandbox ranking and recommendation outputs.
- `reversal_study.py` — Tactical backtest for the selling-exhaustion / reversal-watchlist subsystem.

## Integration expectations

You should replace the placeholder `load_price_history(...)` and `load_universe_history(...)` functions with your own working PostgreSQL loader.

The scripts assume you can produce a dictionary:

```python
{
    'TICKER': pd.DataFrame(...datetime index...)
}
```

with columns expected by the sandbox strategy pipeline, such as:
- `close`, `high`, `low`
- optional precomputed fields like `rsi14`, `adx14`, `relativevolume`, `closevsema30pct`, `closevssma50pct`, `rszscore`, `sectrszscore`, `rotationrec`, `sector`, `theme`

If you do not have those precomputed in your DB, compute them before passing into the scripts or adapt the enrichment step.

## Workflow

1. Load historical data from your DB.
2. Run one of the studies.
3. Export CSV results.
4. Compare bucket monotonicity, hit rate, CAGR, Sharpe, drawdown, and turnover.

These scripts intentionally backtest the sandbox strategy logic only; they do not attempt to call your existing repo backtest engine.
