# Backtesting Framework — Complete Reference

> **File**: `docs/Backtest_Reference.md`
> **Last updated**: 2026-05-01
> **Applies to**: `backtest/phase2/` (v2 pipeline)

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Pipeline Flow (What Actually Happens)](#pipeline-flow)
3. [Configuration Architecture](#configuration-architecture)
4. [Known Issues & Design Debt](#known-issues--design-debt)
5. [Prerequisites: Data Preparation](#prerequisites-data-preparation)
6. [CLI Reference](#cli-reference)
7. [Basic Usage](#basic-usage)
8. [Advanced Usage](#advanced-usage)
9. [Comparison Mode (A/B Testing)](#comparison-mode-ab-testing)
10. [Common Workflows](#common-workflows)
11. [Output Files](#output-files)
12. [Metrics Reference](#metrics-reference)
13. [Configuration Knobs](#configuration-knobs)
14. [Logging & Diagnostics](#logging--diagnostics)
15. [Troubleshooting](#troubleshooting)
16. [Legacy Backtest (Phase 1)](#legacy-backtest-phase-1)

---

## Architecture Overview

```
run_backtest.py
  └─ run_single() or run_compare()
       └─ BacktestEngine.run()             # day loop over trading calendar
            ├─ _run_pipeline(day, tickers)  # calls run_pipeline_v2()
            │    └─ refactor.pipeline_v2.run_pipeline_v2(
            │         tradable_frames, bench_df, breadth_df,
            │         market, leadership_frames, portfolio_params,
            │         config={...}          ← all strategy params
            │       )
            │    returns a dict with key "action_table" or "snapshot" or "actions"
            │
            ├─ _extract_actions(output)     # reads action column from DataFrame
            │    looks for column: "action_v2" > "action" > "signal"
            │    returns {ticker: "BUY"/"SELL"/"HOLD"/"STRONG_BUY"/...}
            │
            └─ tracker.process_signals()    # blind executor, no logic
                 executes BUY/SELL based on position sizing rules
```

### Data Flow

```
Parquet File (data/{MARKET}_cash.parquet)
    │
    ▼
BacktestDataSource.from_parquet()
    │  - loads all tickers into memory
    │  - provides date-windowed slices per day
    │  - handles benchmark separately
    ▼
BacktestEngine
    │  - iterates over trading days
    │  - for each day: slice data → run pipeline → extract actions → execute
    ▼
Results Dict
    │  - equity_curve (daily portfolio value)
    │  - trade_log (every entry/exit)
    │  - daily_log (per-day state)
    ▼
compute_metrics() → metrics dict
    │
    ▼
Console Display + CSV Files
```

---

## Pipeline Flow

What happens inside `run_pipeline_v2()` on each trading day:

```
run_pipeline_v2()
  │
  ├─ 1. compute_composite_v2(latest, weights, params)         [scoring_v2.py]
  │      Inputs: indicator DataFrame, SCORINGWEIGHTS_V2, SCORINGPARAMS_V2
  │      Output: score_composite_v2 (float 0–1 per ticker)
  │
  ├─ 2. Leadership boost (hardcoded +10%)                     [pipeline_v2.py]
  │      scored["score_composite_v2"] += 0.10 * leadership_strength
  │      ⚠ leadership_strength = 0.0 for India (no LEADERSHIP_TICKERS)
  │      ⚠ leadership_strength = 0.0 for HK unless configured
  │
  ├─ 3. apply_signals_v2(scored, params=signal_params)        [signals_v2.py]
  │      Inputs: scored DataFrame, SIGNALPARAMS_V2
  │      Output: sig_confirmed_v2, sig_exit_v2, sig_effective_entry_min_v2
  │      Gates: score threshold, RS regime, sector regime, breadth regime
  │
  ├─ 4. apply_convergence_v2(signaled, params=convergence_params)  [signals_v2.py]
  │      Inputs: signaled DataFrame, CONVERGENCEPARAMS_V2
  │      Output: score_adjusted_v2
  │      ⚠ rotation_rec is never stamped → always defaults to "HOLD"
  │      ⚠ convergence is effectively a no-op (passthrough + minor adjustments)
  │
  └─ 5. _generate_actions(converged, params=action_params)    [pipeline_v2.py]
         Inputs: converged DataFrame, ACTIONPARAMS_V2 (or None)
         Output: action_v2 = "BUY" / "SELL" / "HOLD" / "STRONG_BUY"
         ⚠ IGNORES params — all thresholds hardcoded: 0.90, 0.76, 0.65, 0.62
         ⚠ Re-checks regimes that signals already handled
```

---

## Configuration Architecture

```
config_refactor.py
  │
  ├── VOLREGIMEPARAMS ────→ classify_volatility_regime()     ✅ reads params
  │
  ├── SCORINGWEIGHTS_V2 ──→ compute_composite_v2()           ✅ reads weights
  ├── SCORINGPARAMS_V2  ──→ compute_composite_v2()           ✅ reads params
  │     produces: score_composite_v2
  │
  ├── (hardcoded 0.10) ───→ pipeline leadership boost        ❌ ignores config
  │     scored["score_composite_v2"] += 0.10 * leadership_strength
  │
  ├── SIGNALPARAMS_V2  ──→ apply_signals_v2()                ✅ reads params
  │     produces: sig_confirmed_v2, sig_exit_v2, sig_effective_entry_min_v2
  │     Note: gates sector via blocked_sector_regimes
  │
  ├── CONVERGENCEPARAMS_V2 → apply_convergence_v2()          ✅ reads params
  │     produces: score_adjusted_v2
  │     ⚠ rotation_rec never stamped → always defaults to "HOLD"
  │     ⚠ convergence is effectively score passthrough
  │
  ├── ACTIONPARAMS_V2  ──→ _generate_actions()               ❌ BROKEN
  │     receives params= but NEVER reads from it
  │     ALL thresholds hardcoded internally
  │
  ├── BREADTHPARAMS ──────→ breadth regime classification    ✅ reads params
  │
  └── ROTATIONPARAMS ────→ rotation logic                    ✅ reads params
```

### Config Dict Passed to Pipeline

```python
config = {
    "vol_regime_params":    VOLREGIMEPARAMS,
    "scoring_weights":      SCORINGWEIGHTS_V2,
    "scoring_params":       SCORINGPARAMS_V2,
    "signal_params":        SIGNALPARAMS_V2,       # ← CLI overrides applied here
    "convergence_params":   CONVERGENCEPARAMS_V2,
    "action_params":        ACTIONPARAMS_V2,       # ← currently ignored by code
    "breadth_params":       BREADTHPARAMS,
    "rotation_params":      ROTATIONPARAMS,
    "buy_ranking_params":   {...},                  # engine-level ranking
    "signal_cap_params":    {...},                  # engine-level capping
    "invalidate_cache":     True/False,
}
```

---

## Known Issues & Design Debt

| Issue | Location | Impact | Severity |
|-------|----------|--------|----------|
| `_generate_actions()` ignores `action_params` entirely | `pipeline_v2.py` | Hardcoded thresholds (0.90, 0.76, 0.65, 0.62) cannot be tuned via config | **High** |
| Leadership boost is hardcoded at +10% | `pipeline_v2.py` | Cannot tune leadership sensitivity | Medium |
| `rotation_rec` is never stamped | upstream of `apply_convergence_v2()` | Convergence is a no-op — always defaults to "HOLD" | **High** |
| `_generate_actions()` re-checks regimes | `pipeline_v2.py` | Redundant with `apply_signals_v2()` regime gates | Low |
| No `LEADERSHIP_TICKERS` for India | `config_refactor.py` | Leadership boost always 0.0 for IN market | Medium |
| Breadth disabled for HK/IN | design choice | Pillar 5 = 0, weights redistributed | By design |

### Workarounds in the Engine

The `BacktestEngine` compensates for some pipeline issues:

- **Signal capping layer**: Re-ranks BUY signals by a momentum/vol composite score, limiting how many can fire per day. This partially mitigates the hardcoded thresholds in `_generate_actions()`.
- **Buy ranking params**: Engine applies its own ranking logic on top of pipeline actions, so even if the pipeline is permissive, only the best N signals execute.
- **Trailing stop enforcement**: Engine enforces stops independently of pipeline exit signals.

---

## Prerequisites: Data Preparation

### Download OHLCV Data

```bash
# Minimum: 2 years (enough for most backtests)
python src/ingest_cash.py --market HK --period 2y
python src/ingest_cash.py --market US --period 2y
python src/ingest_cash.py --market IN --period 2y

# All markets at once
python src/ingest_cash.py --market all --period 2y

# Maximum history (~20 years for US, varies by market)
python src/ingest_cash.py --market US --period max

# Recent refresh only (daily use)
python src/ingest_cash.py --market all --period 5d
```

### Optional: Load to PostgreSQL

```bash
python src/db/load_db.py --market all --type cash
```

### Verify Data

Parquet files should exist at:
- `data/US_cash.parquet`
- `data/HK_cash.parquet`
- `data/IN_cash.parquet` (or `india_cash.parquet` depending on config)

The engine will print the date range and ticker coverage on startup.

---

## CLI Reference

```
python -m backtest.phase2.run_backtest [OPTIONS]
```

### Required Arguments

| Flag | Description |
|------|-------------|
| `--market` | Market code: `HK`, `IN`, `US` |
| `--start` | Start date in `YYYY-MM-DD` format |
| `--end` | End date in `YYYY-MM-DD` format |

### Portfolio Parameters

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--capital` | float | 1,000,000 | Initial capital (local currency units) |
| `--max-positions` | int | 25 | Maximum concurrent open positions |

### Strategy Overrides

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--trailing-stop` | float | config default | Trailing stop as decimal (e.g. `0.18` = 18%) |
| `--max-hold` | int | config default | Maximum holding period in trading days |
| `--min-hold` | int | config default | Minimum holding period (anti-churn) |

### Data & Cache

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--data-dir` | str | `data` | Directory containing parquet files |
| `--lookback` | int | 300 | Indicator warmup bars (more = better SMA/RS accuracy) |
| `--fresh` | flag | False | Bypass `.cache/` and recompute all indicators |

### Mode & Debug

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--compare` | flag | False | Run A/B comparison (Baseline vs Variant) |
| `--no-cap` | flag | False | Disable signal capping (see all raw signals) |
| `--debug` | flag | False | Verbose DEBUG-level output to console |

---

## Basic Usage

### Single Market, Default Parameters

```bash
# Hong Kong — full defaults
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20

# United States
python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2026-04-20

# India (data starts later typically)
python -m backtest.phase2.run_backtest --market IN --start 2023-01-01 --end 2026-04-20
```

### With Risk Overrides

```bash
# Tight stops, forced exit after 60 days
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 \
    --trailing-stop 0.15 --max-hold 60

# Wide stops for volatile market, minimum hold to prevent whipsaws
python -m backtest.phase2.run_backtest --market IN --start 2023-01-01 --end 2026-04-20 \
    --trailing-stop 0.25 --min-hold 10

# Only override max hold (use config default for stop)
python -m backtest.phase2.run_backtest --market US --start 2021-01-01 --end 2026-04-20 \
    --max-hold 120
```

### With Portfolio Size Changes

```bash
# Concentrated: fewer positions, more capital per position
python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2026-04-20 \
    --max-positions 8 --capital 2000000

# Diversified: many positions, smaller capital
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 \
    --max-positions 40 --capital 500000
```

---

## Advanced Usage

### Fresh Computation (Cache Bypass)

Use `--fresh` when:
- You've changed indicator calculation code
- You've re-downloaded / updated the parquet data
- You suspect stale cached results
- You want reproducible comparison between two runs

```bash
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --fresh
```

⚠️ First run after `--fresh` will be significantly slower (computing all indicators from scratch). Subsequent runs without `--fresh` use the cache.

### Extended Lookback

The `--lookback` parameter controls how many historical bars are available before the backtest start date for indicator warmup. Default is 300 (~14 months of trading days).

```bash
# More warmup for strategies using 200-day SMAs or long RS windows
python -m backtest.phase2.run_backtest --market US --start 2020-01-01 --end 2026-04-20 \
    --lookback 500
```

If your parquet data doesn't have enough history before `--start`, the engine will use whatever is available and warn you.

### Signal Debugging

```bash
# See all raw signals without capping (diagnostic only — don't use for real results)
python -m backtest.phase2.run_backtest --market HK --start 2024-01-01 --end 2024-03-31 \
    --no-cap --debug

# This reveals:
# - How many BUY signals fire per day (if >50, thresholds are too loose)
# - Whether SELL signals are working
# - Whether breadth/regime gates are blocking everything
```

---

## Comparison Mode (A/B Testing)

### Running a Comparison

```bash
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --compare
```

This runs **Baseline** (from `build_config()`) and **Variant** (from `build_config_b()`) on identical data, then prints a side-by-side comparison table with a winner column.

### Defining the Variant

Edit `build_config_b()` in `run_backtest.py`:

```python
def build_config_b(args):
    """Edit this function to define what you're testing."""
    config = build_config(args)

    # Example 1: Tighter risk management
    config["signal_params"] = dict(config["signal_params"])
    config["signal_params"]["trailing_stop_pct"] = 0.12
    config["signal_params"]["max_hold_days"] = 60

    # Example 2: Different momentum tilt in buy ranking
    config["buy_ranking_params"] = dict(config["buy_ranking_params"])
    config["buy_ranking_params"]["momentum_tilt"] = 0.30

    # Example 3: Fewer signals allowed
    config["signal_cap_params"] = {
        "strong_buy_limit": 10,
        "max_buy_signals": 20,
    }

    return config
```

### Comparison Output Format

```
⚔️  Config Comparison
┌──────────────────┬────────────┬────────────┬──────────────┐
│ Metric           │   Baseline │    Variant │       Winner │
├──────────────────┼────────────┼────────────┼──────────────┤
│ Total Return     │     +42.3% │     +38.1% │  ◄ Baseline  │
│ CAGR             │     +10.2% │      +9.3% │  ◄ Baseline  │
│ Sharpe           │      1.240 │      1.380 │   Variant ►  │
│ Sortino          │      1.510 │      1.720 │   Variant ►  │
│ Max Drawdown     │     -18.2% │     -12.4% │   Variant ►  │
│ Calmar           │      0.560 │      0.750 │   Variant ►  │
│ Volatility       │     -16.4% │     -13.1% │   Variant ►  │
│ ──────────────── │ ────────── │ ────────── │ ──────────── │
│ Win Rate         │      58.3% │      62.1% │   Variant ►  │
│ Profit Factor    │       1.82 │       2.01 │   Variant ►  │
│ Avg PnL          │     +2.34% │     +2.87% │   Variant ►  │
│ Total Trades     │        156 │        112 │              │
│ Avg Holding Days │       34.2 │       28.1 │              │
│ ──────────────── │ ────────── │ ────────── │ ──────────── │
│ Alpha            │     +3.21% │     +2.84% │  ◄ Baseline  │
│ Beta             │       0.72 │       0.58 │              │
│ Info Ratio       │      0.890 │      0.920 │   Variant ►  │
│ ──────────────── │ ────────── │ ────────── │ ──────────── │
│ Avg Positions    │       18.3 │       14.1 │              │
│ Avg Cash %       │      22.1% │      31.4% │  ◄ Baseline  │
│ Avg Signals/Day  │        4.2 │        3.1 │              │
└──────────────────┴────────────┴────────────┴──────────────┘
```

### What to Test (Ideas)

| Hypothesis | What to Change in `build_config_b()` |
|------------|-------------------------------------|
| Tighter stops reduce drawdown | `trailing_stop_pct`: 0.18 → 0.12 |
| Wider stops improve win rate | `trailing_stop_pct`: 0.18 → 0.25 |
| Fewer positions = higher conviction | `max_positions`: 25 → 10, `strong_buy_limit`: 15 → 5 |
| More positions = better diversification | `max_positions`: 25 → 40 |
| Longer holds capture more trend | `max_hold_days`: 120 → 240, `min_hold_days`: 5 → 15 |
| Higher momentum tilt improves CAGR | `momentum_tilt`: 0.50 → 0.70 |
| Lower vol preference avoids crashes | `vol_preference`: 0.30 → 0.50, `min_trailing_vol`: 0.25 → 0.15 |
| Different scoring weights | Change `SCORINGWEIGHTS_V2` ratios |

---

## Common Workflows

### Workflow 1: Initial Strategy Validation

```bash
# 1. Get data
python src/ingest_cash.py --market HK --period 2y

# 2. Run with defaults
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20

# 3. Evaluate:
#    ✅ Positive alpha?
#    ✅ Sharpe > 1.0?
#    ✅ Win rate > 50%?
#    ✅ Max drawdown < benchmark max drawdown?
#    ✅ Profit factor > 1.5?
#    ✅ Avg cash % < 30%? (not too much idle capital)
```

### Workflow 2: Parameter Sweep

```bash
# Trailing stop sweep
for stop in 0.10 0.12 0.15 0.18 0.20 0.22 0.25; do
    echo "=== Stop: $stop ==="
    python -m backtest.phase2.run_backtest --market HK \
        --start 2022-01-01 --end 2026-04-20 --trailing-stop $stop
done

# Position count sweep
for pos in 5 10 15 20 25 30 40; do
    echo "=== Positions: $pos ==="
    python -m backtest.phase2.run_backtest --market HK \
        --start 2022-01-01 --end 2026-04-20 --max-positions $pos
done

# Capital sweep (same positions — tests sizing effects)
for cap in 200000 500000 1000000 2000000 5000000; do
    echo "=== Capital: $cap ==="
    python -m backtest.phase2.run_backtest --market HK \
        --start 2022-01-01 --end 2026-04-20 --capital $cap
done
```

### Workflow 3: Market Regime Analysis

```bash
# Bull market (US 2023 rally)
python -m backtest.phase2.run_backtest --market US --start 2023-01-01 --end 2024-06-30

# Bear market (2022 drawdown)
python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2022-12-31

# High volatility (HK 2022 China crackdown)
python -m backtest.phase2.run_backtest --market HK --start 2022-03-01 --end 2022-12-31

# Recovery (HK 2023)
python -m backtest.phase2.run_backtest --market HK --start 2023-01-01 --end 2023-12-31

# COVID crash + recovery
python -m backtest.phase2.run_backtest --market US --start 2020-01-01 --end 2020-12-31

# Interest rate hiking cycle
python -m backtest.phase2.run_backtest --market US --start 2022-03-01 --end 2023-07-31
```

### Workflow 4: Cross-Market Consistency

```bash
# Same parameters across all markets — does the strategy generalise?
PARAMS="--trailing-stop 0.18 --max-positions 20 --capital 1000000"

python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2026-04-20 $PARAMS
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 $PARAMS
python -m backtest.phase2.run_backtest --market IN --start 2023-01-01 --end 2026-04-20 $PARAMS

# Compare: does Sharpe hold across markets? Is drawdown consistent?
# If US Sharpe=1.5 but HK Sharpe=0.3, the strategy is market-specific.
```

### Workflow 5: Debugging Zero Trades

```bash
# Step 1: Remove all caps, enable debug
python -m backtest.phase2.run_backtest --market HK \
    --start 2024-01-01 --end 2024-03-31 --no-cap --debug

# Step 2: Look at output for:
#   - "0 BUY signals" → pipeline not generating actions
#   - "N BUY signals but 0 executed" → engine filtering them out
#   - "breadth_regime=weak blocking entries" → macro gate active

# Step 3: Check daily_log.csv
#   Column: buy_signals, sell_signals, positions_held, cash_pct

# Step 4: Check if data is present
#   The startup message shows "Loaded X/Y tickers (date_range)"
#   If X=0 or date range doesn't cover your period, that's the problem
```

### Workflow 6: Reproducing a Specific Day's Signals

```bash
# Run a very short period with full debug to see exactly what happened
python -m backtest.phase2.run_backtest --market HK \
    --start 2024-03-15 --end 2024-03-20 --debug --fresh

# The log file will contain per-ticker scores, signals, and actions for those days
# Check: logs/backtest/backtest_HK_*.log
```

---

## Output Files

### Directory Structure

```
backtest_results/
├── HK/
│   ├── metrics.csv          # All computed performance metrics
│   ├── equity_curve.csv     # Daily portfolio + benchmark value
│   ├── trades.csv           # Every trade with entry/exit details
│   └── daily_log.csv        # Per-day portfolio state
│
├── HK/baseline/             # (comparison mode — config A)
│   ├── metrics.csv
│   ├── equity_curve.csv
│   ├── trades.csv
│   └── daily_log.csv
│
└── HK/variant/              # (comparison mode — config B)
    ├── metrics.csv
    ├── equity_curve.csv
    ├── trades.csv
    └── daily_log.csv
```

### File Schemas

#### `metrics.csv`

| Column | Description |
|--------|-------------|
| Metric | Metric name |
| Value | Metric value (float or string) |

#### `equity_curve.csv`

| Column | Description |
|--------|-------------|
| date | Trading date |
| portfolio_value | Total portfolio value (cash + positions) |
| benchmark_value | Buy-and-hold benchmark value |
| cash | Cash held |
| positions_count | Number of open positions |
| drawdown | Current drawdown from peak |

#### `trades.csv`

| Column | Description |
|--------|-------------|
| ticker | Stock ticker |
| entry_date | Date position opened |
| exit_date | Date position closed (NaT if still open) |
| entry_price | Average entry price |
| exit_price | Average exit price |
| shares | Number of shares |
| pnl | Profit/loss in currency |
| pnl_pct | Profit/loss as percentage |
| holding_days | Days held |
| exit_reason | Why exited (trailing_stop, max_hold, signal_sell, etc.) |

#### `daily_log.csv`

| Column | Description |
|--------|-------------|
| date | Trading date |
| portfolio_value | End-of-day portfolio value |
| cash | Cash available |
| cash_pct | Cash as % of portfolio |
| positions_held | Number of positions |
| buy_signals | BUY signals generated by pipeline |
| sell_signals | SELL signals generated |
| buys_executed | Positions actually opened |
| sells_executed | Positions actually closed |
| strong_buys | STRONG_BUY signals count |

---

## Metrics Reference

### Performance Metrics

| Metric | Key | What It Tells You | Good Values |
|--------|-----|-------------------|-------------|
| Total Return | `total_return` | Cumulative P&L | > benchmark |
| CAGR | `annualized_return` | Annualised compound growth | > benchmark CAGR |
| Volatility | `annualized_vol` | Annualised standard deviation | < benchmark (ideally) |
| Sharpe Ratio | `sharpe_ratio` | Return per unit of total risk | > 1.0 good, > 1.5 excellent |
| Sortino Ratio | `sortino_ratio` | Return per unit of downside risk | > Sharpe |
| Max Drawdown | `max_drawdown` | Worst peak-to-trough decline | > -20% for equity |
| Max DD Duration | `max_dd_duration_days` | Longest time underwater | < 180 days |
| Calmar Ratio | `calmar_ratio` | CAGR / abs(Max Drawdown) | > 0.5 |

### Risk-Adjusted Alpha

| Metric | Key | What It Tells You | Good Values |
|--------|-----|-------------------|-------------|
| Alpha (Jensen) | `alpha` | Excess return vs benchmark, risk-adjusted | > 0% |
| Beta | `beta` | Sensitivity to benchmark | 0.5–0.8 (less volatile than market) |
| Tracking Error | `tracking_error` | Divergence from benchmark | Depends on style |
| Information Ratio | `information_ratio` | Alpha / Tracking Error | > 0.5 good, > 1.0 excellent |

### Trade Statistics

| Metric | Key | What It Tells You | Good Values |
|--------|-----|-------------------|-------------|
| Total Trades | `total_trades` | Activity level | Market-dependent |
| Win Rate | `win_rate` | % of profitable trades | > 50% |
| Avg Win | `avg_win_pct` | Average winning trade size | As high as possible |
| Avg Loss | `avg_loss_pct` | Average losing trade size | Small (contained by stops) |
| Profit Factor | `profit_factor` | Gross profit / Gross loss | > 1.5 good, > 2.0 excellent |
| Avg PnL | `avg_pnl_pct` | Average trade return | > 0% (positive expectancy) |
| Median PnL | `median_pnl_pct` | Median trade return | > 0% |
| Best Trade | `best_trade_pct` | Largest single win | Context-dependent |
| Worst Trade | `worst_trade_pct` | Largest single loss | Should be < trailing stop |
| Avg Holding Days | `avg_holding_days` | Time in positions | Style-dependent |
| Expectancy ($) | `expectancy_dollar` | Expected $ per trade | > 0 |

### Portfolio Utilisation

| Metric | Key | What It Tells You | Good Values |
|--------|-----|-------------------|-------------|
| Avg Positions | `avg_positions` | Average concurrent positions | Relative to max |
| Max Positions Held | `max_positions_held` | Peak position count | ≤ max_positions |
| Total Buy Signals | `total_buy_signals` | Pipeline signal volume | Context-dependent |
| Total Sell Signals | `total_sell_signals` | Exit signal volume | Should be > 0 |
| Avg Signals/Day | `avg_daily_signals` | Daily signal frequency | 1–10 typical |
| Avg Cash % | `avg_cash_pct` | Idle capital | < 30% (else cash drag) |

### Benchmark Comparison

| Metric | Key |
|--------|-----|
| Benchmark Total Return | `benchmark_total_return` |
| Benchmark CAGR | `benchmark_ann_return` |
| Benchmark Volatility | `benchmark_ann_vol` |
| Benchmark Sharpe | `benchmark_sharpe` |
| Benchmark Sortino | `benchmark_sortino` |
| Benchmark Max DD | `benchmark_max_dd` |
| Benchmark Calmar | `benchmark_calmar` |

---

## Configuration Knobs

### Signal Parameters (`SIGNALPARAMS_V2`)

These control when the pipeline says BUY or SELL:

| Parameter | Effect | Typical Range |
|-----------|--------|---------------|
| `entry_score_threshold` | Min composite score to consider entry | 0.55–0.75 |
| `exit_score_threshold` | Score below which exit triggers | 0.30–0.50 |
| `trailing_stop_pct` | Trailing stop distance | 0.10–0.25 |
| `max_hold_days` | Force exit after N days | 60–180 |
| `min_hold_days` | Don't exit before N days | 3–10 |
| `blocked_sector_regimes` | Regime labels that block entry | ["lagging"] |
| `required_rs_regimes` | RS regimes allowed for entry | ["leading", "improving"] |
| `momentum_streak_days` | Consecutive days score must be above threshold | 2–5 |
| `cooldown_days` | Days after exit before re-entry allowed | 5–15 |

### Scoring Weights (`SCORINGWEIGHTS_V2`)

Control relative importance of each pillar:

| Pillar | Default Weight | What It Captures |
|--------|---------------|------------------|
| Rotation | 30% | Relative strength vs benchmark |
| Momentum | 25% | RSI, MACD, ADX confirmation |
| Volatility | 15% | Risk/reward favorability |
| Microstructure | 20% | Institutional volume patterns |
| Breadth | 10% | Market-level participation |

⚠️ For HK and India, breadth is disabled (set to 0) and weights are redistributed to momentum and microstructure.

### Buy Ranking Parameters (Engine-Level)

Control how the engine ranks competing BUY signals:

| Parameter | Default | Effect |
|-----------|---------|--------|
| `momentum_tilt` | 0.50 | Weight of momentum in ranking score |
| `vol_preference` | 0.30 | Weight of inverse volatility in ranking |
| `min_trailing_vol` | 0.25 | Min annualised vol to consider (filters dead stocks) |
| `min_rszscore` | -0.50 | Min RS z-score to consider |
| `vol_window` | 60 | Lookback for volatility calculation |

### Signal Cap Parameters (Engine-Level)

Control maximum daily signal throughput:

| Parameter | Default | Effect |
|-----------|---------|--------|
| `strong_buy_limit` | 15 | Max STRONG_BUY signals per day |
| `max_buy_signals` | 25 | Max total BUY+STRONG_BUY per day |

Use `--no-cap` to disable both (set to 999).

---

## Logging & Diagnostics

### Log Files

Every run creates a log file at:
```
logs/backtest/backtest_{MARKET}_{YYYYMMDD_HHMMSS}.log
```

The log contains:
- Full DEBUG-level output (regardless of `--debug` flag)
- Per-day pipeline execution details
- Signal counts and which tickers triggered
- Position changes (entries, exits, stop hits)
- Any warnings (missing data, fallback behaviour)
- Timing information

### Console Output Levels

| Mode | Console Shows |
|------|---------------|
| Default | INFO: progress bars, summaries, warnings |
| `--debug` | DEBUG: every signal, every day's details |

### What to Look For in Logs

```
# Healthy run:
INFO  Day 2024-03-15: 5 BUY, 2 SELL, 18 positions, cash=22%
INFO  Day 2024-03-16: 3 BUY, 1 SELL, 19 positions, cash=18%

# Problem — no signals:
INFO  Day 2024-03-15: 0 BUY, 0 SELL, 0 positions, cash=100%
WARNING  breadth_regime=weak blocking all entries

# Problem — too many signals:
INFO  Day 2024-03-15: 47 BUY (capped to 25), 0 SELL
WARNING  signal_cap applied: 47 → 25

# Problem — all exits:
INFO  Day 2024-03-15: 0 BUY, 25 SELL, 0 positions, cash=100%
WARNING  trailing stops triggered for 25 positions (market-wide drawdown?)
```

---

## Troubleshooting

### Data Issues

| Problem | Symptom | Fix |
|---------|---------|-----|
| Parquet file missing | "Parquet file not found" error | `python src/ingest_cash.py --market {X} --period 2y` |
| Start date before data | "Loaded 0 tickers" or very few trading days | Adjust `--start` to after data begins |
| Stale data | Results differ from expected | Re-ingest: `python src/ingest_cash.py --market {X} --period 2y` |
| Missing tickers | "Missing N tickers" warning | Normal for newer listings; check universe.py |

### Signal Issues

| Problem | Symptom | Fix |
|---------|---------|-----|
| Zero trades | `total_trades = 0`, `avg_cash_pct = 100%` | Run with `--no-cap --debug`, check breadth regime |
| Too few trades | < 10 trades over 2 years | Lower thresholds in config, check regime gates |
| Too many trades (churning) | > 500 trades/year, low win rate | Increase `min_hold_days`, raise entry threshold |
| All signals on same day | Spiky trading | Check if signal cap is working (`--debug`) |
| Only BUYs, no SELLs | Positions held forever until max_hold | Check exit threshold, verify trailing stop |
| Only SELLs, no BUYs | Immediate exits | Entry threshold too low (enters bad positions) |

### Performance Issues

| Problem | Symptom | Fix |
|---------|---------|-----|
| Negative alpha | Strategy underperforms benchmark | Review signal quality; possibly too much cash drag |
| Sharpe < 0.5 | Poor risk-adjusted returns | Tighten stops, raise entry bar, test with `--compare` |
| Drawdown > 30% | Excessive losses | Implement `--trailing-stop 0.15`, reduce positions |
| High cash % (> 40%) | Capital sitting idle | Regime gates too strict; lower thresholds |
| Low cash % (< 5%) | Always fully invested | May need position limits or regime scaling |

### Runtime Issues

| Problem | Symptom | Fix |
|---------|---------|-----|
| Slow (> 10 min) | First run on large universe | Normal — building cache. Use `--fresh` only when needed |
| Memory error | OOM crash | Reduce universe size or `--lookback` |
| Cache corruption | Inconsistent results | Delete `.cache/backtest/` and run with `--fresh` |
| Import error | Module not found | Check you're running from repo root: `python -m backtest.phase2.run_backtest` |

### Cache Management

```bash
# View cache size
du -sh .cache/backtest/

# Clear all cached indicators (forces recomputation)
rm -rf .cache/backtest/

# Or use --fresh flag (equivalent but doesn't delete files)
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --fresh
```

⚠️ **Important**: `.cache/` is in `.gitignore`. Never commit cache files to git (they can be 500MB+). If you accidentally did, see the repo's git history cleanup docs.

---

## Legacy Backtest (Phase 1)

The older backtest module at `backtest/` (not `backtest/phase2/`) uses the v1 pipeline:

```bash
# Download historical data (20 years)
python -m backtest.data_loader --years 20

# Run default backtest
python -m backtest.runner

# Compare predefined strategy variants (from backtest/strategies.py)
python -m backtest.runner --compare

# Custom period
python -m backtest.runner --start 2010-01-01 --end 2026-04-16
```

### Differences from Phase 2

| Aspect | Phase 1 (`backtest/`) | Phase 2 (`backtest/phase2/`) |
|--------|----------------------|------------------------------|
| Pipeline | v1 (original scoring) | v2 (refactored scoring + signals) |
| Config | `common/config.py` | `phase2/common/config_refactor.py` |
| Signal generation | `strategy/signals.py` | `phase2/strategy/signals_v2.py` |
| Convergence | `strategy/convergence.py` | `phase2/strategy/` (partially broken) |
| CLI | `backtest.runner` | `backtest.phase2.run_backtest` |
| A/B testing | `--compare` (fixed variants) | `--compare` (editable `build_config_b`) |
| Signal capping | No | Yes (buy ranking + caps) |
| Cache support | No | Yes (`.cache/backtest/`) |

Use Phase 1 for:
- Long-horizon testing (10–20 years) where you have the data
- Comparing against the original v1 strategy
- Quick runs without v2 pipeline complexity

Use Phase 2 for:
- Current strategy development
- Parameter tuning with CLI overrides
- A/B testing with full control over config

---

## Benchmarks

| Market | Benchmark Ticker | Description |
|--------|-----------------|-------------|
| US | `SPY` | S&P 500 ETF |
| HK | `2800.HK` | Tracker Fund of Hong Kong (Hang Seng) |
| IN | `NIFTYBEES.NS` | Nippon India Nifty BeES (Nifty 50) |

The benchmark is used for:
- Computing alpha, beta, tracking error, information ratio
- Relative strength calculations (RS ratio = stock / benchmark)
- Side-by-side return comparison in output tables

---

## Quick Reference Card

```bash
# ─── DAILY ─────────────────────────────────────────────────────
# Refresh data + quick backtest validation
python src/ingest_cash.py --market all --period 5d
python -m backtest.phase2.run_backtest --market HK --start 2024-01-01 --end 2026-04-20

# ─── WEEKLY ────────────────────────────────────────────────────
# Full data refresh + comprehensive backtest
python src/ingest_cash.py --market all --period 2y
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20
python -m backtest.phase2.run_backtest --market US --start 2022-01-01 --end 2026-04-20
python -m backtest.phase2.run_backtest --market IN --start 2023-01-01 --end 2026-04-20

# ─── RESEARCH ──────────────────────────────────────────────────
# A/B test a hypothesis
# 1. Edit build_config_b() in run_backtest.py
# 2. Run:
python -m backtest.phase2.run_backtest --market HK --start 2022-01-01 --end 2026-04-20 --compare

# ─── DEBUGGING ─────────────────────────────────────────────────
# Something looks wrong
python -m backtest.phase2.run_backtest --market HK --start 2024-01-01 --end 2024-03-31 \
    --debug --fresh --no-cap
# Then check: logs/backtest/backtest_HK_*.log

