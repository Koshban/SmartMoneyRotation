# CASH — Composite Adaptive Signal Hierarchy

## What Is This?

CASH is a quantitative rotation-detection system that identifies which
ETFs, sectors, and stocks are receiving institutional capital inflows
("smart money rotation") and translates that signal into a ranked,
risk-managed portfolio with concrete position sizes and stop-losses.

It answers one question every day:
**"Where is the money moving, and how should my portfolio respond?"**

---

## Architecture Overview

CASH is organised into five layers, each feeding the next:

Layer 1: DATA Load OHLCV prices from parquet/DB/API
↓
Layer 2: COMPUTE Technical indicators → Relative Strength
→ 5-Pillar Composite Scoring
→ Market Breadth → Sector RS
↓
Layer 3: STRATEGY Per-ticker 6-gate signal filter
Multi-stock portfolio construction
↓
Layer 4: OUTPUT Cross-sectional rankings
Portfolio-level BUY/HOLD/SELL signals
Signal strength (conviction scoring)
↓
Layer 5: REPORTS Recommendation report (text + HTML)
Rebalance plan vs current holdings
Backtest performance

## The Five Pillars

Every ticker is scored on five dimensions, each producing a sub-score
in [0, 1].  The weighted average is the **composite score** — the
single number that drives ranking and portfolio selection.

### Pillar 1 — Rotation (30%)
*"Is smart money rotating into this name?"*

Measures relative strength vs benchmark (SPY).  The core signal is the
**RS slope** — the direction of the stock÷benchmark ratio.  When the
slope turns positive, money is flowing in before the crowd notices.

Sub-components: RS z-score (35%), RS regime (30%), RS momentum (20%),
volume confirmation (15%).

The RS regime is a 2×2 matrix:

| | Slope > 0 | Slope ≤ 0 |
|---|---|---|
| **EMA > SMA** | LEADING | WEAKENING |
| **EMA ≤ SMA** | IMPROVING ★ | LAGGING |

★ "Improving" is the sweet spot — early rotation before the RS line
crosses above its trend.  This is where the edge lives.

### Pillar 2 — Momentum (25%)
*"Is price action confirming the rotation?"*

RSI in the trending sweet spot (40–70), MACD histogram strength,
and ADX confirming a directional trend (>25).

### Pillar 3 — Volatility (15%)
*"Is risk/reward favourable for entry?"*

Moderate volatility scores highest (not dead money, not panic).
ATR-based risk assessment and Amihud illiquidity penalty (liquid
names get capital first).

### Pillar 4 — Microstructure (20%)
*"Is institutional volume backing the move?"*

OBV slope (accumulation), A/D line slope (weighted buying pressure),
and relative volume (1.5–2.5× average = institutional interest).

### Pillar 5 — Breadth (10%)
*"Is the broad market confirming the move?"*

Universe-level signal: advance/decline ratios, % above moving averages,
McClellan oscillator, new highs vs lows.  Same value for every ticker
on a given day — acts as a tide gauge.

---

## The Six Gates

Before any ticker earns a BUY signal, it must pass six quality gates
(per-ticker, in `strategy/signals.py`):

1. **Score threshold** — `score_adjusted ≥ 0.60`
2. **RS regime** — stock in `leading` or `improving`
3. **Sector regime** — sector ETF not `lagging`
4. **Breadth regime** — market not `weak`
5. **Momentum streak** — 3 consecutive days with score > 0.50
6. **Cooldown** — not recently exited (5-day anti-churn)

All six must pass → `sig_confirmed = 1`

Then the portfolio-level allocator (`output/signals.py`) adds:
- Cross-sectional rank filter with hysteresis (enter ≤ rank 5, exit > rank 8)
- Position limits (max 5 active)
- Breadth circuit breaker (block new entries when market weak)

---

## Signal Flow (Detailed)
Raw OHLCV for ticker + benchmark
↓
compute/indicators.py ~30 technical columns
↓
compute/relative_strength.py RS ratio, slope, z-score, momentum, regime
↓
compute/scoring.py 5 pillar sub-scores → composite [0,1]
↓
compute/sectors.py Sector tailwind/headwind → score_adjusted
↓
strategy/signals.py 6-gate filter → sig_confirmed, sig_exit
↓
output/rankings.py Cross-sectional rank per day
↓
output/signals.py BUY / HOLD / SELL / NEUTRAL + conviction
↓
reports/recommendations.py Final report with allocations
reports/portfolio_view.py Rebalance plan vs current holdings

## Universe

### Tier 1: ETF Universe (core rotation engine)
~80 ETFs across: Broad Market (SPY, QQQ, IWM, DIA, MDY), 11 GICS
Sectors, ~30 Thematic ETFs (SOXX, IBIT, URA, ARKK, etc.), International,
HK-listed, Fixed Income, and Commodities.

### Tier 2: Single Names (future stock-picking layer)
~150 individual stocks organised by theme: AI, Semiconductors, Quantum,
Nuclear, Megacap, Bitcoin, Biotech, Defense, Clean Energy, Fintech,
India, HK/China, Momentum, etc.

Each theme has an ETF proxy for relative-strength benchmarking.

---

## Portfolio Construction

After scoring and ranking, the portfolio builder:

1. **Extracts snapshots** — latest-day data for every ticker
2. **Filters candidates** — only `sig_confirmed == 1`
3. **Ranks** — by composite score with incumbent bonus (+2% to reduce churn)
4. **Selects positions** — top N, enforce 30% max sector concentration
5. **Sizes positions** — water-fill normalisation with [2%, 8%] per-position limits
6. **Adjusts for breadth** — scale total exposure by market regime:
   - Strong → 100%
   - Neutral → 80%
   - Weak → 50%

Position sizing scales linearly with composite score (higher score →
larger position) and inversely with breadth weakness.

Stop-losses are set at 2× ATR below entry price.

---

## Directory Structure

smartmoneyrotation/
├── common/ Configuration & universe definitions
│ ├── config.py All tunable parameters (single source of truth)
│ ├── universe.py Ticker lists + category helpers
│ ├── credentials.py DB/broker credentials (not in git)
│ └── expiry.py Option expiry date utilities
│
├── compute/ Pure math — no DB, no opinions
│ ├── indicators.py ~30 technical indicators
│ ├── relative_strength.py RS vs benchmark (Pillar 1)
│ ├── scoring.py 5-pillar composite scoring
│ ├── breadth.py Universe-level market breadth
│ └── sectors.py Sector RS + tailwind merge
│
├── strategy/ Trade decision logic
│ ├── signals.py Per-ticker 6-gate signal filter
│ └── portfolio.py Multi-stock portfolio construction
│
├── output/ Cross-sectional analysis
│ ├── rankings.py Universe rankings per day
│ ├── signals.py Portfolio-level BUY/HOLD/SELL
│ └── reports.py Unified strategy reports
│
├── pipeline/ Orchestration
│ ├── runner.py Single-ticker pipeline + batch
│ └── orchestrator.py Multi-phase coordinator
│
├── portfolio/ Execution & simulation
│ ├── sizing.py Position sizing algorithms
│ ├── rebalance.py Drift-based rebalancing
│ ├── risk.py Risk metrics (VaR, drawdown, etc.)
│ └── backtest.py Historical simulation engine
│
├── reports/ Final deliverables
│ ├── recommendations.py Recommendation report (text/HTML)
│ └── portfolio_view.py Rebalance plan generator
│
├── src/ Data ingestion (DB + API)
│ ├── db.py Database connection
│ ├── schema.py Table definitions
│ ├── load_db.py DB read utilities
│ ├── ingest_cash.py OHLCV ingestion (IBKR/yfinance)
│ └── ingest_options.py Options chain ingestion
│
├── data/ Local data cache
│ ├── india_cash.parquet
│ ├── universe_ohlcv.parquet
│ └── options/
│
├── utils/
│ └── run_logger.py Rich dual-output logger
│
├── tests/
├── main.py CLI entry point
├── requirements.txt
└── pyproject.toml

## Configuration

Everything lives in `common/config.py`.  Key sections:

| Section | What it controls |
|---------|-----------------|
| `INDICATOR_PARAMS` | All technical indicator periods/windows |
| `RS_PARAMS` | Relative strength lookbacks and thresholds |
| `SCORING_WEIGHTS` | Pillar weights and sub-component weights |
| `SIGNAL_PARAMS` | Entry/exit thresholds, gates, position sizing |
| `PORTFOLIO_PARAMS` | Max positions, sector caps, rebalance threshold |
| `BREADTH_PARAMS` | McClellan, thrust, regime classification |
| `BREADTH_PORTFOLIO` | Breadth → exposure scaling |
| `SECTOR_RS_PARAMS` | Sector RS computation + tailwind/headwind |
| `SECTOR_ETFS` | 11 GICS sector ETF mappings |
| `TICKER_SECTOR_MAP` | Individual stock → sector assignments |

---

## Key Design Decisions

**Why RS slope instead of RS ratio?**
The ratio tells you who's winning; the slope tells you who's *starting*
to win.  We want to detect rotation *as it begins*, not after it's obvious.

**Why 5 pillars instead of a single score?**
Each pillar captures a different dimension.  A ticker with strong RS but
collapsing volume (no institutional backing) is a trap.  Pillar agreement
(4/5 or 5/5 bullish) is itself a confirmation signal.

**Why per-ticker gates AND portfolio-level signals?**
The gates answer "is this ticker trade-worthy?" (absolute quality).
The portfolio signals answer "does this ticker make the portfolio?"
(relative ranking + capacity).  Both are needed.

**Why breadth as a separate pillar AND a gate?**
As a pillar, breadth contributes to the composite score (a weak-breadth
environment dampens all scores slightly).  As a gate, it blocks new
entries entirely in weak markets.  As a portfolio dial, it reduces total
exposure.  Three layers of protection.

**Why hysteresis in rank-based signals?**
Without it, a ticker at the rank 5/6 boundary would flip BUY/SELL
daily.  The band (enter ≤ 5, exit > 8) eliminates this churn.

---

## Backtest Engine

The backtester (`portfolio/backtest.py`) simulates the full signal
pipeline day-by-day:

1. Mark to market
2. Process SELL signals
3. Compute target weights (vol-based sizing)
4. Process BUY entries (priority by composite score)
5. Check drift-based rebalancing for HOLD positions
6. Record equity, positions, trades

Metrics computed: total return, CAGR, Sharpe, Sortino, Calmar,
max drawdown, max DD duration, win rate, profit factor, avg win/loss,
total commission.

Trade execution: signals on day T execute at day T close (configurable
delay to T+1).  Commission (10 bps) and slippage (5 bps) applied.

---

## Requirements

sqlalchemy>=2.0
ib_insync==0.9.86
sqlalchemy==2.0.36
psycopg2-binary==2.9.10
pandas==2.2.3
pyarrow==23.0.1
python-dotenv==1.2.2
numpy==1.26.4
ta==0.11.0
yfinance==1.3.0
