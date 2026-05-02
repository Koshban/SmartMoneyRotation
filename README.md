# CASH — Composite Adaptive Signal Hierarchy

**A multi-market smart money rotation and scoring system for systematic equity allocation.**

CASH detects where institutional capital is flowing across sectors and individual stocks, scores every ticker on five technical dimensions, merges top-down rotation signals with bottom-up scoring for maximum conviction, and produces actionable portfolio recommendations with position sizing, stop-losses, and rebalance plans.

---

## Table of Contents

- [What We Are Trying to Achieve](#what-we-are-trying-to-achieve)
- [Which Strategies We Have Chosen](#which-strategies-we-have-chosen)
- [Why Those Strategies](#why-those-strategies)
- [System Architecture](#system-architecture)
- [Market Coverage](#market-coverage)
- [Data Pipeline](#data-pipeline)
- [Strategy Modes](#strategy-modes)
- [Project Structure & File Reference](#project-structure--file-reference)
- [Quick Start](#quick-start)
- [Daily & Weekly Workflows](#daily--weekly-workflows)
- [Backtesting](#backtesting)
- [Configuration Reference](#configuration-reference)
- [Output & Reports](#output--reports)

---

## What We Are Trying to Achieve

Traditional stock picking is a bottom-up exercise: analyse one company at a time and decide whether to buy. The problem is that even a fundamentally strong stock underperforms when its sector is out of favour. Conversely, a mediocre stock in a leading sector often outperforms a great stock in a lagging one — this is the sector rotation effect, and it accounts for a large fraction of equity return dispersion.

CASH solves this by combining two complementary lenses into a single decision framework:

**Top-down (where is the money going?):** We rank the 11 GICS sectors by composite relative strength against SPY. The top three sectors are "Leading" — that's where institutional capital is flowing. The bottom three are "Lagging" — capital is leaving. Within each leading sector, we rank individual stocks by relative strength blended with a six-dimensional technical quality score to find the strongest names riding the strongest sector tide.

**Bottom-up (which stocks are mechanically sound?):** Every ticker in the universe passes through a full technical scoring pipeline: momentum oscillators (RSI, MACD, ADX), trend structure (moving average alignment), volume confirmation (OBV slope, accumulation/distribution, relative volume), risk assessment (realised volatility, ATR, Amihud illiquidity), and market breadth context. Each dimension becomes a pillar score normalised to 0–1, weighted and summed into a composite.

**Convergence (what should I actually trade?):** When both engines agree on a ticker — strong individual profile AND in a leading sector — that's a STRONG_BUY, the highest conviction signal. When they disagree, that's a CONFLICT flagged for manual review. When both say sell, that's a STRONG_SELL. This dual-list convergence approach eliminates the single-engine blind spots that plague most systematic strategies.

The system covers three markets (US, Hong Kong, India), generates HTML and text reports, computes portfolio allocations with position sizing, and includes a backtesting framework for parameter tuning across 20 years of data.

---

## Which Strategies We Have Chosen

CASH implements four interlocking strategies that work as a hierarchy:

### 1. Sector Rotation via Relative Strength (Top-Down)

The core rotation signal is composite relative strength — a weighted blend of excess returns over three time horizons:

- 40% weight on 21-day return (recent momentum, roughly one month)
- 35% weight on 63-day return (medium-term, roughly one quarter)
- 25% weight on 126-day return (persistence filter, roughly six months)

For each sector ETF (XLK, XLF, XLE, XLV, XLI, XLC, XLY, XLP, XLU, XLRE, XLB), we compute this composite RS versus SPY. Sectors are then ranked 1–11 and tiered into Leading (top 3), Neutral (middle 5), and Lagging (bottom 3).

Within each leading sector, individual US single-name stocks are ranked by their own composite RS versus SPY, optionally blended with a technical quality score (60% RS, 40% quality by default). The quality filter gates on four hard conditions (price above 50 SMA, EMA above SMA, RSI 30–75, ADX ≥ 18) and scores six soft dimensions (MA positioning, RSI zone, volume profile, MACD state, directional strength, volatility regime).

Holdings in lagging sectors receive SELL signals. Holdings in neutral sectors receive REDUCE signals. Holdings in leading sectors with adequate individual RS receive HOLD.

### 2. Five-Pillar Composite Scoring (Bottom-Up)

Every ticker is scored on five pillars, each normalised to 0–1:

**Pillar 1 — Rotation (30%):** Relative strength z-score, RS regime classification (leading/improving/weakening/lagging), RS momentum (acceleration of the rotation), and volume-confirmed RS. This pillar answers whether smart money is rotating into the name.

**Pillar 2 — Momentum (25%):** RSI positioning (sweet spot 40–70), MACD histogram rolling percentile rank, and ADX trend strength. This pillar answers whether price action confirms the rotation.

**Pillar 3 — Volatility (15%):** Realised volatility percentile rank (tent function — moderate is best), ATR percentage of price (same tent logic), and Amihud illiquidity (inverted — liquid names score higher). This pillar answers whether risk/reward is favourable.

**Pillar 4 — Microstructure (20%):** OBV slope percentile rank (rising = accumulation), accumulation/distribution line slope, and relative volume (1.5–2.5× average = institutional interest). This pillar answers whether institutional volume backs the move.

**Pillar 5 — Breadth (10%):** Universe-level market breadth score (advance/decline ratio, McClellan oscillator, percent above 50/200 SMA, new highs vs lows, breadth thrust detection). This pillar is the same value for every ticker on a given day — it acts as a tide gauge that lifts or dampens all signals simultaneously.

The five pillars are weighted, summed, and clipped to produce `score_composite` in [0, 1].

### 3. Market Breadth Regime Overlay

Before any individual ticker is scored, the system computes universe-level breadth indicators from the entire scoring universe. These feed into a regime classification (strong / neutral / weak) that acts at three levels:

At the **scoring level**, breadth is Pillar 5 of the composite — when broad participation is strong, every ticker gets a small lift; when it deteriorates, conviction is dampened.

At the **signal level**, a weak breadth regime blocks new entries entirely (Gate 3 in the per-ticker signal filter) and raises the effective entry score threshold in neutral regimes.

At the **portfolio level**, the target invested percentage scales down: 100% in strong breadth, 80% in neutral, 50% in weak. This is a macro risk dial that reduces exposure automatically when the market environment deteriorates.

### 4. Dual-List Convergence (US Only)

For US markets where both the scoring engine and the rotation engine produce independent signal lists, the convergence layer merges them:

- **STRONG_BUY:** Both engines agree on BUY. Composite score receives a +0.10 convergence boost.
- **BUY_SCORING:** Scoring says BUY, rotation is neutral. Good individual profile but sector isn't leading.
- **BUY_ROTATION:** Rotation says BUY, scoring is neutral. In a leading sector but individual metrics not yet confirmed.
- **CONFLICT:** One says BUY, the other says SELL. Composite receives a −0.05 penalty. Flagged for manual review.
- **STRONG_SELL:** Both engines agree on SELL. Highest conviction exit.

For HK and India, where no sector rotation engine exists (no GICS sector ETFs for those markets), signals pass through from the scoring engine with appropriate convergence labels.

---

## Why Those Strategies

### Why Relative Strength?

Relative strength is one of the most empirically robust factors in equity markets. Academic research (Jegadeesh & Titman 1993, Asness, Moskowitz & Pedersen 2013) demonstrates that stocks that have outperformed over the past 3–12 months tend to continue outperforming over the next 1–6 months. The key insight is that RS is not about absolute returns — it measures which assets are gaining favour relative to a benchmark, which captures the capital rotation dynamics that drive market structure.

By computing RS at both the sector level (11 GICS sectors) and the individual stock level, we capture two distinct but complementary rotation effects: macro capital flows between sectors (driven by economic cycle, interest rates, and thematic shifts) and micro capital flows within sectors (driven by earnings quality, competitive positioning, and institutional discovery).

### Why Multi-Period Blending?

A single lookback window is fragile. The 21-day window catches recent momentum but is noisy and prone to mean-reversion traps. The 126-day window is stable but slow to react to regime changes. Blending three periods (21d at 40%, 63d at 35%, 126d at 25%) gives heavier weight to recent performance (for responsiveness) while requiring persistence across longer horizons (for conviction). This is the same logic behind Faber's "relative momentum" approach — require confirmation across multiple timeframes before committing capital.

### Why Five Pillars Instead of One Score?

A single momentum score collapses all information into one number, making it impossible to diagnose why a signal fired or failed. The five-pillar architecture separates orthogonal information sources: trend direction (RS), trend confirmation (momentum oscillators), risk context (volatility), institutional participation (volume), and macro environment (breadth). Each pillar can be independently weighted, diagnosed, and tuned. When four out of five pillars are bullish and one is bearish, the system knows exactly which dimension is raising the red flag.

### Why a Quality Gate on Rotation Picks?

Pure RS ranking within a sector picks the strongest past performers, but some of those are technically exhausted — RSI at 85, price 20% above the 50 SMA, declining volume. The quality gate filters out names where the RS reading is a lagging artefact of a move that has already played out. The quality score then blends with RS so that among two stocks with similar sector-relative performance, the one with healthier technical structure (trending MAs, RSI in the sweet spot, rising volume) ranks higher.

### Why Breadth as a Portfolio-Level Overlay?

Breadth measures participation. A market can rally on the back of a handful of mega-caps while the average stock declines — this is a narrow rally and historically precedes corrections. By computing advance/decline ratios, McClellan oscillators, percent-above-MA readings, and new-high/low counts across the scoring universe, the system detects whether the market's internal health supports its price level. When breadth is weak, the system reduces exposure even if individual stocks still look strong, because the base rate for new positions succeeding drops significantly in deteriorating breadth environments.

### Why Convergence Instead of Averaging?

Averaging the rotation score with the scoring composite would produce a mushy middle — every ticker gets a blended number that obscures whether either engine actually has conviction. Convergence preserves the distinct opinions: when both agree, conviction is highest; when they disagree, the conflict is surfaced for human judgment. This is particularly valuable for the "conflict" case — a stock in a lagging sector with a strong individual profile (BUY on scoring, SELL on rotation) is a research opportunity, not an automatic trade.

---

## System Architecture
┌─────────────────────────────────────────────────────────────────┐
│ DATA LAYER │
│ │
│ ingest_cash.py → parquet → load_db.py → PostgreSQL │
│ │
│ loader.py reads from: PostgreSQL → Parquet → yfinance │
└──────────────────────────────┬──────────────────────────────────┘
│
┌──────────────────────────────▼──────────────────────────────────┐
│ ORCHESTRATOR (pipeline/) │
│ │
│ Phase 0: Data Loading (loader.py) │
│ Phase 1: Universe Context (breadth + sector RS) │
│ Phase 2: Per-Ticker Pipeline (indicators → RS → scoring) │
│ Phase 2.5: Sector Rotation Engine (US only) │
│ Phase 2.75: Convergence Merge (scoring + rotation) │
│ Phase 3: Cross-Sectional Analysis (rankings + portfolio) │
│ Phase 4: Reports + Optional Backtest │
└──────────────────────────────┬──────────────────────────────────┘
│
┌──────────────────────────────▼──────────────────────────────────┐
│ OUTPUT LAYER │
│ │
│ HTML report (dark-themed dashboard) │
│ Text report (terminal / log-friendly) │
│ JSON export (downstream consumption) │
│ Rebalance plan (current holdings → trades needed) │
└─────────────────────────────────────────────────────────────────┘

---

## Market Coverage

| Market | Benchmark | Top-Down Engine | Bottom-Up Scoring | Convergence |
|--------|-----------|-----------------|-------------------|-------------|
| US | SPY | 11 GICS sector rotation with quality-filtered stock selection | Five-pillar composite scoring | Dual-list merge (STRONG_BUY / CONFLICT / etc.) |
| HK | 2800.HK (Tracker Fund) | Composite RS ranking vs benchmark | Five-pillar scoring (breadth disabled, volume emphasis) | Scoring passthrough |
| India | NIFTYBEES.NS (Nifty BeES) | Composite RS ranking vs benchmark | Five-pillar scoring (breadth disabled, volume emphasis) | Scoring passthrough |

US gets the full dual-engine treatment because the 11 GICS sector ETFs provide clean, liquid benchmarks for sector rotation. HK and India lack equivalent sector ETF infrastructure, so they use scoring-only with RS ranking as the top-down substitute.

---

## Data Pipeline

Data flows through three layers: ingestion, storage, and reading.

**Ingestion** auto-selects the data source based on the requested time window. Historical backfills (>5 days) use yfinance for fast bulk downloads. Recent refreshes (≤5 days) use IBKR TWS for accurate, fresh data. The source selection is automatic but can be overridden.

**Storage** uses parquet files as the primary intermediate format and PostgreSQL as the normalised store. Per-market parquet files (`us_cash.parquet`, `hk_cash.parquet`, `india_cash.parquet`) feed into `load_db.py` for database loading. A combined `universe_ohlcv.parquet` serves as the fast-path for the data loader.

**Reading** is handled exclusively by `loader.py`, which tries PostgreSQL first (fastest, pre-loaded), falls back to parquet (local file), and uses yfinance as the last resort. The compute pipeline never calls ingestion scripts directly — it only reads through the loader.

Full backfill (first time / weekly)
-----
python ingest/ingest_cash.py --market all --period 2y
python ingest/db/load_db.py --market all --type cash

Daily refresh
-----
python ingest/ingest_cash.py --market all --period 5d
python ingest/db/load_db.py --market all --type cash

---

## Strategy Modes

### Top-Down Mode

Runs sector rotation (US) or RS ranking (HK/India) without the full per-ticker scoring pipeline. Fast (~10–30 seconds). Answers "where is the smart money flowing?"

```bash
python -m scripts.run_strategy top-down --market US
python -m scripts.run_strategy top-down --market US --quality --holdings NVDA,CRWD
python -m scripts.run_strategy top-down --market HK


Bottom-Up Mode
Runs the full per-ticker scoring pipeline via the orchestrator (Phases 0–4, skipping rotation and convergence). Thorough (~1–3 minutes per market). Answers "which individual stocks look strongest?"

bash
python -m scripts.run_strategy bottom-up --market US
python -m scripts.run_strategy bottom-up --market HK --lookback 365

Full Mode
Runs everything: bottom-up scoring feeds indicator data into the rotation engine's quality filter, then convergence merges both signal lists. Maximum conviction (~2–5 minutes per market). Answers "what should I buy, sell, or hold?"

bash
python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG
python -m scripts.run_strategy full --market ALL -o results/report.json

Project Structure & File Reference
common/ — Shared Configuration & Universe Definitions
config.py — Central configuration for the entire system. Every tunable parameter lives here: indicator periods, scoring weights, signal thresholds, portfolio constraints, per-market overrides (HK scoring weights, India signal params, etc.), sector ETF mappings, breadth regime thresholds, and the master MARKET_CONFIG dict that downstream code uses to look up any market's settings. Nothing is hard-coded elsewhere.

universe.py — Full investable universe across three tiers. Tier 1 is the ETF universe (broad market, sectors, thematic, international, fixed income, commodities — ~70 US ETFs plus HK ETFs). Tier 1b is the HK scoring universe (~45 tickers: China tech, financials, property, energy, auto, consumer, telecom, healthcare). Tier 1c is the India scoring universe (~50 Nifty large-caps). Tier 2 is the single-name stock universe organised by theme (AI, semiconductors, quantum, nuclear, megacap, data centres, bitcoin, defence, biotech, clean energy, fintech, India small-caps). Helper functions detect market by suffix (.HK, .NS), build per-market ticker lists, and cross-reference themes.

sector_map.py — Maps every ticker in the universe to one of the 11 GICS sectors. Used by the rotation engine to determine which stocks belong to each sector, rank tickers within leading sectors, and flag holdings in lagging sectors. Handles edge cases: crypto miners → Technology, nuclear reactor builders → Industrials, solar hardware → Technology, UBER → Industrials. Includes thematic ETF→sector mappings and non-sector asset classification (broad market, international, fixed income, commodities).

expiry.py — Monthly option expiry date utilities. Calculates the third Friday (US/HK) and last Thursday (India NSE) for option chain downloads. Used by ingest_options.py.

credentials.py — Database and IBKR connection credentials. Not committed to version control.

compute/ — Pure Technical Computation
indicators.py — Computes ~30 technical indicator columns on any OHLCV DataFrame. Returns, RSI (Wilder smoothing), MACD (line, signal, histogram), ADX (+DI/−DI), moving averages (EMA-30, SMA-30, SMA-50 with price-distance percentages), ATR (absolute and percentage), realised volatility (annualised with 5-day change), OBV (with 10-day slope), accumulation/distribution line (CLV-weighted with slope), volume metrics (average, relative, dollar volume), Amihud illiquidity, and VWAP distance. Master function: compute_all_indicators(df).

relative_strength.py — Relative strength versus benchmark — the core rotation signal. Computes RS ratio (stock/benchmark, normalised to 1.0 at start), RS slope (rolling linear regression of smoothed ratio — positive means money rotating in), RS z-score (standardised slope for cross-ticker comparison), RS momentum (short-term slope minus long-term slope — catches early rotation), volume-confirmed RS (RS improvement backed by above-average volume), and RS regime (four-quadrant classification: leading, weakening, lagging, improving — where "improving" is the entry sweet spot). Master function: compute_all_rs(stock_df, bench_df).

scoring.py — Five-pillar composite scoring engine. Takes a DataFrame with indicator columns and RS columns, plus optional breadth scores, and produces pillar sub-scores and a weighted composite. Each pillar uses tailored normalisation: sigmoid for RS z-score, piecewise linear for RSI and ADX, rolling percentile rank for MACD histogram and volume metrics, tent function for volatility (moderate is best). Falls back to four-pillar mode with renormalised weights when breadth is unavailable. Master function: compute_composite_score(df, breadth_scores).

sector_rs.py — Sector-level relative-strength analysis. Downloads OHLCV for the 11 sector ETFs plus SPY, computes RS ratio/slope/z-score/regime per sector (same math as stock-level RS), cross-sectionally ranks sectors each day, and derives a tailwind/headwind value per sector for composite score adjustment. Provides merge_sector_context() to add sector columns to individual stock DataFrames.

breadth.py — Universe-level market breadth analytics. Computes advance/decline (from a universe of stock DataFrames, not exchange-level feeds), McClellan oscillator and summation index, percent above 50/200 SMA, new 252-day highs/lows with hi-lo ratio, up-volume ratio (flags 90% up-volume days), breadth thrust detection (10-day EMA of advance ratio crossing 61.5%), and a composite breadth score (0–1) that feeds the regime classifier (strong/neutral/weak). Master function: compute_all_breadth(universe).

strategy/ — Signal Generation & Decision Logic
signals.py — Per-ticker entry/exit signal generation. Six quality gates that must all pass for sig_confirmed = 1: score above threshold (adjusted for breadth), RS regime in allowed set (leading/improving), sector regime not blocked (lagging), breadth regime not weak, momentum streak (N consecutive days with score > 0.5), and not in cooldown after recent exit. Also computes exit triggers (score collapse below threshold) and position sizing (linear scale from base to max as score increases, scaled down by breadth regime). This is the per-ticker quality filter — it answers "is this ticker trade-worthy today?"

rotation.py — Core smart money rotation engine. Computes composite RS for each sector ETF, ranks sectors into Leading/Neutral/Lagging tiers, picks stocks within leading sectors (ranked by RS blended with quality score from rotation_filters.py), evaluates current holdings against sell rules (sector drift → SELL/REDUCE, individual RS collapse → SELL), and enforces maximum position caps. Returns a RotationResult with sector rankings and per-ticker recommendations.

rotation_filters.py — Technical quality filters for rotation stock selection. Implements the quality gate (four hard checks: price > 50 SMA, EMA > SMA, RSI 30–75, ADX ≥ 18) and the quality score (six sub-components: MA positioning, RSI zone, volume profile, MACD state, ADX + directional bias, volatility regime). The blend_rs_quality() function normalises RS via sigmoid and combines it with the quality score using configurable weights (default 60% RS, 40% quality).

convergence.py — Dual-list signal merger. For US, takes scoring snapshots and rotation recommendations, classifies each ticker into one of nine convergence labels (STRONG_BUY through STRONG_SELL), applies score adjustments (convergence boost / conflict penalty), sorts by adjusted score, and assigns ranks. For HK/India, passes scoring signals through with appropriate labels. Provides enrich_snapshots() to write convergence data back into snapshot dicts and build_price_matrix() to construct the wide close-price matrix the rotation engine needs.

portfolio.py — Multi-stock portfolio construction. Extracts latest-day snapshots from scored universes, filters candidates (sig_confirmed = 1), ranks with incumbent bonus (anti-churn), selects positions subject to max-position and max-sector-pct constraints, normalises weights via a water-fill algorithm (iterative scale-clip-redistribute), enforces sector caps post-normalisation, and generates rebalance trade lists when current holdings are provided. Breadth regime scales the target invested percentage (100% strong, 80% neutral, 50% weak).

pipeline/ — Orchestration
runner.py — Single-ticker pipeline. The atomic unit of work: takes raw OHLCV for one ticker plus benchmark, runs indicators → RS → scoring → sector merge → signals in sequence, and returns a TickerResult with the enriched DataFrame and a snapshot dict. Stages 1–3 (indicators, RS, scoring) are required; stages 4–5 (sector, signals) degrade gracefully. Also provides run_batch() for processing the entire universe with progress logging.

orchestrator.py — Top-level pipeline coordinator. Ties together all phases: data loading (Phase 0), universe context (Phase 1 — breadth + sector RS), per-ticker pipeline (Phase 2 — via runner.run_batch()), rotation engine (Phase 2.5 — US only, with quality filter from Phase 2's indicator data), convergence merge (Phase 2.75), cross-sectional analysis (Phase 3 — rankings + portfolio + signals), and reporting (Phase 4). Can be run end-to-end via run_full_pipeline() or phase-by-phase via the Orchestrator class for interactive use. Supports multi-market via run_multi_market_pipeline().

output/ — Cross-Sectional Analysis & Signal Reconciliation
rankings.py — Daily cross-sectional rankings across the scored universe. Stacks per-symbol scored DataFrames into a MultiIndex panel, computes cross-sectional rank per date, tracks day-over-day rank movement, and counts pillar agreement (how many pillars > 0.50). Provides snapshot extractors (latest_rankings, filter_top_n, filter_by_regime), history extractors (rank_history), and summary statistics.

signals.py — Portfolio-level trade signal generation. Layers on top of per-ticker quality gates and cross-sectional rankings to produce final BUY/HOLD/SELL/NEUTRAL signals. Uses rank hysteresis (enter at rank ≤ 8, exit only at rank > 20) to prevent churning. Enforces position limits, applies a portfolio-level breadth circuit breaker, and computes conviction scores (0–1 blend of composite score, rank percentile, pillar agreement, regime quality, and breadth quality). Processes signals date-by-date maintaining a stateful held-positions set.

reports.py — Comprehensive strategy reports combining rankings, signals, breadth, gate diagnostics, and optional backtest performance into a unified document. Includes daily report (positions, watchlist, exit candidates, transitions, turnover), breadth section, strategy overview (rules reference), and performance report (CAGR, Sharpe, drawdown, trade stats).

portfolio/ — Simulation & Risk
backtest.py — Historical simulation engine. Takes the full signal panel, simulates day-by-day portfolio management (mark-to-market → process exits → compute target weights → process entries → check rebalance → record state), and computes comprehensive performance metrics (total return, CAGR, Sharpe, Sortino, Calmar, max drawdown, win rate, profit factor, total commission). Supports execution delay and configurable commission/slippage.

sizing.py — Position sizing algorithms. Four methods: equal weight, score-weighted (proportional to signal strength), inverse volatility, and approximate risk parity. All methods respect per-position min/max limits and normalise to a target exposure level via iterative clip-and-rescale.

rebalance.py — Rebalancing logic and trade generation. Computes per-position drift from target, checks whether maximum drift exceeds the threshold, and generates a list of Trade objects (sells first to free cash, then buys) with commission and slippage estimates.

risk.py — Portfolio risk metrics. Drawdown series and statistics (max drawdown, duration, current), Value at Risk (historical VaR and Conditional VaR / Expected Shortfall), concentration risk (Herfindahl-Hirschman Index, effective number of positions), and rolling annualised volatility.

reports/ — Report Generation
recommendations.py — Structured recommendation report generator. Accepts either a PipelineResult or a legacy dict and produces a structured report dict (for programmatic use), a plain-text report (for terminal), and an HTML report (for browser). Handles regime detection, risk flag annotation, bucket weight allocation, and detailed buy/sell/hold lists with sub-scores and key indicators.

html_report.py — Self-contained HTML recommendation dashboard. Dark-themed responsive design with summary cards, strong-buy highlight cards, per-category signal tables with scores and reasons, interactive sort/collapse/search. No external JS/CSS dependencies (Google Fonts optional). Generated from MarketSignalResult convergence data.

portfolio_view.py — Portfolio view and rebalance-delta generator. Compares CASH recommendations against actual current holdings and produces a rebalance plan (BUY_NEW, ADD, TRIM, CLOSE trades), a current-portfolio health check (concentration risk, underwater positions, stop proximity, signal disagreement), and bucket drift analysis (target vs actual allocation). Outputs as plain text, HTML, or JSON.

weekly_report.py — Weekly report wrapper. Runs the standard pipeline, saves output with ISO-week filenames, and compares against the previous week's JSON to surface new/removed positions and regime changes. Designed for cron scheduling.

scripts/ — CLI Entry Points
run_strategy.py — Unified CLI for all three strategy modes (top-down, bottom-up, full) across all three markets. Handles argument parsing (market, lookback, holdings, output, verbosity, quality filter options), dispatches to the appropriate engine, prints formatted results, and exports JSON. Can be called programmatically for testing.

run_market.py — Single-market runner that produces an HTML convergence report. Simpler interface than run_strategy.py for the common case of running one market and opening the result in a browser.

ingest/ — Data Ingestion & Storage
ingest_cash.py — Downloads OHLCV data for all markets. Auto-selects yfinance (>5 days) or IBKR (≤5 days) based on the requested period. Saves per-market parquet files plus a combined universe parquet. Supports --market, --period, --days, --source, --full, and --backfill flags.

ingest_options.py — Downloads options chain data (not shown in full above). Produces per-ticker CSV files in data/options/{market}/, with a --consolidate flag to merge into market-level parquet files.

db/db.py — Database connection utilities. Provides a singleton SQLAlchemy engine with connection pooling and a test_connection() health check.

db/schema.py — Single source of truth for all database table definitions. Cash tables (us_cash, hk_cash, india_cash, others_cash), options tables (us_options, hk_options), and a signals table for pipeline output. Provides CLI commands for create, drop, recreate, and status.

db/load_db.py — Loads parquet files into PostgreSQL tables with upsert semantics.

db/loader.py — Unified OHLCV data loader for the compute pipeline. Reads from parquet (cached in memory), PostgreSQL, or yfinance (fallback). Returns DataFrames in the standard format expected by compute modules (lowercase columns, DatetimeIndex, no NaN closes). The days parameter limits output to recent data. This is the single read interface — the pipeline never calls ingestion scripts directly.

backtest/ — Strategy Backtesting Framework
data_loader.py — Downloads and caches 20 years of OHLCV data for backtesting.

engine.py — Runs the pipeline over historical periods and feeds results into the backtest simulation.

metrics.py — Computes CAGR, Sharpe ratio, maximum drawdown, annual return breakdown, and other performance metrics for strategy comparison.

strategies.py — Predefined strategy parameter variants for systematic comparison (different pillar weights, lookback windows, quality filter settings).

comparison.py — Multi-strategy comparison framework that runs variants side by side and produces comparative reports.

runner.py — CLI entry point for backtesting (python -m backtest.runner).

Root Files
main.py — Primary entry point. One command runs the full pipeline and saves reports. Supports --portfolio, --positions (for rebalance plans), --tickers, --universe, --dry-run, --backtest, --json, --text-only, and --verbose flags. Also provides a run() function for programmatic use from notebooks.

DataArchitecture.clj — Living documentation (Clojure namespace format) describing the data pipeline architecture, source selection logic, step-by-step commands, and file tree.

Usage.clj — Living documentation describing system usage, execution modes, CLI commands, workflows, output interpretation, and troubleshooting.



# 1. Install dependencies
pip install -r requirements.txt

# 2. Download 2 years of data
python ingest/ingest_cash.py --market all --period 2y

# 3. Load into PostgreSQL (optional — parquet works standalone)
python ingest/db/load_db.py --market all --type cash

# 4. Run the full pipeline for US
python -m scripts.run_strategy full --market US

# 5. Or run all markets with an HTML report
python -m scripts.run_market -m US --days 365 --open

Daily & Weekly Workflows
Morning Scan (~2 minutes)
bash
python ingest/ingest_cash.py --market all --period 5d
python ingest/db/load_db.py --market all --type cash
python -m scripts.run_strategy top-down --market US
python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG

Weekly Portfolio Review (~5 minutes)
bash
python ingest/ingest_cash.py --market all --period 2y
python ingest/db/load_db.py --market all --type cash
python -m scripts.run_strategy full --market ALL \
    --holdings NVDA,CRWD,CEG,LMT,VST,HDFCBANK.NS \
    -o results/weekly_$(date +%Y%m%d).json

Backtesting
The backtest/ module enables testing any strategy variant over up to 20 years of historical data.

bash
# Download 20 years of data
python -m backtest.data_loader --years 20

# Run default backtest
python -m backtest.runner

# Compare strategy variants
python -m backtest.runner --compare

# Custom period
python -m backtest.runner --start 2010-01-01 --end 2026-04-16

Key metrics computed: CAGR, Sharpe ratio, Sortino ratio, Calmar ratio, maximum drawdown (depth and duration), win rate, profit factor, annual return breakdown, and total transaction costs.

Configuration Reference
All parameters live in common/config.py. Key sections:

Section	Controls
INDICATOR_PARAMS	RSI period, MACD fast/slow/signal, ADX period, MA lengths, ATR period, volume windows, etc.
SCORING_WEIGHTS	Pillar weights (rotation 30%, momentum 25%, volatility 15%, microstructure 20%, breadth 10%)
SCORING_PARAMS	Sub-component weights within each pillar, sector adjustment values
SIGNAL_PARAMS	Entry/exit score thresholds, regime gates, momentum streak, cooldown days, position sizing limits
PORTFOLIO_PARAMS	Capital, max/min positions, sector caps, target invested %, rebalance threshold, incumbent bonus
BREADTH_PARAMS	McClellan fast/slow spans, MA windows, thrust thresholds, regime classification cutoffs
BREADTH_PORTFOLIO	Exposure multipliers per breadth regime (strong 100%, neutral 80%, weak 50%)
SECTOR_RS_PARAMS	Lookback, slope window, z-score window, top-N sectors, tailwind regimes
US_CONVERGENCE	Convergence boost (+0.10), conflict penalty (−0.05), override rules
MARKET_CONFIG	Master per-market settings dict (universe, benchmark, engines, all params)
Per-market overrides (HK, India) inherit from US defaults and adjust pillar weights (no breadth → redistribute to momentum and microstructure), signal thresholds (more permissive regime gates for smaller universes), and portfolio constraints (fewer positions, higher concentration limits).


Output & Reports
The system produces several report formats:

HTML Dashboard (reports/html_report.py) — Dark-themed responsive page with summary cards (signal counts), strong-buy highlight cards, per-category signal tables with scores/reasons/sector data, interactive sort and ticker search. Self-contained — no external dependencies.

Text Report (reports/recommendations.py) — Terminal-friendly plain text with regime assessment, risk flags, allocation targets, ranked buy list with sub-scores and indicators, sell recommendations, hold watchlist, and portfolio snapshot.

JSON Export (--output / -o flag) — Machine-readable output containing scored tickers, rotation results, convergence labels, and pipeline metadata. Suitable for downstream dashboards or automated trading systems.

Rebalance Plan (reports/portfolio_view.py) — When current holdings are provided, produces a trade list (BUY_NEW, ADD, TRIM, CLOSE), execution order (sells before buys), bucket drift analysis, and per-position health diagnostics.

License
This project is proprietary. All rights reserved.



