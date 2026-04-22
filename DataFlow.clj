ingest_cash.py → data/universe_ohlcv.parquet
                          ↓
src/db/loader.py  →  {ticker: OHLCV DataFrame}
                          ↓
orchestrator.py Phase 0 (load_data)
         ↓
Phase 1: compute_all_breadth()     → self._breadth
         compute_all_sector_rs()   → self._sector_rs
         breadth_to_pillar_scores()→ self._breadth_scores
         ↓
Phase 2: runner.run_batch()        → self._ticker_results
         results_to_scored_universe() → self._scored_universe
         results_to_snapshots()    → self._snapshots
         ↓
Phase 3a: compute_all_rankings()   → self._rankings
Phase 3b: build_portfolio()        → self._portfolio
          _enrich_snapshots_with_allocations()
Phase 3c: compute_all_signals()    → self._signals
          _enrich_snapshots_with_signals()
         ↓
Phase 4: _build_report_input()     → dict (correct keys ✓)
         build_report(dict)        → self._recommendation_report
         ↓
PipelineResult assembled
         ↓
main.py: save_text / save_html / print_report
         build_rebalance_plan() (if positions provided)


# CASH — Logic Flow & File Reference

## Part 1: System-Level Logic Flow

╔══════════════════════════════════════════════════════════════════════════╗
║ DATA INGESTION ║
║ ║
║ ingest_cash.py ║
║ ┌──────────────────────────────────────────────────────────────────┐ ║
║ │ CLI: --market --period --days --source │ ║
║ │ │ ║
║ │ if period ≤ 5 days AND market ∈ {us, hk} │ ║
║ │ → IBKR TWS (live bars, contract-by-contract) │ ║
║ │ else │ ║
║ │ → yfinance (bulk download, multi-ticker) │ ║
║ │ │ ║
║ │ Output: │ ║
║ │ data/us_cash.parquet │ ║
║ │ data/hk_cash.parquet │ ║
║ │ data/india_cash.parquet │ ║
║ │ data/universe_ohlcv.parquet (combined) │ ║
║ └──────────────────────────────────────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ load_db.py (optional) ║
║ ┌──────────────────────────────────────────────────────────────────┐ ║
║ │ Reads parquet → upserts into PostgreSQL │ ║
║ │ Tables: us_cash, hk_cash, india_cash, others_cash │ ║
║ └──────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════╝
│
▼
╔══════════════════════════════════════════════════════════════════════════╗
║ DATA READING ║
║ ║
║ loader.py (single read interface for the entire pipeline) ║
║ ┌──────────────────────────────────────────────────────────────────┐ ║
║ │ load_ohlcv("AAPL", days=365) │ ║
║ │ │ ║
║ │ Try 1: Parquet cache (in-memory, fastest) │ ║
║ │ └─ universe_ohlcv.parquet, india_ohlcv.parquet │ ║
║ │ Try 2: PostgreSQL (pre-loaded, fast) │ ║
║ │ └─ SELECT from {market}_cash WHERE symbol = ? AND date >= ? │ ║
║ │ Try 3: yfinance (network, slow, last resort) │ ║
║ │ └─ yf.download(ticker, start=..., end=...) │ ║
║ │ │ ║
║ │ Output: DataFrame[date, open, high, low, close, volume] │ ║
║ │ DatetimeIndex, sorted ascending, no NaN closes │ ║
║ └──────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════╝
│
▼
╔══════════════════════════════════════════════════════════════════════════╗
║ ORCHESTRATOR PIPELINE ║
║ (pipeline/orchestrator.py) ║
║ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 0: DATA LOADING │ ║
║ │ ───────────────────── │ ║
║ │ Load OHLCV for all tickers + benchmark via loader.py │ ║
║ │ When lookback_days is set, fetches lookback + 220 warmup │ ║
║ │ days so indicators (200-day SMA etc.) can initialise. │ ║
║ │ │ ║
║ │ State populated: │ ║
║ │ self._ohlcv = {ticker: DataFrame} │ ║
║ │ self._bench_df = benchmark DataFrame (SPY / 2800.HK / etc)│ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 1: UNIVERSE CONTEXT │ ║
║ │ ───────────────────────── │ ║
║ │ │ ║
║ │ ┌─ Breadth (compute/breadth.py) ──────────────────────────┐ │ ║
║ │ │ Input: {ticker: OHLCV DataFrame} (full universe) │ │ ║
║ │ │ Steps: align_universe → build close/volume panels │ │ ║
║ │ │ compute_advance_decline → A-D line, adv_ratio │ │ ║
║ │ │ compute_mcclellan → oscillator + summation │ │ ║
║ │ │ compute_pct_above_ma → % above 50d/200d SMA │ │ ║
║ │ │ compute_new_highs_lows → 252d hi-lo ratio │ │ ║
║ │ │ compute_up_volume_ratio → 90% up-vol days │ │ ║
║ │ │ compute_breadth_thrust → thrust/washout detect │ │ ║
║ │ │ compute_breadth_score → 0-1 composite │ │ ║
║ │ │ classify_breadth_regime → strong/neutral/weak │ │ ║
║ │ │ Output: breadth DataFrame + breadth_scores panel │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ ║
║ │ ┌─ Sector RS (compute/sector_rs.py) ──────────────────────┐ │ ║
║ │ │ Input: sector ETF OHLCV + benchmark OHLCV │ │ ║
║ │ │ Steps: For each sector ETF: │ │ ║
║ │ │ RS ratio = sector_close / bench_close │ │ ║
║ │ │ RS slope = rolling OLS of ratio │ │ ║
║ │ │ RS z-score = (slope - mean) / std │ │ ║
║ │ │ RS momentum = slope.diff(10) │ │ ║
║ │ │ Regime = leading/improving/weakening/lagging │ │ ║
║ │ │ Cross-sectional rank per date │ │ ║
║ │ │ Percentile rank → tailwind/headwind score │ │ ║
║ │ │ Output: MultiIndex(date, sector) RS panel │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ ║
║ │ State populated: │ ║
║ │ self._breadth = universe breadth DataFrame │ ║
║ │ self._breadth_scores = per-ticker breadth score panel │ ║
║ │ self._sector_rs = sector RS MultiIndex panel │ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 2: PER-TICKER PIPELINE (pipeline/runner.py) │ ║
║ │ ────────────────────────────── │ ║
║ │ │ ║
║ │ For each ticker in universe (via run_batch): │ ║
║ │ │ ║
║ │ ┌─ Stage 1: indicators.py ─────────────────────────────────┐│ ║
║ │ │ Input: raw OHLCV DataFrame ││ ║
║ │ │ Output: +30 columns (RSI, MACD, ADX, MAs, ATR, OBV, ││ ║
║ │ │ A/D line, volume metrics, Amihud, VWAP) ││ ║
║ │ └──────────────────────────────────────────────────────────┘│ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Stage 2: relative_strength.py ──────────────────────────┐│ ║
║ │ │ Input: indicator-enriched stock + benchmark OHLCV ││ ║
║ │ │ Output: rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore, ││ ║
║ │ │ rs_momentum, rs_vol_confirmed, rs_regime ││ ║
║ │ │ Note: aligns stock + bench on common dates ││ ║
║ │ └──────────────────────────────────────────────────────────┘│ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Stage 3: scoring.py ───────────────────────────────────┐ │ ║
║ │ │ Input: indicator + RS enriched DataFrame │ │ ║
║ │ │ + optional breadth_scores (per-ticker series) │ │ ║
║ │ │ Compute: │ │ ║
║ │ │ P1 Rotation = f(rs_zscore, regime, momentum, │ │ ║
║ │ │ vol_confirmed) [0-1] │ │ ║
║ │ │ P2 Momentum = f(rsi, macd_hist, adx) [0-1] │ │ ║
║ │ │ P3 Volatility = f(realized_vol, atr%, amihud)[0-1] │ │ ║
║ │ │ P4 Microstructure= f(obv_slope, ad_slope, │ │ ║
║ │ │ relative_vol) [0-1] │ │ ║
║ │ │ P5 Breadth = breadth_score / 100 [0-1] │ │ ║
║ │ │ │ │ ║
║ │ │ composite = Σ weight_i × pillar_i [0-1] │ │ ║
║ │ │ percentile = rolling_pctrank(composite, 252) │ │ ║
║ │ │ │ │ ║
║ │ │ Output: score_rotation, score_momentum, │ │ ║
║ │ │ score_volatility, score_microstructure, │ │ ║
║ │ │ score_breadth, score_composite, │ │ ║
║ │ │ score_percentile │ │ ║
║ │ └──────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Stage 4: sector_rs.merge_sector_context() ─────────────┐ │ ║
║ │ │ (OPTIONAL — skipped if sector unknown or no sector RS) │ │ ║
║ │ │ Input: scored DataFrame + sector RS panel │ │ ║
║ │ │ Output: +sect_rs_zscore, sect_rs_regime, sect_rs_rank, │ │ ║
║ │ │ sector_tailwind, sector_name │ │ ║
║ │ │ score_adjusted = composite + tailwind [0-1] │ │ ║
║ │ └──────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Stage 5: strategy/signals.py ──────────────────────────┐ │ ║
║ │ │ (OPTIONAL — per-ticker quality gates) │ │ ║
║ │ │ │ │ ║
║ │ │ Gate 1: score_adjusted ≥ entry_min (+ breadth adj) │ │ ║
║ │ │ Gate 2: rs_regime ∈ {leading, improving} │ │ ║
║ │ │ Gate 3: sect_rs_regime ∈ {leading, improving, neutral} │ │ ║
║ │ │ Gate 4: breadth_regime ≠ weak │ │ ║
║ │ │ Gate 5: momentum_streak ≥ 2 consecutive days > 0.5 │ │ ║
║ │ │ Gate 6: NOT in cooldown (15 days after exit) │ │ ║
║ │ │ │ │ ║
║ │ │ ALL 6 pass → sig_confirmed = 1 │ │ ║
║ │ │ Any fail → sig_confirmed = 0, sig_reason = which gate │ │ ║
║ │ │ │ │ ║
║ │ │ Also computes: sig_exit, sig_position_pct │ │ ║
║ │ └──────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ Build snapshot dict (latest row summary for orchestrator) │ ║
║ │ Return TickerResult { df, snapshot, error, stages } │ ║
║ │ │ ║
║ │ State populated: │ ║
║ │ self._ticker_results = {ticker: TickerResult} │ ║
║ │ self._scored_universe = {ticker: enriched DataFrame} │ ║
║ │ self._snapshots = [snapshot dicts, sorted by score] │ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 2.5: ROTATION ENGINE (US only) │ ║
║ │ ───────────────────────────── │ ║
║ │ strategy/rotation.py │ ║
║ │ │ ║
║ │ ┌─ Step 1: Rank Sectors ──────────────────────────────────┐ │ ║
║ │ │ For each sector ETF (XLK, XLF, XLE, ...): │ │ ║
║ │ │ RS = 0.40 × excess_ret_21d │ │ ║
║ │ │ + 0.35 × excess_ret_63d │ │ ║
║ │ │ + 0.25 × excess_ret_126d │ │ ║
║ │ │ Sort descending → rank 1-11 │ │ ║
║ │ │ Top 3 = Leading, Bottom 3 = Lagging, Rest = Neutral │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Step 2: Pick Stocks in Leading Sectors ────────────────┐ │ ║
║ │ │ For each leading sector: │ │ ║
║ │ │ Get US single names (from sector_map.py) │ │ ║
║ │ │ Filter: RS ≥ min, RS vs sector ETF > 0 │ │ ║
║ │ │ │ │ ║
║ │ │ If quality enabled AND indicator_data available: │ │ ║
║ │ │ ┌─ rotation_filters.py ────────────────────────┐ │ │ ║
║ │ │ │ quality_gate(): │ │ │ ║
║ │ │ │ ✓ close > SMA_50 │ │ │ ║
║ │ │ │ ✓ EMA_30 > SMA_50 │ │ │ ║
║ │ │ │ ✓ 30 ≤ RSI ≤ 75 │ │ │ ║
║ │ │ │ ✓ ADX ≥ 18 │ │ │ ║
║ │ │ │ quality_score(): │ │ │ ║
║ │ │ │ 0.25 × MA positioning │ │ │ ║
║ │ │ │ 0.20 × RSI zone │ │ │ ║
║ │ │ │ 0.20 × volume profile │ │ │ ║
║ │ │ │ 0.15 × MACD state │ │ │ ║
║ │ │ │ 0.10 × ADX + direction │ │ │ ║
║ │ │ │ 0.10 × volatility regime │ │ │ ║
║ │ │ │ blend_rs_quality(): │ │ │ ║
║ │ │ │ blended = 0.60 × sigmoid(RS) │ │ │ ║
║ │ │ │ + 0.40 × quality │ │ │ ║
║ │ │ └──────────────────────────────────────────────┘ │ │ ║
║ │ │ Else: rank by raw RS only │ │ ║
║ │ │ │ │ ║
║ │ │ Take top 3 per sector → BUY recommendations │ │ ║
║ │ │ (Fall back to sector ETF if no single names pass) │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Step 3: Evaluate Holdings ─────────────────────────────┐ │ ║
║ │ │ For each current holding: │ │ ║
║ │ │ If sector → Lagging: SELL │ │ ║
║ │ │ If sector → Neutral: REDUCE │ │ ║
║ │ │ If individual RS < floor: SELL │ │ ║
║ │ │ Otherwise: HOLD │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ Step 4: Enforce Position Cap ──────────────────────────┐ │ ║
║ │ │ max_total_positions = 12 │ │ ║
║ │ │ Slots = max - (HOLD count + REDUCE count) │ │ ║
║ │ │ Take top N new BUYs that fit │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ ║
║ │ Output: RotationResult │ ║
║ │ .sector_rankings — 11 SectorScore objects │ ║
║ │ .recommendations — list of Recommendation (BUY/SELL/etc) │ ║
║ │ .buys / .sells / .reduces / .holds │ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 2.75: CONVERGENCE MERGE (strategy/convergence.py) │ ║
║ │ ───────────────────────────────── │ ║
║ │ │ ║
║ │ For US (dual-list): │ ║
║ │ ┌────────────────────────────────────────────────────────┐ │ ║
║ │ │ For each ticker in scoring_snapshots ∪ rotation_recs: │ │ ║
║ │ │ │ │ ║
║ │ │ Scoring Rotation → Label Score Adj │ │ ║
║ │ │ ─────── ──────── ───── ───────── │ │ ║
║ │ │ BUY BUY → STRONG_BUY + 0.10 │ │ ║
║ │ │ BUY neutral → BUY_SCORING unchanged │ │ ║
║ │ │ neutral BUY → BUY_ROTATION unchanged │ │ ║
║ │ │ BUY SELL → CONFLICT − 0.05 │ │ ║
║ │ │ SELL BUY → CONFLICT − 0.05 │ │ ║
║ │ │ SELL SELL → STRONG_SELL − 0.05 │ │ ║
║ │ │ SELL neutral → SELL_SCORING unchanged │ │ ║
║ │ │ neutral SELL → SELL_ROTATION unchanged │ │ ║
║ │ │ HOLD HOLD → HOLD unchanged │ │ ║
║ │ │ else → NEUTRAL unchanged │ │ ║
║ │ └────────────────────────────────────────────────────────┘ │ ║
║ │ │ ║
║ │ For HK / India (scoring only): │ ║
║ │ BUY/confirmed → BUY_SCORING │ ║
║ │ SELL/exit → SELL_SCORING │ ║
║ │ HOLD → HOLD │ ║
║ │ else → NEUTRAL │ ║
║ │ │ ║
║ │ Sort all by adjusted_score descending → assign rank │ ║
║ │ Enrich snapshot dicts with convergence labels │ ║
║ │ │ ║
║ │ Output: MarketSignalResult │ ║
║ │ .signals — list[ConvergedSignal] │ ║
║ │ .strong_buys — highest conviction entries │ ║
║ │ .conflicts — engines disagree, review manually │ ║
║ │ .strong_sells — highest conviction exits │ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 3: CROSS-SECTIONAL ANALYSIS │ ║
║ │ ────────────────────────────────── │ ║
║ │ │ ║
║ │ ┌─ 3a: Rankings (output/rankings.py) ─────────────────────┐ │ ║
║ │ │ Stack {ticker: scored_df} → MultiIndex(date, ticker) │ │ ║
║ │ │ Cross-sectional rank per date (score_composite) │ │ ║
║ │ │ Day-over-day rank changes │ │ ║
║ │ │ Pillar agreement (how many pillars > 0.50) │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ 3b: Portfolio (strategy/portfolio.py) ─────────────────┐ │ ║
║ │ │ extract_snapshots → latest-day data per ticker │ │ ║
║ │ │ filter_candidates → sig_confirmed == 1 only │ │ ║
║ │ │ rank_candidates → score + incumbent bonus │ │ ║
║ │ │ select_positions → top N, enforce sector caps │ │ ║
║ │ │ compute_weights → water-fill normalisation │ │ ║
║ │ │ Breadth regime scales target invested %: │ │ ║
║ │ │ strong → 100%, neutral → 80%, weak → 50% │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ │ │ │ ║
║ │ ▼ │ ║
║ │ ┌─ 3c: Portfolio Signals (output/signals.py) ─────────────┐ │ ║
║ │ │ For each date, maintain a set of held positions: │ │ ║
║ │ │ │ │ ║
║ │ │ Held tickers: │ │ ║
║ │ │ If breadth bearish + exit_all → SELL │ │ ║
║ │ │ If rank > 20 OR score < 0.30 → SELL │ │ ║
║ │ │ Otherwise → HOLD │ │ ║
║ │ │ │ │ ║
║ │ │ Non-held tickers: │ │ ║
║ │ │ If breadth bearish → block (NEUTRAL) │ │ ║
║ │ │ If sig_confirmed AND rank ≤ 8 AND slots → BUY │ │ ║
║ │ │ Otherwise → NEUTRAL │ │ ║
║ │ │ │ │ ║
║ │ │ Hysteresis: enter at rank ≤ 8, exit only at rank > 20 │ │ ║
║ │ │ Priority: best composite score gets slots first │ │ ║
║ │ │ │ │ ║
║ │ │ Compute signal_strength (0-1 conviction): │ │ ║
║ │ │ 30% composite + 20% rank + 20% agreement │ │ ║
║ │ │ + 15% regime + 15% breadth │ │ ║
║ │ └─────────────────────────────────────────────────────────┘ │ ║
║ └───────────────────────────┬───────────────────────────────────┘ ║
║ │ ║
║ ▼ ║
║ ┌───────────────────────────────────────────────────────────────┐ ║
║ │ PHASE 4: REPORTS & OPTIONAL BACKTEST │ ║
║ │ ──────────────────────────────────── │ ║
║ │ │ ║
║ │ recommendations.py → structured report dict │ ║
║ │ → to_text() → terminal / .txt file │ ║
║ │ → to_html() → .html file │ ║
║ │ │ ║
║ │ html_report.py → convergence dashboard HTML │ ║
║ │ (summary cards, signal tables, strong-buy cards, │ ║
║ │ interactive sort/filter/search) │ ║
║ │ │ ║
║ │ portfolio/backtest.py (optional): │ ║
║ │ Simulate day-by-day from signal panel │ ║
║ │ Mark-to-market → exits → sizing → entries → rebalance │ ║
║ │ → equity curve, trades, metrics (CAGR, Sharpe, DD) │ ║
║ │ │ ║
║ │ Output: PipelineResult │ ║
║ │ .scored_universe, .snapshots, .rankings │ ║
║ │ .portfolio, .signals, .convergence │ ║
║ │ .breadth, .sector_rs, .rotation_result │ ║
║ │ .recommendation_report, .backtest │ ║
║ │ .timings, .errors │ ║
║ └───────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════╝

## Part 2: Detailed Per-Ticker Pipeline Flow

This is what happens inside `pipeline/runner.py::run_ticker()` for a single ticker:


apache
                raw OHLCV (date, O, H, L, C, V)
                          │
                ┌─────────▼──────────┐
                │  Date slice (as_of) │  Cut future data for backtesting
                │  Min 200 bars check │
                └─────────┬──────────┘
                          │
          ┌───────────────▼───────────────────┐
          │    STAGE 1: compute_all_indicators │  REQUIRED
          │                                    │
          │  add_returns     (5d, 10d, 15d)    │
          │  add_rsi         (14-period)        │
          │  add_macd        (12/26/9)          │
          │  add_adx         (14-period +DI/-DI)│
          │  add_moving_averages (EMA30, SMA30, │
          │                      SMA50 + %dist) │
          │  add_atr         (14-period + %)    │
          │  add_realized_vol (20d annualised)  │
          │  add_obv         (cumulative + slope)│
          │  add_ad_line     (CLV-weighted +slp)│
          │  add_volume_metrics (avg, relative,  │
          │                     dollar volume)  │
          │  add_amihud      (illiquidity ratio) │
          │  add_vwap_distance (rolling proxy)   │
          │                                    │
          │  Result: +30 indicator columns      │
          └───────────────┬───────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │    STAGE 2: compute_all_rs         │  REQUIRED
          │                                    │
          │  Align stock + benchmark dates     │
          │  add_rs_ratio    (stock/bench,     │
          │                   normalised,      │
          │                   EMA10, SMA50)    │
          │  add_rs_slope    (rolling OLS of   │
          │                   smoothed ratio)  │
          │  add_rs_zscore   (standardised     │
          │                   slope, 60d)      │
          │  add_rs_momentum (short slope −    │
          │                   long slope)      │
          │  add_rs_volume_confirmation        │
          │                   (RS↑ + volume↑)  │
          │  add_rs_regime   (leading /        │
          │                   weakening /      │
          │                   lagging /        │
          │                   improving)       │
          │                                    │
          │  Result: +9 RS columns             │
          └───────────────┬───────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │    STAGE 3: compute_composite_score│  REQUIRED
          │                                    │
          │  Pillar 1 (Rotation):              │
          │    sigmoid(rs_zscore)        35%   │
          │    regime_map(rs_regime)     30%   │
          │    sigmoid(rs_momentum)      20%   │
          │    float(rs_vol_confirmed)   15%   │
          │                                    │
          │  Pillar 2 (Momentum):              │
          │    piecewise(rsi)            35%   │
          │    rolling_pctrank(macd_h)   35%   │
          │    piecewise(adx)            30%   │
          │                                    │
          │  Pillar 3 (Volatility):            │
          │    tent(vol_rank)            40%   │
          │    tent(atr_rank)            30%   │
          │    inv(amihud_rank)          30%   │
          │                                    │
          │  Pillar 4 (Microstructure):        │
          │    pctrank(obv_slope)        35%   │
          │    pctrank(ad_slope)         30%   │
          │    piecewise(rel_volume)     35%   │
          │                                    │
          │  Pillar 5 (Breadth):               │
          │    breadth_score / 100  (0-1)      │
          │                                    │
          │  composite = 0.30 × P1             │
          │            + 0.25 × P2             │
          │            + 0.15 × P3             │
          │            + 0.20 × P4             │
          │            + 0.10 × P5             │
          │                                    │
          │  percentile = rolling_pctrank(252)  │
          │                                    │
          │  Result: +7 score columns          │
          └───────────────┬───────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │    STAGE 4: merge_sector_context   │  OPTIONAL
          │                                    │
          │  Look up ticker's sector           │
          │  Join sector RS columns:           │
          │    sect_rs_zscore, sect_rs_regime,  │
          │    sect_rs_rank, sect_rs_pctrank,   │
          │    sector_tailwind, sector_name     │
          │                                    │
          │  score_adjusted = composite         │
          │                  + sector_tailwind  │
          │                  clipped [0, 1]     │
          │                                    │
          │  Degrades: ticker still scored      │
          │  without sector adjustments         │
          └───────────────┬───────────────────┘
                          │
          ┌───────────────▼───────────────────┐
          │    STAGE 5: generate_signals       │  OPTIONAL
          │                                    │
          │  Gate 1: score ≥ entry_min + adj   │
          │  Gate 2: rs_regime ∈ allowed       │
          │  Gate 3: sect_regime ∈ allowed     │
          │  Gate 4: breadth ≠ weak            │
          │  Gate 5: momentum streak ≥ 2       │
          │  Gate 6: NOT in cooldown           │
          │                                    │
          │  → sig_confirmed (0/1)             │
          │  → sig_exit (score collapse)        │
          │  → sig_position_pct (sizing)        │
          │  → sig_reason (diagnostic)          │
          │                                    │
          │  Degrades: ticker appears in        │
          │  rankings without quality gates     │
          └───────────────┬───────────────────┘
                          │
                          ▼
                TickerResult { df, snapshot }

---

## Part 3: File-by-File Reference

### Root

| File | What It Does |
|------|-------------|
| `main.py` | Primary CLI entry point. Parses arguments, runs the full pipeline via the orchestrator, saves text/HTML/JSON reports, optionally generates a rebalance plan against current holdings, and provides a `run()` function for notebooks. |
| `pyproject.toml` | Project metadata and build configuration. |
| `requirements.txt` | Python package dependencies. |
| `DataArchitecture.clj` | Living documentation describing the data pipeline architecture, source selection logic, and file hierarchy. |
| `.gitignore` | Standard gitignore for Python, data files, credentials, and IDE artefacts. |

### `common/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `config.py` | Central configuration. Every tunable parameter for the entire system: indicator periods, scoring weights, signal thresholds, portfolio constraints, sector ETF mappings, breadth thresholds, per-market overrides (HK, India), convergence settings, and the master `MARKET_CONFIG` dict. |
| `universe.py` | Defines the full investable universe across three tiers (ETFs, HK single names, India large caps, thematic stock baskets). Provides helper functions for market detection, ticker parsing, category lookup, and universe assembly. |
| `sector_map.py` | Maps every ticker to one of the 11 GICS sectors. Handles edge cases (crypto miners, nuclear, solar, ride-share). Provides lookup functions for tickers-by-sector, per-market filtering, and universe coverage validation. |
| `expiry.py` | Calculates monthly option expiry dates. Third Friday for US/HK, last Thursday for India NSE. Used by options ingestion. |
| `credentials.py` | Database and IBKR connection credentials. Excluded from version control. |

### `compute/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `indicators.py` | Computes ~30 technical indicator columns on an OHLCV DataFrame: returns, RSI, MACD, ADX, moving averages, ATR, realised volatility, OBV, A/D line, volume metrics, Amihud illiquidity, VWAP distance. Pure functions, no scoring opinions. |
| `relative_strength.py` | Computes relative strength of a stock versus its benchmark: RS ratio (normalised), RS slope (OLS of smoothed ratio), RS z-score (standardised), RS momentum (acceleration), volume-confirmed RS, and RS regime (leading/weakening/lagging/improving quadrant). |
| `scoring.py` | Five-pillar composite scoring engine. Combines rotation, momentum, volatility, microstructure, and breadth sub-scores into a weighted composite [0–1]. Uses sigmoid, piecewise linear, rolling percentile rank, and tent function normalisations. Falls back to four pillars when breadth is unavailable. |
| `sector_rs.py` | Sector-level relative-strength analysis. Computes RS ratio/slope/z-score/regime for each of 11 GICS sector ETFs versus benchmark, cross-sectionally ranks sectors each day, and derives tailwind/headwind adjustments for individual stock scores. |
| `breadth.py` | Universe-level market breadth analytics. Computes advance/decline, McClellan oscillator, percent above MAs, new highs/lows, up-volume ratio, breadth thrust detection, composite breadth score (0–1), and regime classification (strong/neutral/weak). |

### `strategy/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `signals.py` | Per-ticker entry/exit signal generation. Implements six quality gates (score, RS regime, sector regime, breadth, momentum streak, cooldown) that must all pass for `sig_confirmed = 1`. Computes exit triggers and breadth-adjusted position sizing. |
| `rotation.py` | Core smart money rotation engine. Ranks 11 GICS sectors by composite RS, picks top stocks in leading sectors (blended RS + quality), evaluates current holdings against sell rules (sector drift, RS collapse), enforces position caps. Returns `RotationResult` with sector rankings and recommendations. |
| `rotation_filters.py` | Technical quality filters for rotation stock selection. Hard quality gate (4 checks) and soft quality score (6 sub-components). Blends normalised RS with quality via sigmoid for final stock ranking within leading sectors. |
| `convergence.py` | Dual-list signal convergence for US (merges scoring + rotation into STRONG_BUY / CONFLICT / etc.), scoring-only passthrough for HK/India. Market dispatcher routes to the correct merge logic. Also provides `build_price_matrix()` and `enrich_snapshots()`. |
| `portfolio.py` | Multi-stock portfolio construction. Extracts snapshots, filters on sig_confirmed, ranks with incumbent bonus, selects under position/sector caps, normalises weights via water-fill, scales by breadth regime, generates rebalance trades. |

### `pipeline/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `runner.py` | Single-ticker pipeline. Chains indicators → RS → scoring → sector merge → signals for one ticker. Returns `TickerResult` with the enriched DataFrame and snapshot dict. Also provides `run_batch()` for processing the full universe. |
| `orchestrator.py` | Top-level coordinator. Executes phases 0–4 in sequence (data loading → universe context → per-ticker pipeline → rotation → convergence → cross-sectional → reports). Supports phase-by-phase execution, multi-market dispatch, and parameter re-runs without re-computing indicators. |

### `output/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `rankings.py` | Cross-sectional rankings across the scored universe. Stacks per-symbol data into a MultiIndex panel, computes rank/pct_rank per date, tracks rank changes, counts pillar agreement, and provides snapshot extractors and text reports. |
| `signals.py` | Portfolio-level trade signal generation. Adds cross-sectional rank filter with hysteresis, enforces position limits, applies breadth circuit breaker, computes conviction scores. Processes dates statefully maintaining held-position sets. |
| `reports.py` | Comprehensive strategy reports combining rankings, signals, breadth, gate diagnostics, and backtest performance into a unified text document with daily, breadth, strategy overview, and performance sections. |

### `portfolio/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `backtest.py` | Historical simulation engine. Processes signal panel day-by-day (mark-to-market → exits → target weights → entries → rebalance). Computes equity curve, all trades, and comprehensive metrics (CAGR, Sharpe, Sortino, Calmar, drawdown, win rate, profit factor). |
| `sizing.py` | Position sizing algorithms: equal weight, score-weighted, inverse volatility, risk parity. Iterative clip-and-rescale to respect per-position min/max and target exposure. |
| `rebalance.py` | Rebalancing logic. Computes per-position drift, checks threshold, generates Trade objects (sells first, then buys) with commission/slippage estimates. |
| `risk.py` | Portfolio risk metrics: drawdown series and stats, historical VaR, CVaR/expected shortfall, concentration risk (HHI), rolling volatility. |

### `reports/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `recommendations.py` | Structured recommendation report generator. Accepts `PipelineResult` or dict, produces header/regime/allocation/buy/sell/hold/risk sections. Outputs as text, HTML, or dict for JSON. |
| `html_report.py` | Self-contained HTML convergence dashboard. Dark theme, responsive layout, summary cards, strong-buy highlights, signal tables with sort/search. No external dependencies. |
| `portfolio_view.py` | Rebalance-delta generator. Compares CASH recommendations against actual holdings → trade list (BUY_NEW/ADD/TRIM/CLOSE), bucket drift analysis, per-position health diagnostics. Text and HTML output. |
| `weekly_report.py` | Weekly report wrapper. Runs pipeline, saves with ISO-week filenames, diffs against previous week's JSON (new/removed positions, regime changes). Designed for cron scheduling. |

### `scripts/`

| File | What It Does |
|------|-------------|
| `run_strategy.py` | Unified CLI for top-down, bottom-up, and full strategy modes across all markets. Parses arguments, dispatches to the appropriate engine, prints results, exports JSON. |
| `run_market.py` | Single-market runner producing an HTML convergence report. Simplified interface for the common case. Optionally opens result in browser. |

### `src/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `ingest_cash.py` | Downloads OHLCV data for all markets. Auto-selects yfinance or IBKR based on period. Saves per-market and combined parquet files. |
| `ingest_options.py` | Downloads options chain data. Per-ticker CSV files with consolidation to parquet. |

### `src/db/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `db.py` | Database connection utilities. Singleton SQLAlchemy engine with pooling. Health check function. |
| `schema.py` | All database table DDL. Cash tables (4 regions), options tables (2 regions), signals table. CLI for create/drop/recreate/status. |
| `load_db.py` | Loads parquet files into PostgreSQL with upsert semantics. |
| `loader.py` | Unified OHLCV data reader. Tries parquet cache → PostgreSQL → yfinance. Returns normalised DataFrames. The single read interface for the pipeline. |

### `backtest/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `data_loader.py` | Downloads and caches 20 years of OHLCV for backtesting. |
| `engine.py` | Runs the pipeline over historical periods for backtest simulation. |
| `metrics.py` | Computes CAGR, Sharpe, drawdown, annual returns, and other performance metrics. |
| `strategies.py` | Predefined strategy parameter variants for systematic comparison. |
| `comparison.py` | Multi-strategy comparison framework with side-by-side reports. |
| `runner.py` | CLI entry point: `python -m backtest.runner`. |

### `tests/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `test_connections.py` | Verifies database connectivity and IBKR TWS availability. |

### `utils/`

| File | What It Does |
|------|-------------|
| `__init__.py` | Package marker. |
| `run_logger.py` | Utility for structured run logging with timestamps and result capture. |

### `results/`

| File | What It Does |
|------|-------------|
| `comparison_report.txt` | Output from multi-strategy backtesting comparisons. |
| `run_logger.py` | Logs pipeline run metadata (date, tickers, timings, errors) for audit trail. |