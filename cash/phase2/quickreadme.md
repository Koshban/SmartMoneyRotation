# Smart Money Rotation v2 — Quick Reference
> Last updated: 2026-04-24

---

## Pipeline Execution Flow
Entry point (main.py / cron / notebook)
│
▼
run_pipeline_v2(market, tickers, data_source, ...)
│
├── 1. Fetch OHLCV data
├── 2. ensure_columns(df) ← adapters_v2
├── 3. Compute indicators (RSI, ADX, ATR, MACD, etc.)
├── 4. compute_breadth(...) ← breadth_v2
├── 5. compute_regime(...) ← regime_v2
├── 6. compute_rs(...) ← rs_v2
├── 7. compute_rotation(...) ← rotation_v2
│ └── stamps sectrsregime on every row
├── 8. ensure_columns(df) ← second pass
├── 9. merge leadership snapshot
├── 10. compute_composite_v2(df) ← scoring_v2
│ └── scoretrend, scoreparticipation,
│ scorerisk, scoreregime,
│ scorerotation → scorecomposite_v2
├── 11. leadership overlay (+0.10 × strength)
├── 12. generate_signals_v2(df) ← signals_v2
├── 13. compute_convergence_v2(df) ← convergence_v2
├── 14. generate_actions_v2(df) ← actions_v2
├── 15. build_portfolio_v2(action_table) ← portfolio_v2
├── 16. build selling exhaustion table
├── 17. build review table
│
▼
returns output dict
│
▼
build_report_v2(output) ← report_v2
│
▼
to_text_v2(report) ← report_v2
│
▼
log / email / file / dashboard

---

## Output Table — Column Reference

### Score (scoreadjusted_v2)

Final composite score on a 0–1 scale. It blends five sub-scores (trend,
participation, risk, regime, rotation) and subtracts penalties.

| Range       | Interpretation          |
|-------------|-------------------------|
| ≥ 0.62      | Strong candidate        |
| 0.50 – 0.62 | Decent, worth watching  |
| 0.44 – 0.50 | Marginal                |
| < 0.44      | Weak / avoid            |

This is the single number that drives the Action column.

---

### Action (action_v2)

The system's recommendation based on the adjusted score plus signal
confirmation gates.

| Action     | Meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| STRONG_BUY | Score well above entry threshold; all signals confirmed; high conviction |
| BUY        | Score cleared entry threshold (0.60); momentum, breadth, RS aligned     |
| HOLD       | Above the exit floor but below the buy line; maintain existing position |
| SELL       | Fell below exit threshold (0.44) or triggered an exit signal            |

**Thresholds (defaults):**
- Entry (BUY): 0.60
- Exit (SELL): 0.44
- Strong BUY requires additional relative-volume and momentum gates

---

### RS Regime (rsregime)

Where the stock sits on the relative-strength lifecycle versus the
benchmark (e.g. 2800.HK for Hong Kong).

| Regime      | Icon | Meaning                                              |
|-------------|------|------------------------------------------------------|
| Leading     | 🟢   | Outperforming and accelerating — best quadrant        |
| Improving   | 🔵   | Was lagging, but RS is turning up — early recovery    |
| Weakening   | 🟡   | Was leading, but RS momentum is fading — take notice  |
| Lagging     | 🔴   | Underperforming and getting worse — avoid or exit     |

Think of it like the four quadrants of an RRG (Relative Rotation Graph):
stocks rotate clockwise through Leading → Weakening → Lagging → Improving.

---

### RSI (rsi14) — Relative Strength Index, 14-day

Classic momentum oscillator, range 0–100. Measures speed and magnitude
of recent price changes.

| Range  | Reading                                                    |
|--------|------------------------------------------------------------|
| > 70   | Overbought — momentum is stretched, pullback risk rising   |
| 30–70  | Neutral zone — no extreme                                  |
| < 30   | Oversold — potential bounce setup                          |

**Context matters:** In a strong uptrend RSI can stay above 50 for weeks.
A stock at RSI 65 in a leading RS regime is healthy, not dangerous.

---

### ADX (adx14) — Average Directional Index, 14-day

Measures **trend strength**, not direction. A rising ADX means the trend
(up or down) is getting stronger.

| Range  | Reading                                                    |
|--------|------------------------------------------------------------|
| < 20   | No real trend — choppy, range-bound                        |
| 20–25  | Trend emerging — early but not confirmed                   |
| 25–40  | Confirmed trend — directional move underway                |
| > 40   | Strong trend — powerful move, but may be late-stage        |

**Tip:** A BUY with ADX < 20 means the setup is early / speculative.
A BUY with ADX 25–35 has the best risk/reward — trend is real but not
exhausted.

---

### RVol (relativevolume) — Relative Volume

Today's volume divided by the 20-day average volume. Tells you whether
institutions are participating.

| Range  | Reading                                                    |
|--------|------------------------------------------------------------|
| ≥ 1.50 | High conviction — big players likely involved              |
| 1.00–1.50 | Normal to mildly elevated — standard participation     |
| < 0.80 | Thin volume — move is less trustworthy                     |

**Tip:** A breakout on RVol < 0.8 often fails. Ideally you want BUY
signals confirmed by RVol ≥ 1.0. STRONG_BUY requires even higher.

---

### Trend (scoretrend)

Sub-score (0–1) that feeds into the composite. Captures price position
relative to moving averages (SMA 20/50/200), slope of those averages,
and directional bias.

| Range      | Reading                                              |
|------------|------------------------------------------------------|
| ≥ 0.70     | Healthy uptrend on multiple timeframes               |
| 0.50–0.70  | Mixed — some timeframes trending, others not         |
| < 0.50     | Downtrend or no trend                                |

---

### Partic. (scoreparticipation)

Sub-score (0–1) that measures whether volume confirms the price move.
Captures whether buyers show up on up-days, whether volume expands on
breakouts, and accumulation/distribution patterns.

| Range      | Reading                                              |
|------------|------------------------------------------------------|
| ≥ 0.70     | Strong volume confirmation — money is flowing in     |
| 0.50–0.70  | Moderate — price leads but volume hasn't fully caught up |
| < 0.50     | Weak — price move on declining or thin volume        |

**Tip:** When Trend is high but Participation is low, the move may be
fragile. The best setups have both above 0.65.

---

## Composite Score Weights (defaults)

| Sub-Score          | Weight | Source Column         |
|--------------------|--------|-----------------------|
| Trend              | 0.30   | scoretrend            |
| Participation      | 0.25   | scoreparticipation    |
| Regime             | 0.18   | scoreregime           |
| Risk               | 0.15   | scorerisk             |
| Rotation           | 0.12   | scorerotation         |
| Penalty            | −adj   | scorepenalty          |

Final = (weighted sum of sub-scores) − penalty + leadership overlay

---

## Selling Exhaustion Table

Identifies stocks that have been beaten down and may be approaching a
reversal. Not a buy signal — it's a watch list.

| Column                    | Meaning                                          |
|---------------------------|--------------------------------------------------|
| Status                    | Stage of exhaustion (WEAK_SELLING, EARLY_REVERSAL_SIGNAL) |
| Quality                   | Confidence label (TOO_EARLY → EARLY → CONFIRMED) |
| Exh. (exhaustion score)   | Count of bearish exhaustion signals firing (higher = more oversold) |
| Rev. (reversal trigger)   | Count of reversal confirmation signals (higher = closer to turning) |
| RSI                       | Current RSI — lower means more oversold          |
| 5d Chg                    | 5-day price change — magnitude of recent drop    |

**How to use it:** Watch names that move from TOO_EARLY to EARLY to
CONFIRMED over successive days. When a name reaches CONFIRMED with
Rev. ≥ 3 and RSI starting to curl up, it's worth deeper analysis as a
mean-reversion candidate.

---

## Market Regime Context

The pipeline stamps two regime dimensions that shape thresholds and
position sizing:

**Breadth Regime** — overall market health based on advance/decline,
% above SMA 50/200, net new highs, and dispersion.

| Regime   | Meaning                        | Effect on Portfolio            |
|----------|--------------------------------|--------------------------------|
| Strong   | Broad participation            | Higher target exposure         |
| Moderate | Mixed signals                  | Normal exposure                |
| Weak     | Narrow / deteriorating breadth | Reduced exposure, tighter stops|
| Crisis   | Broad selling                  | Minimal exposure, cash heavy   |

**Volatility Regime** — market-level realized volatility.

| Regime   | Meaning           | Effect on Portfolio               |
|----------|--------------------|-----------------------------------|
| Calm     | Low vol            | Full position sizes, wider entry  |
| Volatile | Elevated vol       | Smaller positions, tighter gates  |
| Chaotic  | Extreme vol        | Minimal new entries               |

---

## Quick Decision Framework


Is the stock a BUY?
│
├── YES → Check ADX
│ ├── ADX ≥ 25 → Trend confirmed, good entry
│ └── ADX < 25 → Early / speculative, size smaller
│
│ Check RVol
│ ├── RVol ≥ 1.0 → Volume confirms, proceed
│ └── RVol < 0.8 → Caution, wait for volume
│
│ Check RSI
│ ├── RSI < 70 → Room to run
│ └── RSI > 70 → May pull back first, tighter stop
│
│ Check RS Regime
│ ├── Leading / Improving → Aligned with market rotation
│ └── Weakening → Momentum fading, less conviction
│
├── HOLD → Already own it? Keep. Don't own it? Skip.
│
└── SELL → Exit or avoid entirely.

gherkin

---

## File Locations

| File                              | Purpose                        |
|-----------------------------------|--------------------------------|
| `phase2/runner_v2.py`          | CLI entry point                |
| `phase2/pipeline_v2.py`        | Orchestrator — runs all steps  |
| `phase2/strategy/breadth_v2.py`| Market breadth computation     |
| `phase2/strategy/rs_v2.py`     | Relative strength z-scores     |
| `phase2/strategy/rotation_v2.py`| Sector rotation regimes       |
| `phase2/strategy/scoring_v2.py`| Composite score calculation    |
| `phase2/strategy/signals_v2.py`| Signal generation & convergence|
| `phase2/strategy/portfolio_v2.py`| Portfolio construction        |
| `phase2/report_v2.py`          | Text report builder            |
| `utils/display_results.py`       | Rich terminal + HTML display   |
| `utils/run_logger.py`            | Dual-output logger             |
| `common/config.py`               | Paths, thresholds, universes   |