# How to Read the Smart Money Rotation Report

A practical guide to every section, column, and signal in the report — and how to turn them into actionable decisions.

---

## Table of Contents

1. [Report Header](#1-report-header)
2. [Market Overview](#2-market-overview)
3. [Sector Rotation](#3-sector-rotation)
4. [ETF Ranking](#4-etf-ranking)
5. [Portfolio Allocations](#5-portfolio-allocations)
6. [All Buy-Rated Names](#6-all-buy-rated-names)
7. [Full Universe](#7-full-universe)
8. [Selling Exhaustion](#8-selling-exhaustion)
9. [Decision Framework](#9-decision-framework)
10. [Glossary](#10-glossary)

---

## 1. Report Header

The blue banner at the top of every report.

| Field       | Meaning |
|-------------|---------|
| **Market**  | Which market universe was scanned (e.g. `US`, `EU`, `IN`). |
| **Date**    | The trading date the data corresponds to. |
| **Generated** | Wall-clock time the report was built. If this is hours after market close, all indicators reflect end-of-day values. |

---

## 2. Market Overview

The overview section is your **top-down context**. Read it first — it tells you whether the environment favours aggressive buying, cautious positioning, or defence.

### Regime Cards

| Card | What It Tells You | How to Use It |
|------|-------------------|---------------|
| **Breadth** | The market's internal health. Derived from the percentage of stocks above key moving averages and the net advance/decline trend. Values like `healthy`, `moderate`, `weak`, or `deteriorating`. | When breadth is `healthy` or `improving`, broad exposure is safer. When `weak` or `deteriorating`, favour high-conviction names only and keep position sizes small. |
| **Volatility** | The prevailing volatility regime based on VIX level, term-structure, and realised-vs-implied spread. Values like `low`, `moderate`, `elevated`, `crisis`. | `low` / `moderate` → normal position sizing. `elevated` → reduce size, widen stops. `crisis` → cash-heavy, only exhaustion plays. |
| **Exposure** | The system's recommended gross equity exposure as a percentage of capital. Automatically scales down in weak breadth or high-vol regimes. | Treat this as the portfolio's throttle. If the report says 70 %, do not lever up to 100 % — the system is telling you to hold back. |
| **Cash** | The complement of exposure. The percentage of capital the system recommends keeping in cash or equivalents. | This is not "idle" money — it is a deliberate risk buffer. |
| **Universe** | How many individual names were scored after filtering for liquidity, data quality, and minimum market cap. | A shrinking universe may indicate thinning liquidity or data issues. |
| **Candidates** | How many names passed the minimum composite score threshold and are eligible for portfolio inclusion. | A very low candidate count in a large universe confirms a narrow, selective market. |
| **Selected** | How many names the portfolio optimizer actually allocated capital to. | This is your actionable basket size. |
| **Breadth Score** | A continuous 0–1 score summarising breadth health (only shown when available). Higher is healthier. | Values above 0.6 are constructive. Below 0.4 warrants caution. |
| **Dispersion 20d** | Cross-sectional return dispersion over the last 20 trading days. | High dispersion means stock-picking matters more than market direction — a good environment for active rotation. Low dispersion means everything moves together and sector/macro bets dominate. |

### Action Counters

The coloured badges show how many names in the full universe received each action rating:

| Badge | Meaning |
|-------|---------|
| **STRONG BUY** | Top-tier composite score, favourable regime, strong trend + momentum + participation. Highest conviction. |
| **BUY** | Above-average composite score with acceptable regime conditions. Solid candidates. |
| **HOLD** | Scores are middling or regime conditions are mixed. Not worth new capital, but not an urgent sell if already owned. |
| **SELL** | Below threshold on score, deteriorating trend, or regime headwinds. Reduce or exit. |

**Quick read:** If STRONG BUY + BUY names outnumber HOLD + SELL names, the tape is generally constructive. If SELL dominates, the environment is hostile.

---

## 3. Sector Rotation

This section combines **Relative Rotation Graph (RRG)** analysis with **ETF composite scoring** to rank sectors by momentum, trend, and relative strength.

### Quadrant Badges

Each sector is placed into one of four RRG quadrants based on its relative-strength level and relative-strength momentum versus the benchmark:

| Quadrant | Icon | RS Level | RS Momentum | Interpretation |
|----------|------|----------|-------------|----------------|
| **Leading** | 🟢 | High | Positive | Sector is outperforming and the outperformance is accelerating. Best place to be. |
| **Improving** | 🔵 | Low | Positive | Sector has been lagging but relative strength is now turning up. Early-stage recovery — potential overweight opportunity. |
| **Weakening** | 🟡 | High | Negative | Sector has been leading but momentum is fading. Consider trimming or tightening stops. |
| **Lagging** | 🔴 | Low | Negative | Underperforming and getting worse. Avoid or short. |

The typical lifecycle is: **Improving → Leading → Weakening → Lagging → Improving** (clockwise rotation).

### Column Definitions

| Column | Description | Range | How to Read |
|--------|-------------|-------|-------------|
| **#** | Rank by blended score (descending). | 1 – N | Lower rank = stronger sector. |
| **Sector** | GICS sector name. | — | — |
| **ETF** | The SPDR or iShares sector ETF used as the proxy (e.g. `XLK`, `XLY`). | — | — |
| **Regime** | The RRG quadrant badge (see above). | leading / improving / weakening / lagging | Primary directional signal for sector allocation. |
| **Blended** | A weighted combination of the ETF composite score and the RRG relative-strength metrics. Shown as a coloured bar. | 0 – 1 | The single best "how attractive is this sector right now" number. Above 0.55 is constructive; below 0.35 is cautionary. |
| **RS Lvl** | Normalised relative-strength level vs. the broad benchmark. | Typically –0.10 to +0.10 | Positive means the sector is outperforming the index. |
| **RS Mom** | Rate of change of RS Level (its first derivative). | Typically –0.05 to +0.05 | Positive means relative strength is accelerating. |
| **ETF Scr** | The sector ETF's own composite score (momentum + trend + participation sub-scores). | 0 – 1 | Captures the ETF's absolute technical health, independent of relative strength. |
| **Excess 20d** | The sector's excess return over the benchmark in the last 20 trading days. | Percentage | Confirms or contradicts the regime. A "leading" sector should show positive excess return. |
| **RRG** | The raw RRG quadrant classification (may differ slightly from the blended regime if the ETF composite is very strong/weak). | Same four quadrants | Cross-check with the Regime column. Agreement between both is a higher-confidence signal. |

### How to Use for Decisions

- **Overweight** sectors that are 🟢 Leading with a high Blended score and positive Excess 20d.
- **Start building** positions in 🔵 Improving sectors — they are the next potential leaders.
- **Reduce** exposure to 🟡 Weakening sectors, especially if Excess 20d has turned negative.
- **Avoid** 🔴 Lagging sectors entirely for new long positions.
- When Regime and RRG agree, confidence is high. When they disagree, the Blended score is the tiebreaker.

---

## 4. ETF Ranking

A granular ranking of all scored ETFs (sector, thematic, and broad), sorted by composite score. Use this to find the best vehicle for expressing a sector or theme bet.

### Summary Line

- **Scored:** Total ETFs evaluated.
- **Mean:** Average composite score across all ETFs. Tells you whether the broad ETF universe is healthy (mean > 0.55) or stressed (mean < 0.40).

### Column Definitions

| Column | Description | Range | How to Read |
|--------|-------------|-------|-------------|
| **#** | Rank by composite score. | 1 – N | — |
| **Ticker** | ETF ticker. A filled dot (●) means it is a core sector ETF; an open dot (○) marks a broad-market ETF. | — | Sector ETFs are your primary rotation vehicles. |
| **Theme** | Sub-theme or focus (e.g. "Semiconductors", "Clean Energy"). | — | Helps distinguish niche ETFs from broad sector ones. |
| **Sector** | Parent GICS sector. | — | Cross-reference with the Sector Rotation table. |
| **Score** | The ETF composite score — a weighted blend of Momentum, Trend, and Participation sub-scores. Shown as a coloured bar. | 0 – 1 | The primary ranking criterion. Higher is better. |
| **Mom** | Momentum sub-score. Captures rate-of-change across 5d, 10d, 20d, and 60d windows with recency weighting. | 0 – 1 | High values mean the ETF has been accelerating. Extreme values (>0.95) can indicate short-term over-extension. |
| **Trend** | Trend sub-score. Based on moving-average alignment (20/50/100/200 MA), slope, and price position relative to MAs. | 0 – 1 | The "structural health" gauge. Scores above 0.7 indicate a clean, well-ordered uptrend. |
| **Part** | Participation sub-score. Measures how broad-based the move is — volume confirmation, breadth within the ETF, and relative volume. | 0 – 1 | High participation means the move is well-supported. Low participation in a high-momentum ETF is a divergence warning. |
| **RSI** | 14-day Relative Strength Index of the ETF itself. | 0 – 100 | 30–70 is neutral. Below 30 is oversold (potential bounce). Above 70 is overbought (potential pullback, but can persist in strong trends). |
| **RVol** | Relative Volume — today's volume divided by the 20-day average volume. | 0 – ∞ | Above 1.5 means unusual activity. Below 0.5 means disinterest. Breakouts on high RVol are more trustworthy. |
| **Ret 20d** | Total return over the last 20 trading days. | Percentage | Context for the score. A high score with a large positive return may mean "already moved." A high score with a modest return could be early-stage. |

### How to Use for Decisions

- Pick ETFs from the **top quartile** by Score that belong to sectors in the Leading or Improving quadrant.
- Cross-check Mom vs. Part: strong momentum with weak participation is fragile. Prefer ETFs where all three sub-scores are reasonably aligned.
- Use RSI to time entries: even if the Score is high, consider waiting for a pullback to RSI 40–50 rather than chasing at RSI 75+.
- Use RVol spikes to confirm breakouts.

---

## 5. Portfolio Allocations

The final output of the allocation engine — the names the system actually recommends holding, with sizing.

### Column Definitions

| Column | Description | Range | How to Read |
|--------|-------------|-------|-------------|
| **#** | Rank within the portfolio (by adjusted score). | 1 – N | — |
| **Ticker** | Stock or ETF ticker. | — | — |
| **Score** | The adjusted composite score. This is the raw composite multiplied by regime, sector, and volatility adjustments. Shown as a coloured bar. | 0 – 1 | The "conviction meter." Higher means the system has more confidence. |
| **Action** | The system's recommendation badge. | STRONG BUY / BUY / HOLD / SELL | Only STRONG BUY and BUY names appear in the portfolio. |
| **Sector** | GICS sector. | — | Check for concentration — if 8 of 12 names are in Technology, you have sector risk. |
| **RS Regime** | The name's relative-strength regime vs. its sector. | leading / improving / weakening / lagging | Tells you whether this stock is outperforming its sector peers. A "leading" stock in a "leading" sector is the strongest possible setup. |
| **RSI** | 14-day RSI of the individual stock. | 0 – 100 | Same interpretation as the ETF table. Prefer entries below 65 for new positions. |
| **ADX** | 14-day Average Directional Index. Measures trend strength regardless of direction. | 0 – 100 | Below 20: no trend (range-bound). 20–40: healthy trend. Above 40: very strong trend. Above 60: extreme, potential exhaustion. |
| **RVol** | Relative Volume. | 0 – ∞ | Same as ETF table. |
| **Vol Fav** | Volatility Favourability. Evaluates whether the stock's recent volatility profile favours a long position. | `favorable` / `neutral` / `unfavorable` | `favorable`: Volatility is contracting or positioned for an expansion in the right direction (bullish compression). `neutral`: No strong volatility signal either way. `unfavorable`: Volatility is expanding in a way that suggests risk (e.g. downside gaps, whipsaws). **Avoid adding to `unfavorable` names. Prefer `favorable` for new entries.** |
| **Trend** | Trend sub-score for the individual name. | 0 – 1 | Same meaning as in the ETF table but applied to a single stock. |
| **Partic.** | Participation sub-score. | 0 – 1 | Volume and breadth confirmation at the individual name level. |
| **Weight** | The recommended portfolio weight (capital allocation percentage). | 0 – 100% | Higher weight = higher conviction from the optimizer. Weights are constrained by max-position-size rules and sector caps. |

### How to Use for Decisions

- This is the **actionable basket**. If you are running this as a systematic strategy, buy these names at these weights.
- If you are using it as a screen, focus on the top half of the table — they have the highest scores and weights.
- Always cross-reference Vol Fav: a STRONG BUY with `unfavorable` volatility deserves a smaller position or a delayed entry.
- Check sector concentration: the system applies sector caps, but review manually if you have additional sector views.
- Watch for low-ADX names (below 20). They may be in the portfolio on score merit but lack a trending environment — expect choppier returns.

### Interpreting Vol Fav Values

A raw decimal like `0.64` in the Vol Fav column indicates the underlying favourability score before it is bucketed into a label. Interpret it on a 0–1 scale:

| Value | Interpretation |
|-------|---------------|
| > 0.60 | Favourable — volatility dynamics support a long position |
| 0.40 – 0.60 | Neutral — no strong signal from volatility |
| < 0.40 | Unfavourable — volatility headwinds present |

---

## 6. All Buy-Rated Names

An expanded list of every name that received STRONG BUY or BUY — including names that did not make it into the portfolio due to capacity constraints, sector caps, or lower scores. This is your **bench** or **watch list**.

The columns are identical to the Portfolio table (minus the Weight column, since these names are not yet allocated).

### How to Use for Decisions

- If a portfolio name hits a stop or you need to replace it, draw from this list.
- Names near the top of the Buy list that are not in the portfolio were likely crowded out by sector caps — they may be equally strong.
- Sort mentally by sector to find alternatives if you want to swap within a sector.

---

## 7. Full Universe

Every name that was scored, regardless of action. This table includes HOLD and SELL names alongside buys.

### How to Use for Decisions

- Use this for **screening and research**. Filter for names with high Trend and Participation scores that are currently rated HOLD — they may be on the verge of upgrading.
- SELL-rated names with deteriorating RS Regime are candidates for short lists or avoid lists.
- Compare the full universe distribution: if most names cluster at low scores, the market is in poor health regardless of what the top names show.

---

## 8. Selling Exhaustion

This section identifies stocks that have been under sustained selling pressure and may be approaching a mean-reversion bounce. These are **not** trend-following plays — they are contrarian setups for experienced traders.

### Column Definitions

| Column | Description | Range | How to Read |
|--------|-------------|-------|-------------|
| **#** | Rank by exhaustion + reversal score. | 1 – N | — |
| **Ticker** | Stock ticker. | — | — |
| **Status** | The stage of the exhaustion pattern. | `TRIGGERED_REVERSAL` / `EARLY_SIGNAL` / `WATCHING` | See status definitions below. |
| **Quality** | Quality grade of the bounce setup. | `HIGH_QUALITY_BOUNCE` / `EARLY` / `LOW` | See quality definitions below. |
| **Exh.** | Selling Exhaustion Score — how extreme the selling has been. Based on RSI depth, consecutive down days, volume climax, and distance below moving averages. | 0 – 10 | Higher means more extreme selling. Scores of 6+ indicate heavy exhaustion. |
| **Rev.** | Reversal Trigger Score — evidence that the selling is actually stopping. Based on bullish candle patterns, RSI divergence, volume dry-up, and first higher low. | 0 – 10 | Higher means more reversal evidence. Scores of 4+ are meaningful. |
| **RSI** | Current 14-day RSI. | 0 – 100 | In this context, you want to see RSI below 35 (deeply oversold). Bounces from sub-30 RSI with a triggered reversal are the classic setup. |
| **5d Chg** | Price change over the last 5 trading days. | Percentage | A small positive change after heavy selling may confirm the reversal. A continued large negative change despite high exhaustion score means the selling is not done yet. |
| **Sector** | GICS sector. | — | Helps filter: exhaustion names in Leading sectors are higher quality than those in Lagging sectors. |

### Status Definitions

| Status | Meaning | Action |
|--------|---------|--------|
| **TRIGGERED_REVERSAL** | Both exhaustion and reversal criteria have been met. The system sees evidence that selling pressure is lifting. | This is the actionable signal. Evaluate entry on the next session. |
| **EARLY_SIGNAL** | Exhaustion is present but reversal evidence is still thin. | Watch closely. May trigger in 1–3 sessions. |
| **WATCHING** | Some oversold characteristics but no exhaustion or reversal yet. | On the radar only. Not actionable. |

### Quality Definitions

| Quality | Meaning | Confidence |
|---------|---------|------------|
| **HIGH_QUALITY_BOUNCE** | The name has strong fundamentals, is in a sector with decent relative strength, RSI is deeply oversold, and there is clear volume or candle confirmation of reversal. | Highest confidence for a mean-reversion trade. |
| **EARLY** | Some reversal evidence exists but it is not fully confirmed — for example, one bullish candle but no higher low yet, or RSI just crossed back above 30. | Moderate confidence. Consider a smaller position or wait one more day. |
| **LOW** | The name is oversold but quality signals are absent — weak sector, no volume confirmation, or the stock is in a structural downtrend. | Low confidence. These can keep falling. |

### How to Use for Decisions

- Focus on **TRIGGERED_REVERSAL** + **HIGH_QUALITY_BOUNCE** names first. These are the best risk/reward setups.
- Cross-reference with the Sector Rotation table: a bounce candidate in a 🟢 Leading or 🔵 Improving sector has sector tailwinds supporting the recovery.
- A bounce candidate in a 🔴 Lagging sector is fighting the tide — the bounce may be short-lived.
- Use tight stops on exhaustion plays. These are mean-reversion trades, not trend trades. If the reversal fails (price makes a new low), exit immediately.
- The 5d Change column is your real-time confirmation. If it is positive, the bounce may already be underway. If it is still deeply negative, the trigger may be premature.

---

## 9. Decision Framework

### Step-by-Step: How to Process a New Report

**Step 1 — Read the Market Overview (30 seconds)**

Check Breadth and Volatility. If breadth is healthy and volatility is low/moderate, you have a green light for full exposure. If either is deteriorating, scale down mentally before looking at individual names.

**Step 2 — Check Sector Rotation (1 minute)**

Identify which sectors are Leading and Improving. These are where you want concentrated exposure. Note any sectors transitioning from Leading to Weakening — begin planning exits from those sectors.

**Step 3 — Review Portfolio Allocations (2 minutes)**

This is your primary action list. Compare it against your current holdings. Identify new additions (names you do not own), removals (names you own that are no longer in the portfolio), and weight changes.

**Step 4 — Scan Buy-Rated Names (1 minute)**

If you cannot buy a portfolio name (illiquid, restricted, or you have a view against it), find a substitute here.

**Step 5 — Review Selling Exhaustion (1 minute)**

Only if you trade mean-reversion. Look at TRIGGERED_REVERSAL + HIGH_QUALITY_BOUNCE in sectors that are not Lagging.

### Signal Confluence Cheat Sheet

| Setup | Conditions | Conviction |
|-------|-----------|------------|
| **Strongest long** | STRONG BUY + Leading sector + Leading RS Regime + favorable Vol Fav + ADX 20–40 + RSI < 65 | ★★★★★ |
| **Strong long** | BUY + Leading or Improving sector + favorable or neutral Vol Fav | ★★★★ |
| **Moderate long** | BUY + any sector + neutral Vol Fav + RSI > 70 | ★★★ |
| **Caution** | HOLD + Weakening sector + unfavorable Vol Fav | ★★ |
| **Avoid / exit** | SELL + Lagging sector + unfavorable Vol Fav + ADX rising | ★ |
| **Contrarian bounce** | TRIGGERED_REVERSAL + HIGH_QUALITY_BOUNCE + Improving or Leading sector | ★★★ |

### Position Sizing Heuristic

Use the report's recommended Weight as a starting point. Then adjust:

- **Vol Fav = unfavorable** → reduce weight by 30–50%.
- **RSI > 75** → reduce weight by 20% or wait for a pullback.
- **ADX < 15** → reduce weight by 20% (range-bound, expect chop).
- **Elevated volatility regime** → the system already scales Exposure down, but you can further reduce individual weights if your risk tolerance is lower.

---

## 10. Glossary

| Term | Definition |
|------|-----------|
| **ADX (Average Directional Index)** | Measures the strength of a trend on a 0–100 scale. Does not indicate direction, only intensity. |
| **Blended Score** | A weighted mix of RRG relative-strength metrics and the ETF composite score for a sector. |
| **Breadth** | The percentage of stocks participating in a market move — typically measured by stocks above key moving averages. |
| **Composite Score** | A weighted sum of Momentum, Trend, and Participation sub-scores, scaled 0–1. |
| **Dispersion** | The cross-sectional standard deviation of returns across the universe. High dispersion = high differentiation between winners and losers. |
| **Excess Return** | A stock or ETF's return minus the benchmark's return over the same period. |
| **Participation Sub-Score** | Measures volume confirmation, breadth, and relative volume — whether the move is "real" or thin. |
| **Relative Volume (RVol)** | Current volume divided by the 20-day average volume. A multiplier: 1.0 = normal, 2.0 = double normal. |
| **Relative Strength (RS)** | A stock or sector's performance relative to a benchmark (not to be confused with RSI). |
| **RRG (Relative Rotation Graph)** | A framework that plots RS Level (x-axis) vs. RS Momentum (y-axis) to classify assets into four quadrants. |
| **RSI (Relative Strength Index)** | A 0–100 momentum oscillator. Below 30 is oversold; above 70 is overbought. |
| **RS Regime** | The RRG quadrant a stock occupies relative to its sector or the market. |
| **Trend Sub-Score** | Captures moving-average alignment, slope, and price position vs. MAs. Reflects structural trend health. |
| **Volatility Favourability (Vol Fav)** | Whether a stock's volatility characteristics support or hinder a long position. Based on implied-vs-realised spread, volatility compression, and directional vol skew. |
| **Volatility Regime** | A classification of the current VIX environment: low, moderate, elevated, or crisis. |

---

*This document corresponds to reports generated by the Smart Money Rotation system. Column availability may vary depending on the market scanned and the data sources configured.*