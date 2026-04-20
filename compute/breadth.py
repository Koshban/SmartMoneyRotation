"""
compute/breadth.py
------------------
Market breadth analytics computed from an internal universe of
stocks.

Rather than relying on exchange-level advance/decline data (which
requires a paid feed), this module derives breadth from whatever
universe the caller provides.  With 15–50 stocks the readings are
noisy but still useful as a regime overlay; with 100+ they
converge toward traditional breadth measures.

Indicators
──────────
  advance_decline       daily advancing − declining count
  ad_line               cumulative A-D line
  adv_ratio             advancing / total (0‒1)
  mcclellan_osc         19/39 EMA of daily A-D (breadth momentum)
  mcclellan_sum         cumulative McClellan Oscillator
  pct_above_50          fraction of universe above 50-day SMA
  pct_above_200         fraction of universe above 200-day SMA
  new_highs             count making rolling 252-day high
  new_lows              count making rolling 252-day low
  hi_lo_diff            new_highs − new_lows
  hi_lo_ratio           new_highs / (new_highs + new_lows)
  thrust_ema            10-day EMA of adv_ratio
  breadth_thrust        1 when thrust_ema crosses above 61.5 %
  breadth_washout       1 when thrust_ema crosses below 25 %
  breadth_regime        strong / neutral / weak
  breadth_score         0–1 composite

Pipeline
────────
  {ticker: DataFrame}  universe of OHLCV DataFrames
       ↓
  align_universe()       — date-align closes into a panel
       ↓
  compute_advance_decline()
  compute_mcclellan()
  compute_pct_above_ma()
  compute_new_highs_lows()
  compute_breadth_thrust()
  compute_breadth_score()
  classify_breadth_regime()
       ↓
  compute_all_breadth()  — master orchestrator → single DataFrame
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import BREADTH_PARAMS


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _bp(key: str):
    """Fetch from BREADTH_PARAMS."""
    return BREADTH_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  UNIVERSE ALIGNMENT
# ═══════════════════════════════════════════════════════════════

def align_universe(
    universe: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Build date-aligned panels from a universe of DataFrames.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} with at least a ``close`` column
        and a DatetimeIndex.

    Returns
    -------
    closes  : pd.DataFrame — columns = tickers, rows = dates
    volumes : pd.DataFrame — same shape, daily volume
    n       : int          — number of tickers in the panel
    """
    close_frames: dict[str, pd.Series]  = {}
    volume_frames: dict[str, pd.Series] = {}

    for ticker, df in universe.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        close_frames[ticker]  = df["close"].copy()
        volume_frames[ticker] = (
            df["volume"].copy() if "volume" in df.columns
            else pd.Series(np.nan, index=df.index)
        )

    if not close_frames:
        return pd.DataFrame(), pd.DataFrame(), 0

    closes  = pd.DataFrame(close_frames).sort_index()
    volumes = pd.DataFrame(volume_frames).sort_index()

    return closes, volumes, len(close_frames)


# ═══════════════════════════════════════════════════════════════
#  ADVANCE / DECLINE
# ═══════════════════════════════════════════════════════════════

def compute_advance_decline(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Daily advancing / declining / unchanged counts and the
    cumulative A-D line.
    """
    if closes.empty:
        return pd.DataFrame()

    daily_ret = closes.pct_change()

    advancing  = (daily_ret > 0).sum(axis=1)
    declining  = (daily_ret < 0).sum(axis=1)
    unchanged  = (daily_ret == 0).sum(axis=1)
    traded     = advancing + declining + unchanged

    result = pd.DataFrame(index=closes.index)
    result["advancing"]        = advancing.astype(int)
    result["declining"]        = declining.astype(int)
    result["unchanged"]        = unchanged.astype(int)
    result["total_traded"]     = traded.astype(int)
    result["advance_decline"]  = (advancing - declining).astype(int)
    result["ad_line"]          = result["advance_decline"].cumsum()

    # Advance ratio (0–1)
    result["adv_ratio"] = np.where(
        traded > 0,
        advancing / traded,
        np.nan,
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  McCLELLAN OSCILLATOR  +  SUMMATION INDEX
# ═══════════════════════════════════════════════════════════════

def compute_mcclellan(
    ad_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    McClellan Oscillator = EMA(fast) − EMA(slow) of daily A-D.
    McClellan Summation Index = cumulative sum of the oscillator.

    Positive oscillator → breadth momentum expanding.
    Rising summation    → sustained breadth improvement.
    """
    if ad_data.empty or "advance_decline" not in ad_data.columns:
        return ad_data

    ad_series = ad_data["advance_decline"].astype(float)

    fast = _bp("mcclellan_fast")
    slow = _bp("mcclellan_slow")

    ema_fast = ad_series.ewm(span=fast, adjust=False).mean()
    ema_slow = ad_series.ewm(span=slow, adjust=False).mean()

    result = ad_data.copy()
    result["mcclellan_osc"] = ema_fast - ema_slow
    result["mcclellan_sum"] = result["mcclellan_osc"].cumsum()

    # Normalised oscillator: divide by total traded to make it
    # comparable across different universe sizes
    total = result["total_traded"].replace(0, np.nan)
    result["mcclellan_osc_pct"] = result["mcclellan_osc"] / total

    return result


# ═══════════════════════════════════════════════════════════════
#  PERCENT ABOVE MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════

def compute_pct_above_ma(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each day, fraction of stocks whose close is above their
    own 50-day and 200-day SMA.

    Returns a DataFrame indexed by date with columns:
        pct_above_50, pct_above_200
    """
    if closes.empty:
        return pd.DataFrame()

    ma_short = _bp("ma_short")
    ma_long  = _bp("ma_long")

    sma50  = closes.rolling(ma_short, min_periods=ma_short).mean()
    sma200 = closes.rolling(ma_long, min_periods=ma_long).mean()

    above_50  = (closes > sma50).sum(axis=1)
    above_200 = (closes > sma200).sum(axis=1)

    count_50  = sma50.notna().sum(axis=1).replace(0, np.nan)
    count_200 = sma200.notna().sum(axis=1).replace(0, np.nan)

    result = pd.DataFrame(index=closes.index)
    result["pct_above_50"]  = above_50 / count_50
    result["pct_above_200"] = above_200 / count_200

    return result


# ═══════════════════════════════════════════════════════════════
#  NEW HIGHS / NEW LOWS
# ═══════════════════════════════════════════════════════════════

def compute_new_highs_lows(
    closes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Daily count of stocks making a rolling 252-day high or low.

    hi_lo_ratio = new_highs / (new_highs + new_lows), smoothed
    with a 10-day SMA to reduce noise.
    """
    if closes.empty:
        return pd.DataFrame()

    window = _bp("high_low_window")

    roll_high = closes.rolling(window, min_periods=window).max()
    roll_low  = closes.rolling(window, min_periods=window).min()

    is_new_high = (closes >= roll_high) & roll_high.notna()
    is_new_low  = (closes <= roll_low) & roll_low.notna()

    result = pd.DataFrame(index=closes.index)
    result["new_highs"]  = is_new_high.sum(axis=1).astype(int)
    result["new_lows"]   = is_new_low.sum(axis=1).astype(int)
    result["hi_lo_diff"] = result["new_highs"] - result["new_lows"]

    total_hl = result["new_highs"] + result["new_lows"]
    result["hi_lo_ratio"] = np.where(
        total_hl > 0,
        result["new_highs"] / total_hl,
        0.5,  # neutral when no new highs or lows
    )
    result["hi_lo_ratio_sma"] = (
        result["hi_lo_ratio"].rolling(10, min_periods=1).mean()
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  BREADTH THRUST
# ═══════════════════════════════════════════════════════════════

def compute_breadth_thrust(
    breadth: pd.DataFrame,
) -> pd.DataFrame:
    """
    Breadth thrust detection.

    A breadth thrust occurs when the 10-day EMA of the advance
    ratio surges above a threshold (historically 61.5 %).  This
    is one of the most reliable bullish signals in market
    history — when breadth expands that rapidly, the odds of a
    sustained rally are very high.

    A breadth washout occurs when the same EMA collapses below
    25 %, marking capitulatory selling — often a setup for a
    subsequent thrust.
    """
    if breadth.empty or "adv_ratio" not in breadth.columns:
        return breadth

    result = breadth.copy()
    window = _bp("thrust_window")

    result["thrust_ema"] = (
        result["adv_ratio"]
        .ewm(span=window, adjust=False)
        .mean()
    )

    up_thresh = _bp("thrust_up_threshold")
    dn_thresh = _bp("thrust_dn_threshold")

    ema      = result["thrust_ema"]
    prev_ema = ema.shift(1)

    result["breadth_thrust"] = (
        ((ema >= up_thresh) & (prev_ema < up_thresh)).astype(int)
    )
    result["breadth_washout"] = (
        ((ema <= dn_thresh) & (prev_ema > dn_thresh)).astype(int)
    )

    # Rolling flag: 1 for 20 days after a thrust fires
    result["thrust_active"] = (
        result["breadth_thrust"]
        .rolling(20, min_periods=1)
        .max()
        .fillna(0)
        .astype(int)
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  BREADTH SCORE  (composite 0–1)
# ═══════════════════════════════════════════════════════════════

def compute_breadth_score(breadth: pd.DataFrame) -> pd.DataFrame:
    """
    Composite breadth score (0–1) combining:

      0.30 × adv_ratio           (advancing breadth)
      0.25 × pct_above_50        (short-term health)
      0.20 × pct_above_200       (long-term health)
      0.15 × hi_lo_ratio_sma     (new-high leadership)
      0.10 × mcclellan_norm      (breadth momentum)

    Each component is already on [0, 1] (or clipped there).
    """
    if breadth.empty:
        return breadth

    result = breadth.copy()

    # Normalise McClellan oscillator pct to [0, 1] via sigmoid
    mc = result.get("mcclellan_osc_pct", pd.Series(0.5, index=result.index))
    mc_norm = 1.0 / (1.0 + np.exp(-10.0 * mc.fillna(0)))

    components = {
        "adv_ratio":       0.30,
        "pct_above_50":    0.25,
        "pct_above_200":   0.20,
        "hi_lo_ratio_sma": 0.15,
    }

    score = pd.Series(0.0, index=result.index)
    for col, weight in components.items():
        vals = result[col].fillna(0.5) if col in result.columns else 0.5
        score += weight * vals

    score += 0.10 * mc_norm

    result["breadth_score"] = score.clip(0, 1)

    return result


# ═══════════════════════════════════════════════════════════════
#  REGIME CLASSIFICATION
# ═══════════════════════════════════════════════════════════════

def classify_breadth_regime(
    breadth: pd.DataFrame,
) -> pd.DataFrame:
    """
    Classify each day's breadth as strong / neutral / weak.

    Uses a smoothed version of ``breadth_score`` (5-day SMA) so
    the regime doesn't flip on single noisy days.
    """
    if breadth.empty or "breadth_score" not in breadth.columns:
        return breadth

    result = breadth.copy()

    smoothed = (
        result["breadth_score"]
        .rolling(5, min_periods=1)
        .mean()
    )

    strong = _bp("regime_strong_pct")
    weak   = _bp("regime_weak_pct")

    conditions = [
        smoothed >= strong,
        smoothed <= weak,
    ]
    choices = ["strong", "weak"]

    result["breadth_regime"] = np.select(
        conditions, choices, default="neutral"
    )
    result["breadth_score_smooth"] = smoothed

    return result


# ═══════════════════════════════════════════════════════════════
#  UP-VOLUME RATIO
# ═══════════════════════════════════════════════════════════════

def compute_up_volume_ratio(
    closes: pd.DataFrame,
    volumes: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fraction of total volume that belongs to advancing stocks.

    up_volume_ratio > 0.9 on a single day is a 90 % up-volume
    day — historically one of the strongest breadth signals.
    """
    if closes.empty or volumes.empty:
        return pd.DataFrame()

    daily_ret = closes.pct_change()
    up_mask   = (daily_ret > 0)

    up_vol    = (volumes * up_mask.astype(float)).sum(axis=1)
    total_vol = volumes.sum(axis=1).replace(0, np.nan)

    result = pd.DataFrame(index=closes.index)
    result["up_volume"]       = up_vol
    result["total_volume"]    = total_vol.fillna(0)
    result["up_volume_ratio"] = (up_vol / total_vol).clip(0, 1)
    result["up_vol_sma10"]    = (
        result["up_volume_ratio"]
        .rolling(10, min_periods=1)
        .mean()
    )

    # Flag 90 % up-volume days
    result["ninety_pct_up_day"] = (
        (result["up_volume_ratio"] >= 0.90).astype(int)
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_breadth(
    universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Full breadth pipeline.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} with at least ``close`` and
        optionally ``volume`` columns.

    Returns
    -------
    pd.DataFrame
        One row per trading day with all breadth indicators,
        breadth_score, and breadth_regime.
    """
    closes, volumes, n = align_universe(universe)

    if n < _bp("min_stocks"):
        return pd.DataFrame()

    # ── Advance / Decline ─────────────────────────────────────
    breadth = compute_advance_decline(closes)

    # ── McClellan ─────────────────────────────────────────────
    breadth = compute_mcclellan(breadth)

    # ── Percent above MAs ─────────────────────────────────────
    pct_ma = compute_pct_above_ma(closes)
    breadth = breadth.join(pct_ma, how="left")

    # ── New highs / lows ──────────────────────────────────────
    hi_lo = compute_new_highs_lows(closes)
    breadth = breadth.join(hi_lo, how="left")

    # ── Up-volume ratio ───────────────────────────────────────
    up_vol = compute_up_volume_ratio(closes, volumes)
    if not up_vol.empty:
        breadth = breadth.join(up_vol, how="left")

    # ── Thrust ────────────────────────────────────────────────
    breadth = compute_breadth_thrust(breadth)

    # ── Composite score ───────────────────────────────────────
    breadth = compute_breadth_score(breadth)

    # ── Regime ────────────────────────────────────────────────
    breadth = classify_breadth_regime(breadth)

    # ── Metadata ──────────────────────────────────────────────
    breadth["breadth_n_stocks"] = n

    return breadth


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def breadth_report(breadth: pd.DataFrame, lookback: int = 5) -> str:
    """
    Format a human-readable breadth summary for the latest
    *lookback* days.
    """
    if breadth.empty:
        return "No breadth data available."

    ln: list[str] = []
    div = "=" * 60
    sub = "-" * 60

    tail = breadth.tail(lookback)
    last = breadth.iloc[-1]

    ln.append(div)
    ln.append("MARKET BREADTH REPORT")
    ln.append(div)
    ln.append(
        f"  Date:              {breadth.index[-1].strftime('%Y-%m-%d')}"
    )
    ln.append(
        f"  Universe size:     "
        f"{int(last.get('breadth_n_stocks', 0))} stocks"
    )
    ln.append(
        f"  Breadth regime:    {last.get('breadth_regime', '?')}"
    )
    ln.append(
        f"  Breadth score:     "
        f"{last.get('breadth_score', 0):.3f}  "
        f"(smooth: {last.get('breadth_score_smooth', 0):.3f})"
    )

    ln.append("")
    ln.append(sub)
    ln.append("CURRENT READINGS")
    ln.append(sub)
    ln.append(
        f"  Advancing:         "
        f"{int(last.get('advancing', 0))} / "
        f"{int(last.get('total_traded', 0))}  "
        f"({last.get('adv_ratio', 0):.1%})"
    )
    ln.append(
        f"  A-D line:          {int(last.get('ad_line', 0))}"
    )
    ln.append(
        f"  McClellan Osc:     {last.get('mcclellan_osc', 0):.2f}"
    )
    ln.append(
        f"  McClellan Sum:     {last.get('mcclellan_sum', 0):.1f}"
    )
    ln.append(
        f"  % above 50d SMA:   "
        f"{last.get('pct_above_50', 0):.1%}"
    )
    ln.append(
        f"  % above 200d SMA:  "
        f"{last.get('pct_above_200', 0):.1%}"
    )
    ln.append(
        f"  New highs:         {int(last.get('new_highs', 0))}"
    )
    ln.append(
        f"  New lows:          {int(last.get('new_lows', 0))}"
    )
    ln.append(
        f"  Hi-Lo ratio:       "
        f"{last.get('hi_lo_ratio_sma', 0):.3f}"
    )

    if "up_volume_ratio" in last.index:
        ln.append(
            f"  Up-volume ratio:   "
            f"{last.get('up_volume_ratio', 0):.1%}"
        )

    if last.get("thrust_active", 0) == 1:
        ln.append(f"  ⚡ Breadth thrust ACTIVE")
    if last.get("breadth_washout", 0) == 1:
        ln.append(f"  ⚠ Breadth washout detected")

    # ── Recent trend ──────────────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append(f"LAST {lookback} DAYS")
    ln.append(sub)
    header = (
        f"  {'Date':<12} {'Adv':>4} {'Dec':>4} "
        f"{'Ratio':>6} {'McCl':>6} {'%>50d':>6} "
        f"{'Score':>6} {'Regime':<8}"
    )
    ln.append(header)
    ln.append(
        f"  {'──────────':<12} {'───':>4} {'───':>4} "
        f"{'─────':>6} {'─────':>6} {'─────':>6} "
        f"{'─────':>6} {'──────':<8}"
    )
    for dt, row in tail.iterrows():
        date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)
        ln.append(
            f"  {date_str:<12} "
            f"{int(row.get('advancing', 0)):>4} "
            f"{int(row.get('declining', 0)):>4} "
            f"{row.get('adv_ratio', 0):>5.1%} "
            f"{row.get('mcclellan_osc', 0):>6.1f} "
            f"{row.get('pct_above_50', 0):>5.1%} "
            f"{row.get('breadth_score', 0):>6.3f} "
            f"{str(row.get('breadth_regime', '?')):<8}"
        )

    return "\n".join(ln)

# ═══════════════════════════════════════════════════════════════
#  SCORING PIPELINE BRIDGE
# ═══════════════════════════════════════════════════════════════

def breadth_to_pillar_scores(
    breadth: pd.DataFrame,
    symbols: list[str],
    scale: float = 100.0,
) -> pd.DataFrame:
    """
    Convert universe-level breadth_score into a per-symbol
    DataFrame shaped for the composite scoring pipeline.

    Option A (broadcast): every symbol receives the same
    daily breadth score.  This is appropriate when breadth
    is used as a market-regime overlay.

    Parameters
    ----------
    breadth : pd.DataFrame
        Output of ``compute_all_breadth()``, must contain
        ``breadth_score`` column (values 0–1).
    symbols : list[str]
        Column names for the output (the ETF/ticker universe).
    scale : float
        Multiply by this to match pillar score range.
        Default 100 converts 0–1 → 0–100.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows × symbol columns, values 0–100.
    """
    if breadth.empty or "breadth_score" not in breadth.columns:
        return pd.DataFrame(index=breadth.index, columns=symbols, dtype=float)

    score = breadth["breadth_score"] * scale

    return pd.DataFrame(
        {symbol: score for symbol in symbols},
        index=breadth.index,
    )


def breadth_to_pillar_scores_grouped(
    group_breadth: dict[str, pd.DataFrame],
    group_map: dict[str, list[str]],
    fallback_breadth: pd.DataFrame | None = None,
    scale: float = 100.0,
) -> pd.DataFrame:
    """
    Option B: per-group breadth scores mapped to individual symbols.

    Parameters
    ----------
    group_breadth : dict
        {group_name: breadth_df} — output of ``compute_all_breadth()``
        run on each group's constituent universe.
    group_map : dict
        {group_name: [symbol, ...]} — maps groups to ETF symbols.
    fallback_breadth : pd.DataFrame or None
        Universe-level breadth for symbols not in any group.
    scale : float
        Score multiplier (default 100).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows × symbol columns, values 0–100.
    """
    all_symbols = [s for syms in group_map.values() for s in syms]

    # Collect all date indices
    all_dates = set()
    for bdf in group_breadth.values():
        if not bdf.empty:
            all_dates.update(bdf.index)
    if fallback_breadth is not None and not fallback_breadth.empty:
        all_dates.update(fallback_breadth.index)

    if not all_dates:
        return pd.DataFrame(columns=all_symbols, dtype=float)

    idx = pd.DatetimeIndex(sorted(all_dates))
    result = pd.DataFrame(index=idx, columns=all_symbols, dtype=float)

    for group_name, symbols in group_map.items():
        bdf = group_breadth.get(group_name)
        if bdf is None or bdf.empty or "breadth_score" not in bdf.columns:
            continue
        score = bdf["breadth_score"].reindex(idx) * scale
        for sym in symbols:
            if sym in result.columns:
                result[sym] = score

    # Fill unmapped symbols with fallback
    if fallback_breadth is not None and "breadth_score" in fallback_breadth.columns:
        fb_score = fallback_breadth["breadth_score"].reindex(idx) * scale
        for sym in result.columns:
            if result[sym].isna().all():
                result[sym] = fb_score

    return result