"""
COMPUTE :

"""
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

#################################    

"""
compute/indicators.py
---------------------
Pure functions that compute technical indicators on OHLCV DataFrames.

No database knowledge, no scoring opinions — just math.

Every function:
    • takes a DataFrame with columns: open, high, low, close, volume
    • returns the same DataFrame with new indicator columns appended
    • pulls default parameters from common.config.INDICATOR_PARAMS

Master entry point:
    df = compute_all_indicators(df)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

_REQUIRED_COLS = {"open", "high", "low", "close", "volume"}


def _validate_ohlcv(df: pd.DataFrame) -> None:
    """Raise if the DataFrame is missing any required OHLCV columns."""
    missing = _REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    if df.empty:
        raise ValueError("DataFrame is empty")


def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _wilder_smooth(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing method (used by RSI, ADX, ATR)."""
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """
    Slope of a least-squares linear fit over a rolling window.
    Positive slope = series trending upward.
    """
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RETURNS
# ═══════════════════════════════════════════════════════════════

def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """N-day percentage returns for each window in config."""
    for w in INDICATOR_PARAMS["return_windows"]:
        df[f"ret_{w}d"] = df["close"].pct_change(w)
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RSI
# ═══════════════════════════════════════════════════════════════

def add_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Relative Strength Index (Wilder's smoothing).
    Output: rsi_{period}  (0–100 scale)
    """
    period = INDICATOR_PARAMS["rsi_period"]
    delta = df["close"].diff()

    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    avg_gain = _wilder_smooth(gain, period)
    avg_loss = _wilder_smooth(loss, period)

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    df[f"rsi_{period}"] = 100.0 - (100.0 / (1.0 + rs))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. MACD
# ═══════════════════════════════════════════════════════════════

def add_macd(df: pd.DataFrame) -> pd.DataFrame:
    """
    MACD line, signal line, histogram.
    Outputs: macd_line, macd_signal, macd_hist
    """
    fast   = INDICATOR_PARAMS["macd_fast"]
    slow   = INDICATOR_PARAMS["macd_slow"]
    signal = INDICATOR_PARAMS["macd_signal"]

    ema_fast = _ema(df["close"], fast)
    ema_slow = _ema(df["close"], slow)

    df["macd_line"]   = ema_fast - ema_slow
    df["macd_signal"] = _ema(df["macd_line"], signal)
    df["macd_hist"]   = df["macd_line"] - df["macd_signal"]
    return df


# ═══════════════════════════════════════════════════════════════
#  4. ADX  (Average Directional Index)
# ═══════════════════════════════════════════════════════════════

def add_adx(df: pd.DataFrame) -> pd.DataFrame:
    """
    ADX with +DI / -DI.
    Outputs: adx_{period}, plus_di, minus_di
    """
    period = INDICATOR_PARAMS["adx_period"]
    high   = df["high"]
    low    = df["low"]
    close  = df["close"]

    # ── True Range ──────────────────────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = _wilder_smooth(tr, period)

    # ── Directional Movement ────────────────────────────────
    up_move   = high - high.shift(1)
    down_move = low.shift(1) - low

    plus_dm = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=df.index,
    )
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=df.index,
    )

    plus_di  = 100.0 * _wilder_smooth(plus_dm,  period) / atr.replace(0, np.nan)
    minus_di = 100.0 * _wilder_smooth(minus_dm, period) / atr.replace(0, np.nan)

    di_sum  = plus_di + minus_di
    di_diff = (plus_di - minus_di).abs()
    dx      = 100.0 * di_diff / di_sum.replace(0, np.nan)

    df[f"adx_{period}"] = _wilder_smooth(dx, period)
    df["plus_di"]       = plus_di
    df["minus_di"]      = minus_di
    return df


# ═══════════════════════════════════════════════════════════════
#  5. MOVING AVERAGES
# ═══════════════════════════════════════════════════════════════

def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    EMA and SMA lines, plus price-to-MA distance (%).
    Outputs: ema_{p}, sma_{s}, sma_{l},
                close_vs_ema_{p}_pct, close_vs_sma_{l}_pct
    """
    ema_p = INDICATOR_PARAMS["ema_period"]
    sma_s = INDICATOR_PARAMS["sma_short"]
    sma_l = INDICATOR_PARAMS["sma_long"]

    df[f"ema_{ema_p}"] = _ema(df["close"], ema_p)
    df[f"sma_{sma_s}"] = _sma(df["close"], sma_s)
    df[f"sma_{sma_l}"] = _sma(df["close"], sma_l)

    # Distance from MA (positive = price above MA)
    df[f"close_vs_ema_{ema_p}_pct"] = (
        (df["close"] / df[f"ema_{ema_p}"] - 1.0) * 100.0
    )
    df[f"close_vs_sma_{sma_l}_pct"] = (
        (df["close"] / df[f"sma_{sma_l}"] - 1.0) * 100.0
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. ATR  (Average True Range)
# ═══════════════════════════════════════════════════════════════

def add_atr(df: pd.DataFrame) -> pd.DataFrame:
    """
    ATR in absolute and percentage-of-price terms.
    Percentage ATR makes cross-asset comparison possible.
    Outputs: atr_{period}, atr_{period}_pct
    """
    period     = INDICATOR_PARAMS["atr_period"]
    prev_close = df["close"].shift(1)

    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)

    df[f"atr_{period}"]     = _wilder_smooth(tr, period)
    df[f"atr_{period}_pct"] = df[f"atr_{period}"] / df["close"] * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
#  7. REALIZED VOLATILITY
# ═══════════════════════════════════════════════════════════════

def add_realized_vol(df: pd.DataFrame) -> pd.DataFrame:
    """
    Annualised realized volatility from log returns.
    Also computes 5-day change to detect vol expansion / contraction.
    Outputs: realized_vol_{w}d, realized_vol_{w}d_chg5
    """
    window  = INDICATOR_PARAMS["realized_vol_window"]
    log_ret = np.log(df["close"] / df["close"].shift(1))

    col = f"realized_vol_{window}d"
    df[col]           = log_ret.rolling(window).std() * np.sqrt(252) * 100.0
    df[f"{col}_chg5"] = df[col].diff(5)
    return df


# ═══════════════════════════════════════════════════════════════
#  8. OBV  (On-Balance Volume)
# ═══════════════════════════════════════════════════════════════

def add_obv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cumulative OBV plus its 10-day slope.
    Rising OBV slope on an up-trend = accumulation confirmation.
    Outputs: obv, obv_slope_10d
    """
    if not INDICATOR_PARAMS.get("obv", True):
        return df

    sign = np.sign(df["close"].diff()).fillna(0.0)
    df["obv"] = (sign * df["volume"]).cumsum()
    df["obv_slope_10d"] = _rolling_slope(df["obv"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
#  9. ACCUMULATION / DISTRIBUTION LINE
# ═══════════════════════════════════════════════════════════════

def add_ad_line(df: pd.DataFrame) -> pd.DataFrame:
    """
    A/D line using the Close Location Value (CLV) multiplier.
    CLV = [(close-low) - (high-close)] / (high-low)
    Outputs: ad_line, ad_line_slope_10d
    """
    if not INDICATOR_PARAMS.get("ad_line", True):
        return df

    hl_range = df["high"] - df["low"]
    clv = (
        (df["close"] - df["low"]) - (df["high"] - df["close"])
    ) / hl_range.replace(0.0, np.nan)
    clv = clv.fillna(0.0)

    df["ad_line"]            = (clv * df["volume"]).cumsum()
    df["ad_line_slope_10d"]  = _rolling_slope(df["ad_line"], 10)
    return df


# ═══════════════════════════════════════════════════════════════
# 10. VOLUME METRICS
# ═══════════════════════════════════════════════════════════════

def add_volume_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average volume, relative volume, dollar volume.
    Relative volume > 1.5 often indicates institutional activity.
    Outputs: volume_avg_{w}, relative_volume,
                dollar_volume, dollar_volume_avg_{w}
    """
    w = INDICATOR_PARAMS["volume_avg_window"]

    df[f"volume_avg_{w}"]        = _sma(df["volume"], w)
    df["relative_volume"]        = (
        df["volume"] / df[f"volume_avg_{w}"].replace(0.0, np.nan)
    )
    df["dollar_volume"]          = df["close"] * df["volume"]
    df[f"dollar_volume_avg_{w}"] = _sma(df["dollar_volume"], w)
    return df


# ═══════════════════════════════════════════════════════════════
# 11. AMIHUD ILLIQUIDITY
# ═══════════════════════════════════════════════════════════════

def add_amihud(df: pd.DataFrame) -> pd.DataFrame:
    """
    Amihud (2002) illiquidity ratio: |return| / dollar_volume.
    Higher = more illiquid.  Scaled by 1e6 for readability.
    Requires dollar_volume column (call add_volume_metrics first).
    Outputs: amihud_{w}d
    """
    w = INDICATOR_PARAMS["amihud_window"]

    if "dollar_volume" not in df.columns:
        df["dollar_volume"] = df["close"] * df["volume"]

    daily_illiq = (
        df["close"].pct_change().abs()
        / df["dollar_volume"].replace(0.0, np.nan)
    )
    df[f"amihud_{w}d"] = daily_illiq.rolling(w).mean() * 1e6
    return df


# ═══════════════════════════════════════════════════════════════
# 12. VWAP DISTANCE  (daily-bar proxy)
# ═══════════════════════════════════════════════════════════════

def add_vwap_distance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling VWAP proxy for daily data.

    True VWAP needs intraday bars.  With daily data we approximate
    using a N-day volume-weighted average of the typical price
    (H+L+C)/3.  Distance > 0 means close is above VWAP — a sign
    of sustained buying pressure (accumulation).

    Outputs: vwap_{w}d, vwap_{w}d_dist_pct
    """
    w       = INDICATOR_PARAMS["volume_avg_window"]     # reuse 20D
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    tp_vol  = typical * df["volume"]

    rolling_tp_vol = tp_vol.rolling(w).sum()
    rolling_vol    = df["volume"].rolling(w).sum()

    vwap_col = f"vwap_{w}d"
    df[vwap_col]              = rolling_tp_vol / rolling_vol.replace(0.0, np.nan)
    df[f"{vwap_col}_dist_pct"] = (df["close"] / df[vwap_col] - 1.0) * 100.0
    return df


# ═══════════════════════════════════════════════════════════════
# MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute every technical indicator on an OHLCV DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: open, high, low, close, volume.
        Must be sorted by date ascending.

    Returns
    -------
    pd.DataFrame
        Copy of the input with ~30 indicator columns appended.
        Early rows will contain NaN where lookback is insufficient.
    """
    _validate_ohlcv(df)
    df = df.copy()

    # ── Order matters: volume_metrics before amihud ─────────
    df = add_returns(df)
    df = add_rsi(df)
    df = add_macd(df)
    df = add_adx(df)
    df = add_moving_averages(df)
    df = add_atr(df)
    df = add_realized_vol(df)
    df = add_obv(df)
    df = add_ad_line(df)
    df = add_volume_metrics(df)      # creates dollar_volume
    df = add_amihud(df)              # needs dollar_volume
    df = add_vwap_distance(df)

    return df

"""
compute/relative_strength.py
-----------------------------
Relative strength vs benchmark — the core rotation signal (Pillar 1).

Pure functions.  Takes a stock OHLCV DataFrame and a benchmark OHLCV
DataFrame, returns the stock DataFrame with RS columns appended.

No database knowledge.  No scoring opinions.  Just math.

Key concepts
------------
RS ratio   : stock_close / bench_close — rising means outperforming.
RS slope   : linear regression slope of the smoothed RS ratio.
             Positive  = money rotating IN.
             Negative  = money rotating OUT.
RS z-score : standardised slope for cross-ticker comparison.
RS momentum: short-term slope minus long-term slope (acceleration).
RS regime  : categorical label — leading / weakening / lagging / improving.
             "improving" is the sweet spot: early rotation before the crowd.

Why EMA / SMA / Volume here?
-----------------------------
indicators.py smooths the stock's own price.  This module smooths the
RS *ratio* (stock ÷ benchmark) — a completely different series.  Volume
is used to confirm whether RS improvement has institutional participation
or is just low-liquidity drift.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import INDICATOR_PARAMS

# ═══════════════════════════════════════════════════════════════
#  DEFAULTS  (fallback if config keys are missing)
# ═══════════════════════════════════════════════════════════════

_DEFAULTS = {
    "rs_ema_span":               10,
    "rs_sma_span":               50,
    "rs_slope_window":           20,
    "rs_zscore_window":          60,
    "rs_momentum_short":         10,
    "rs_momentum_long":          30,
    "rs_vol_confirm_threshold": 1.3,
    "volume_avg_window":         20,
}


def _p(key: str):
    """Fetch parameter from config, fall back to module default."""
    return INDICATOR_PARAMS.get(key, _DEFAULTS[key])


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
    """Simple moving average."""
    return series.rolling(window, min_periods=window).mean()


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """Slope of least-squares linear fit over a rolling window."""
    def _slope(arr):
        if len(arr) < window or np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]

    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True
    )


# ═══════════════════════════════════════════════════════════════
#  1. RS RATIO  (raw + smoothed)
# ═══════════════════════════════════════════════════════════════

def add_rs_ratio(
    df: pd.DataFrame,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS ratio = stock close / benchmark close.
    Normalised to 1.0 at the first valid data point so the
    absolute level is interpretable (>1 = outperforming since start).

    EMA smoothing removes daily noise.
    SMA provides a longer-term trend baseline.

    Outputs: rs_raw, rs_ema, rs_sma
    """
    raw = df["close"] / bench_close

    # Normalise to 1.0 at first valid observation
    first_valid = raw.first_valid_index()
    if first_valid is not None:
        raw = raw / raw.loc[first_valid]

    df["rs_raw"] = raw
    df["rs_ema"] = _ema(raw, _p("rs_ema_span"))
    df["rs_sma"] = _sma(raw, _p("rs_sma_span"))
    return df


# ═══════════════════════════════════════════════════════════════
#  2. RS SLOPE — the rotation signal
# ═══════════════════════════════════════════════════════════════

def add_rs_slope(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rolling linear-regression slope of the smoothed RS ratio.

    This IS the rotation signal:
      slope > 0 → outperforming benchmark → money rotating in
      slope < 0 → underperforming → money rotating out
      slope flips neg→pos → early rotation detected

    Outputs: rs_slope
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    df["rs_slope"] = _rolling_slope(df["rs_ema"], _p("rs_slope_window"))
    return df


# ═══════════════════════════════════════════════════════════════
#  3. RS Z-SCORE — cross-ticker comparison
# ═══════════════════════════════════════════════════════════════

def add_rs_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z-score of RS slope over a longer lookback.

    Allows apples-to-apples comparison across tickers:
      z > +1.5  →  strong relative outperformance
      z < -1.5  →  strong relative underperformance
      z flips from -1 to +1  →  meaningful regime change

    Outputs: rs_zscore
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    w         = _p("rs_zscore_window")
    roll_mean = df["rs_slope"].rolling(w).mean()
    roll_std  = df["rs_slope"].rolling(w).std()

    df["rs_zscore"] = (
        (df["rs_slope"] - roll_mean) / roll_std.replace(0.0, np.nan)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  4. RS MOMENTUM — acceleration of rotation
# ═══════════════════════════════════════════════════════════════

def add_rs_momentum(df: pd.DataFrame) -> pd.DataFrame:
    """
    Short-term RS slope minus long-term RS slope.

    Positive momentum = RS improvement is accelerating.
    This catches early rotation before the slope itself turns
    positive — like a MACD for relative strength.

    Outputs: rs_momentum
    """
    if "rs_ema" not in df.columns:
        raise ValueError("Run add_rs_ratio first — rs_ema column missing")

    slope_short = _rolling_slope(df["rs_ema"], _p("rs_momentum_short"))
    slope_long  = _rolling_slope(df["rs_ema"], _p("rs_momentum_long"))

    df["rs_momentum"] = slope_short - slope_long
    return df


# ═══════════════════════════════════════════════════════════════
#  5. VOLUME-CONFIRMED RS
# ═══════════════════════════════════════════════════════════════

def add_rs_volume_confirmation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks whether improving RS is backed by above-average volume.

    Smart-money rotation shows up as RS improvement on elevated
    volume.  Without volume confirmation, RS improvement could be
    low-liquidity drift — a trap.

    If indicators.py has already run, reuses relative_volume.
    Otherwise computes it here from raw volume.

    Outputs: rs_rel_volume, rs_vol_confirmed
    """
    if "rs_slope" not in df.columns:
        raise ValueError("Run add_rs_slope first — rs_slope column missing")

    threshold = _p("rs_vol_confirm_threshold")

    # Reuse relative_volume from indicators.py if available
    if "relative_volume" in df.columns:
        df["rs_rel_volume"] = df["relative_volume"]
    else:
        vol_avg = _sma(df["volume"], _p("volume_avg_window"))
        df["rs_rel_volume"] = df["volume"] / vol_avg.replace(0.0, np.nan)

    df["rs_vol_confirmed"] = (
        (df["rs_slope"] > 0) & (df["rs_rel_volume"] > threshold)
    )
    return df


# ═══════════════════════════════════════════════════════════════
#  6. RS REGIME — categorical label
# ═══════════════════════════════════════════════════════════════

def add_rs_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorical label based on two dimensions:
      • RS trend  : rs_ema vs rs_sma  (above = uptrend)
      • RS direction: rs_slope sign   (positive = improving)

    Four regimes:
      ┌────────────┬──────────────────┬──────────────────┐
      │            │  slope > 0       │  slope ≤ 0       │
      ├────────────┼──────────────────┼──────────────────┤
      │ EMA > SMA  │  LEADING         │  WEAKENING       │
      │ EMA ≤ SMA  │  IMPROVING  ★    │  LAGGING         │
      └────────────┴──────────────────┴──────────────────┘

      ★ "improving" is the sweet spot for entry — smart money
        is rotating in before the RS line crosses above its
        trend.  This is where the edge lives.

    Outputs: rs_regime
    """
    for col in ("rs_ema", "rs_sma", "rs_slope"):
        if col not in df.columns:
            raise ValueError(
                f"Run prerequisite functions first — {col} missing"
            )

    above_sma = df["rs_ema"] > df["rs_sma"]
    slope_pos = df["rs_slope"] > 0

    conditions = [
        above_sma & slope_pos,     # leading
        above_sma & ~slope_pos,    # weakening
        ~above_sma & ~slope_pos,   # lagging
        ~above_sma & slope_pos,    # improving
    ]
    labels = ["leading", "weakening", "lagging", "improving"]

    df["rs_regime"] = np.select(conditions, labels, default="unknown")
    return df


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rs(
    stock_df: pd.DataFrame,
    bench_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute all relative-strength metrics for a stock vs its benchmark.

    Parameters
    ----------
    stock_df : pd.DataFrame
        Stock OHLCV, date-indexed, sorted ascending.
        Must have: open, high, low, close, volume.
        May already have indicator columns from indicators.py.

    bench_df : pd.DataFrame
        Benchmark OHLCV (SPY / QQQ / IWM), same format.
        Which benchmark to use per ticker is a pipeline decision,
        not a concern of this module.

    Returns
    -------
    pd.DataFrame
        stock_df (date-aligned) with columns appended:
        rs_raw, rs_ema, rs_sma, rs_slope, rs_zscore,
        rs_momentum, rs_rel_volume, rs_vol_confirmed, rs_regime

    Raises
    ------
    ValueError
        If inputs are empty, missing 'close', or have fewer
        than 30 overlapping dates.
    """
    # ── Validate ────────────────────────────────────────────
    for name, d in [("stock", stock_df), ("bench", bench_df)]:
        if "close" not in d.columns:
            raise ValueError(f"{name}_df missing 'close' column")
        if d.empty:
            raise ValueError(f"{name}_df is empty")

    # ── Align on common dates ───────────────────────────────
    common = stock_df.index.intersection(bench_df.index)
    if len(common) < 30:
        raise ValueError(
            f"Only {len(common)} overlapping dates — need at least 30"
        )

    df          = stock_df.loc[common].copy()
    bench_close = bench_df.loc[common, "close"]

    # ── Compute in dependency order ─────────────────────────
    df = add_rs_ratio(df, bench_close)       # rs_raw, rs_ema, rs_sma
    df = add_rs_slope(df)                    # rs_slope
    df = add_rs_zscore(df)                   # rs_zscore
    df = add_rs_momentum(df)                 # rs_momentum
    df = add_rs_volume_confirmation(df)      # rs_rel_volume, rs_vol_confirmed
    df = add_rs_regime(df)                   # rs_regime

    return df

#############################################

"""
compute/scoring.py
-------------------
Five-pillar composite scoring engine.

Takes a DataFrame that already has indicator columns (from indicators.py)
and relative-strength columns (from relative_strength.py), plus an
optional breadth score series (from breadth.py), and produces sub-scores
plus a weighted composite score per row (ticker-day).

Pillar 1 — Rotation       : Is smart money rotating into this name?
Pillar 2 — Momentum       : Is price action confirming the rotation?
Pillar 3 — Volatility     : Is risk / reward favorable for entry?
Pillar 4 — Microstructure : Is institutional volume backing the move?
Pillar 5 — Breadth        : Is the broad market confirming the move?

Each pillar returns a Series in [0, 1].
Weighted average → composite in [0, 1].
All weights live in common/config.py → SCORING_WEIGHTS.

When breadth data is unavailable the engine falls back to a four-pillar
mode, renormalising the remaining weights so they still sum to 1.0.

This module does NOT make trade decisions — it ranks.
The strategy layer downstream decides what score threshold triggers action.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SCORING_WEIGHTS, SCORING_PARAMS


# ═══════════════════════════════════════════════════════════════
#  COLUMN NAME MAP
#  Indicators.py outputs specific suffixed names.
#  Map logical names → actual column names so the scoring
#  engine stays decoupled from naming conventions.
# ═══════════════════════════════════════════════════════════════

COL = {
    # Pillar 1 — Rotation (from relative_strength.py)
    "rs_zscore":        "rs_zscore",
    "rs_regime":        "rs_regime",
    "rs_momentum":      "rs_momentum",
    "rs_vol_confirmed": "rs_vol_confirmed",

    # Pillar 2 — Momentum
    "rsi":              "rsi_14",
    "macd_hist":        "macd_hist",
    "adx":              "adx_14",

    # Pillar 3 — Volatility / Risk
    "realized_vol":     "realized_vol_20d",
    "atr":              "atr_14",
    "close":            "close",
    "amihud":           "amihud_20d",

    # Pillar 4 — Microstructure
    "obv":              "obv",
    "obv_slope":        "obv_slope_10d",
    "ad_line":          "ad_line",
    "ad_slope":         "ad_line_slope_10d",
    "relative_volume":  "relative_volume",
}


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSORS
# ═══════════════════════════════════════════════════════════════

def _w(key: str) -> float:
    """Fetch weight from SCORING_WEIGHTS."""
    return SCORING_WEIGHTS[key]


def _sp(key: str):
    """Fetch parameter from SCORING_PARAMS."""
    return SCORING_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _sigmoid(x):
    """Map real values → (0, 1).  Vectorised, clipped to avoid overflow."""
    return 1.0 / (1.0 + np.exp(-np.clip(x, -10, 10)))


def _rolling_pctrank(series: pd.Series, window: int) -> pd.Series:
    """
    Rolling percentile rank in [0, 1].

    0.80 means today's value exceeds 80 % of values in the lookback.
    Self-normalising — no assumptions about scale or distribution.
    """
    def _pct(arr):
        if np.isnan(arr[-1]):
            return np.nan
        return np.sum(arr <= arr[-1]) / len(arr)

    return series.rolling(window, min_periods=window // 2).apply(
        _pct, raw=True
    )


def _col(key: str) -> str:
    """Resolve logical column name → actual DataFrame column name."""
    return COL[key]


# ═══════════════════════════════════════════════════════════════
#  PILLAR 1 — ROTATION
#  "Is smart money rotating into this stock vs benchmark?"
# ═══════════════════════════════════════════════════════════════

def _pillar_rotation(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    rs_zscore        Standardised RS slope → sigmoid.
                     z = 0 → 0.50,  z = +2 → 0.88,  z = -2 → 0.12.

    rs_regime        Categorical phase map:
                     leading 1.0 | improving 0.75 | weakening 0.25
                     lagging 0.0 | unknown   0.50

    rs_momentum      Acceleration of rotation → sigmoid.
                     Positive = RS improvement is speeding up.

    rs_vol_confirmed Binary: is above-average volume backing the move?
    """
    # ── rs_zscore ────────────────────────────────────────────
    zs = pd.Series(
        _sigmoid(df[_col("rs_zscore")].fillna(0).values),
        index=df.index,
    )

    # ── rs_regime ────────────────────────────────────────────
    regime_map = {
        "leading":   1.00,
        "improving": 0.75,
        "weakening": 0.25,
        "lagging":   0.00,
        "unknown":   0.50,
    }
    regime = df[_col("rs_regime")].map(regime_map).fillna(0.5)

    # ── rs_momentum ──────────────────────────────────────────
    mom = pd.Series(
        _sigmoid(
            df[_col("rs_momentum")].fillna(0).values
            * _sp("rs_momentum_scale")
        ),
        index=df.index,
    )

    # ── rs_vol_confirmed ─────────────────────────────────────
    vol_conf = df[_col("rs_vol_confirmed")].astype(float).fillna(0.0)

    return (
        _w("rs_zscore_w")       * zs
        + _w("rs_regime_w")     * regime
        + _w("rs_momentum_w")   * mom
        + _w("rs_vol_confirm_w") * vol_conf
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 2 — MOMENTUM
#  "Is price action confirming the rotation?"
# ═══════════════════════════════════════════════════════════════

def _pillar_momentum(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    RSI   Piecewise linear.  Sweet spot 40-70 (trending, not overbought).
          25-40 ramps up, 70-80 ramps down, extremes score 0.10.

    MACD  Histogram rolling percentile rank.
          Top of range = strong bullish momentum.

    ADX   Piecewise linear.  >25 = trending = good for momentum.
          <15 = choppy = low score.
    """
    # ── RSI ──────────────────────────────────────────────────
    rsi_score = pd.Series(
        np.interp(
            df[_col("rsi")].fillna(50).values,
            [0, 25, 40, 55, 70, 80, 100],
            [0.10, 0.10, 0.60, 1.00, 1.00, 0.50, 0.10],
        ),
        index=df.index,
    )

    # ── MACD histogram ───────────────────────────────────────
    macd_score = _rolling_pctrank(
        df[_col("macd_hist")].fillna(0.0), _sp("rank_window")
    ).fillna(0.5)

    # ── ADX ──────────────────────────────────────────────────
    adx_score = pd.Series(
        np.interp(
            df[_col("adx")].fillna(15).values,
            [0, 10, 15, 25, 40, 60],
            [0.10, 0.20, 0.40, 0.85, 1.00, 1.00],
        ),
        index=df.index,
    )

    return (
        _w("rsi_w")   * rsi_score
        + _w("macd_w") * macd_score
        + _w("adx_w")  * adx_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 3 — VOLATILITY / RISK
#  "Is risk / reward favorable for entry?"
# ═══════════════════════════════════════════════════════════════

def _pillar_volatility(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    realized_vol  Rolling percentile → tent function.
                  Moderate vol (near median) scores highest.
                  Extremes (dead money OR panic) score low.

    atr_pct       ATR ÷ close, same tent logic.
                  Captures intraday range risk vs close-to-close risk.

    amihud        Illiquidity.  Inverted rank — lower is better.
                  Liquid names get capital first.
    """
    rw = _sp("rank_window")

    # ── Realized vol → tent ──────────────────────────────────
    vol_rank  = _rolling_pctrank(
        df[_col("realized_vol")].ffill(), rw
    ).fillna(0.5)
    vol_score = 1.0 - 2.0 * (vol_rank - 0.5).abs()

    # ── ATR percent → tent ───────────────────────────────────
    atr_pct   = df[_col("atr")] / df[_col("close")].replace(0, np.nan)
    atr_rank  = _rolling_pctrank(atr_pct.ffill(), rw).fillna(0.5)
    atr_score = 1.0 - 2.0 * (atr_rank - 0.5).abs()

    # ── Amihud → inverted rank ───────────────────────────────
    amihud_rank  = _rolling_pctrank(
        df[_col("amihud")].ffill(), rw
    ).fillna(0.5)
    amihud_score = 1.0 - amihud_rank

    return (
        _w("realized_vol_w") * vol_score
        + _w("atr_pct_w")   * atr_score
        + _w("amihud_w")    * amihud_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 4 — MICROSTRUCTURE
#  "Is institutional volume backing the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_microstructure(df: pd.DataFrame) -> pd.Series:
    """
    Sub-components
    ──────────────
    OBV slope    Pre-computed rolling slope → percentile rank.
                 Rising OBV = accumulation (buying pressure).

    A/D slope    Pre-computed rolling slope → percentile rank.
                 Weighted by intra-bar position — more granular than OBV.

    Rel volume   Piecewise linear.  1.5-2.5× average = institutional
                 interest.  >4× could be panic or event-driven.
    """
    rw = _sp("rank_window")

    # ── OBV slope (pre-computed by indicators.py) ────────────
    obv_score = _rolling_pctrank(
        df[_col("obv_slope")].ffill(), rw
    ).fillna(0.5)

    # ── A/D line slope (pre-computed by indicators.py) ───────
    ad_score = _rolling_pctrank(
        df[_col("ad_slope")].ffill(), rw
    ).fillna(0.5)

    # ── Relative volume ──────────────────────────────────────
    rvol_score = pd.Series(
        np.interp(
            df[_col("relative_volume")].fillna(1.0).values,
            [0.0, 0.5, 1.0, 1.5, 2.5, 4.0, 8.0],
            [0.10, 0.20, 0.50, 0.85, 1.00, 0.80, 0.50],
        ),
        index=df.index,
    )

    return (
        _w("obv_slope_w")   * obv_score
        + _w("ad_slope_w")  * ad_score
        + _w("rel_volume_w") * rvol_score
    )


# ═══════════════════════════════════════════════════════════════
#  PILLAR 5 — BREADTH
#  "Is the broad market confirming the move?"
# ═══════════════════════════════════════════════════════════════

def _pillar_breadth(
    df: pd.DataFrame,
    breadth_scores: pd.Series,
) -> pd.Series:
    """
    Market breadth overlay.

    The breadth pillar is unique: it is a universe-level signal
    (the same value for every symbol on a given day), not a
    per-symbol indicator.  It acts as a tide gauge — when broad
    participation is strong, all boats get a lift; when breadth
    deteriorates, conviction is dampened across the board.

    Parameters
    ----------
    df : pd.DataFrame
        The per-symbol indicator DataFrame (used only for its index).
    breadth_scores : pd.Series
        Daily breadth scores on a 0–100 scale, as produced by
        ``breadth_to_pillar_scores()`` for this symbol's column.

    Returns
    -------
    pd.Series
        Values in [0, 1] aligned to df's index.
    """
    # Align to the symbol's date index, forward-fill gaps
    # (breadth may have fewer rows if the constituent universe
    # started trading later than this symbol)
    aligned = breadth_scores.reindex(df.index).ffill().fillna(50.0)

    # Rescale 0–100 → 0–1
    return (aligned / 100.0).clip(0, 1)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_composite_score(
    df: pd.DataFrame,
    breadth_scores: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute all pillar sub-scores and the weighted composite.

    Parameters
    ----------
    df : pd.DataFrame
        Date-indexed OHLCV with indicator columns (from
        compute_all_indicators) **and** RS columns (from
        compute_all_rs) already present.
    breadth_scores : pd.Series or None
        Daily breadth score for this symbol (0–100 scale),
        typically one column from the output of
        ``breadth_to_pillar_scores()``.  If None, the engine
        falls back to four-pillar mode with renormalised weights.

    Returns
    -------
    pd.DataFrame
        Input frame with columns appended:
          score_rotation       [0-1]   Pillar 1
          score_momentum       [0-1]   Pillar 2
          score_volatility     [0-1]   Pillar 3
          score_microstructure [0-1]   Pillar 4
          score_breadth        [0-1]   Pillar 5 (if breadth provided)
          score_composite      [0-1]   Weighted average
          score_percentile     [0-1]   Time-series pct rank of composite
          breadth_available    bool    Whether breadth was used

    Raises
    ------
    ValueError
        Missing columns needed by pillars 1–4.
    """
    # ── Validate required columns (pillars 1–4) ──────────────
    required = {
        "pillar_rotation":       [
            _col("rs_zscore"), _col("rs_regime"),
            _col("rs_momentum"), _col("rs_vol_confirmed"),
        ],
        "pillar_momentum":       [
            _col("rsi"), _col("macd_hist"), _col("adx"),
        ],
        "pillar_volatility":     [
            _col("realized_vol"), _col("atr"),
            _col("close"), _col("amihud"),
        ],
        "pillar_microstructure": [
            _col("obv_slope"), _col("ad_slope"),
            _col("relative_volume"),
        ],
    }
    missing = []
    for pillar, cols in required.items():
        for c in cols:
            if c not in df.columns:
                missing.append(f"{c} (needed by {pillar})")
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # ── Determine mode: 5-pillar or 4-pillar fallback ────────
    use_breadth = (
        breadth_scores is not None
        and not breadth_scores.empty
        and "pillar_breadth" in SCORING_WEIGHTS
    )

    pillar_keys = [
        "pillar_rotation", "pillar_momentum",
        "pillar_volatility", "pillar_microstructure",
    ]
    if use_breadth:
        pillar_keys.append("pillar_breadth")

    raw_weights = {k: _w(k) for k in pillar_keys}
    total_w = sum(raw_weights.values())

    # Renormalise so active pillars sum to exactly 1.0
    if total_w <= 0:
        raise ValueError("Pillar weights sum to zero.")
    weights = {k: v / total_w for k, v in raw_weights.items()}

    # ── Compute pillar scores ─────────────────────────────────
    out = df.copy()

    out["score_rotation"]       = _pillar_rotation(out).clip(0, 1)
    out["score_momentum"]       = _pillar_momentum(out).clip(0, 1)
    out["score_volatility"]     = _pillar_volatility(out).clip(0, 1)
    out["score_microstructure"] = _pillar_microstructure(out).clip(0, 1)

    if use_breadth:
        out["score_breadth"] = _pillar_breadth(out, breadth_scores).clip(0, 1)

    # ── Composite ─────────────────────────────────────────────
    composite = (
        weights["pillar_rotation"]       * out["score_rotation"]
        + weights["pillar_momentum"]     * out["score_momentum"]
        + weights["pillar_volatility"]   * out["score_volatility"]
        + weights["pillar_microstructure"] * out["score_microstructure"]
    )

    if use_breadth:
        composite += weights["pillar_breadth"] * out["score_breadth"]

    out["score_composite"] = composite.clip(0, 1)

    # ── Time-series percentile ────────────────────────────────
    out["score_percentile"] = _rolling_pctrank(
        out["score_composite"], 252
    )

    # ── Metadata ──────────────────────────────────────────────
    out["breadth_available"] = use_breadth

    return out

#########################################################

"""
compute/sector_rs.py
--------------------
Sector-level relative-strength analysis.

Answers: "Which sectors are leading the market rotation?"

Pipeline
────────
  1. Fetch OHLCV for 11 GICS sector ETFs + benchmark (SPY).
  2. Compute RS ratio / slope / z-score / regime per sector
     (same math as stock-level RS in relative_strength.py).
  3. Cross-sectionally rank sectors each day — who's strongest?
  4. Derive a tailwind / headwind value per sector for the
     composite-score adjustment.
  5. Merge sector context into individual stock DataFrames.

All tuneable parameters live in common/config.py.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from common.config import (
    BENCHMARK_TICKER,
    SECTOR_ETFS,
    SECTOR_RS_PARAMS,
    SECTOR_SCORE_ADJUSTMENT,
)


# ═══════════════════════════════════════════════════════════════
#  yfinance sector label → our SECTOR_ETFS key
# ═══════════════════════════════════════════════════════════════

_YF_SECTOR_MAP: dict[str, str] = {
    "Technology":             "Technology",
    "Healthcare":             "Healthcare",
    "Financial Services":     "Financials",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Communication Services": "Communication",
}

# In-memory cache so we only hit yfinance once per ticker
_sector_cache: dict[str, str | None] = {}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance download to lower-case flat columns."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """OLS slope over a rolling window."""
    def _slope(arr):
        if np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]
    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True,
    )


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_sector_data(
    period: str = "2y",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download OHLCV for every sector ETF **and** the benchmark.

    Parameters
    ----------
    period : str
        yfinance period string ("2y", "5y", "max" …).

    Returns
    -------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV DataFrame} for each sector.
    benchmark_df : pd.DataFrame
        OHLCV for the benchmark (SPY by default).
    """
    import yfinance as yf

    sector_data: dict[str, pd.DataFrame] = {}
    for name, etf in SECTOR_ETFS.items():
        raw = yf.download(etf, period=period, progress=False)
        if raw.empty:
            warnings.warn(f"No data for {etf} ({name}), skipping.")
            continue
        sector_data[name] = _clean_download(raw)

    bench_raw = yf.download(BENCHMARK_TICKER, period=period, progress=False)
    if bench_raw.empty:
        raise ValueError(f"Benchmark {BENCHMARK_TICKER} returned no data.")
    benchmark_df = _clean_download(bench_raw)

    return sector_data, benchmark_df


# ═══════════════════════════════════════════════════════════════
#  SINGLE-SECTOR RS COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_single_sector_rs(
    sector_close: pd.Series,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS metrics for **one** sector ETF vs the benchmark.

    Same mathematics as relative_strength.py:
      ratio → slope → z-score → regime.

    Returns DataFrame with columns prefixed ``sect_rs_``.
    """
    p = SECTOR_RS_PARAMS

    # ── RS ratio ─────────────────────────────────────────────
    ratio = sector_close / bench_close

    # ── RS slope (direction of relative performance) ─────────
    slope = _rolling_slope(ratio, p["slope_window"])

    # ── Z-score of slope ─────────────────────────────────────
    s_mean = slope.rolling(p["zscore_window"], min_periods=20).mean()
    s_std  = slope.rolling(p["zscore_window"], min_periods=20).std()
    zscore = (slope - s_mean) / s_std.replace(0, np.nan)

    # ── Momentum (acceleration of rotation) ──────────────────
    momentum = slope.diff(p["momentum_window"])

    # ── Regime ───────────────────────────────────────────────
    conditions = [
        (zscore > 0) & (momentum > 0),
        (zscore <= 0) & (momentum > 0),
        (zscore > 0)  & (momentum <= 0),
        (zscore <= 0) & (momentum <= 0),
    ]
    choices = ["leading", "improving", "weakening", "lagging"]
    regime = pd.Series(
        np.select(conditions, choices, default="unknown"),
        index=ratio.index,
    )

    return pd.DataFrame({
        "sect_rs_ratio":    ratio,
        "sect_rs_slope":    slope,
        "sect_rs_zscore":   zscore,
        "sect_rs_momentum": momentum,
        "sect_rs_regime":   regime,
    }, index=ratio.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER COMPUTATION — ALL SECTORS + CROSS-SECTIONAL RANKS
# ═══════════════════════════════════════════════════════════════

def compute_all_sector_rs(
    sector_data: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute RS for every sector and add cross-sectional rankings.

    Parameters
    ----------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV} from :func:`fetch_sector_data`.
    benchmark_df : pd.DataFrame
        Benchmark OHLCV.

    Returns
    -------
    pd.DataFrame
        **MultiIndex (date, sector)** with columns:

        ============== ===============================================
        sect_rs_ratio  raw price ratio (sector ETF / benchmark)
        sect_rs_slope  regression slope of the ratio
        sect_rs_zscore standardised slope
        sect_rs_momentum acceleration of the slope
        sect_rs_regime leading / improving / weakening / lagging
        sect_rs_rank   integer rank, 1 = strongest (NaN during warmup)
        sect_rs_pctrank percentile rank 0–1, 1.0 = strongest (smoothed)
        sector_tailwind score adjustment value
        etf            sector ETF ticker symbol
        ============== ===============================================
    """
    frames: list[pd.DataFrame] = []

    for sector_name, sector_df in sector_data.items():
        common = sector_df.index.intersection(benchmark_df.index)
        if len(common) < 60:
            warnings.warn(
                f"{sector_name}: only {len(common)} overlapping dates, "
                f"need ≥60.  Skipping."
            )
            continue

        metrics = _compute_single_sector_rs(
            sector_df.loc[common, "close"],
            benchmark_df.loc[common, "close"],
        )
        metrics["sector"] = sector_name
        metrics["etf"]    = SECTOR_ETFS[sector_name]
        metrics.index.name = "date"
        frames.append(metrics)

    if not frames:
        raise ValueError("No sectors produced valid RS data.")

    combined = pd.concat(frames)

    # ── Cross-sectional rank per date ─────────────────────────
    #    rank 1 = highest z-score = strongest sector
    #    NaN zscore rows → NaN rank (warmup period)
    combined["sect_rs_rank"] = (
        combined
        .groupby(level=0)["sect_rs_zscore"]
        .rank(ascending=False, method="min", na_option="keep")
    )

    # ── Percentile rank (0–1, 1.0 = strongest) ───────────────
    #    Count only non-NaN ranks per date
    n_ranked = (
        combined
        .groupby(level=0)["sect_rs_rank"]
        .transform(lambda s: s.notna().sum())
    )
    combined["sect_rs_pctrank"] = np.where(
        combined["sect_rs_rank"].isna() | (n_ranked <= 1),
        np.nan,
        1.0 - (combined["sect_rs_rank"] - 1) / (n_ranked - 1),
    )

    # ── Smooth the percentile rank over time ──────────────────
    smooth_w = SECTOR_RS_PARAMS.get("rank_smoothing", 5)
    if smooth_w > 1:
        combined["sect_rs_pctrank"] = (
            combined
            .groupby("sector")["sect_rs_pctrank"]
            .transform(lambda s: s.rolling(smooth_w, min_periods=1).mean())
        )

    # ── Sector tailwind / headwind ────────────────────────────
    #    pctrank 1.0 → max_boost,  0.0 → max_penalty,  0.5 → 0
    adj = SECTOR_SCORE_ADJUSTMENT
    if adj["enabled"]:
        combined["sector_tailwind"] = np.where(
            combined["sect_rs_pctrank"].isna(),
            0.0,
            adj["max_penalty"]
            + (adj["max_boost"] - adj["max_penalty"])
            * combined["sect_rs_pctrank"],
        )
    else:
        combined["sector_tailwind"] = 0.0

    # ── Set MultiIndex (date, sector) ─────────────────────────
    combined = (
        combined
        .reset_index()
        .set_index(["date", "sector"])
        .sort_index()
    )

    return combined


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT — one-day cross-sectional view (for dashboards)
# ═══════════════════════════════════════════════════════════════

def sector_snapshot(
    sector_rs_df: pd.DataFrame,
    date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sector rankings for a single date, sorted strongest → weakest.

    Parameters
    ----------
    sector_rs_df : pd.DataFrame
        Output of :func:`compute_all_sector_rs` (MultiIndex).
    date : str or Timestamp, optional
        Target date.  Defaults to the latest available date.

    Returns
    -------
    pd.DataFrame  – one row per sector, indexed by sector name.
    """
    dates = sector_rs_df.index.get_level_values("date")

    if date is None:
        target = dates.max()
    else:
        target = pd.Timestamp(date)
        if target not in dates:
            available = dates.unique().sort_values()
            mask = available <= target
            if not mask.any():
                raise ValueError(f"No data on or before {date}")
            target = available[mask][-1]

    snap = sector_rs_df.loc[target].copy()
    return snap.sort_values("sect_rs_rank")


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUP — ticker → sector name
# ═══════════════════════════════════════════════════════════════

def lookup_sector(ticker: str) -> str | None:
    """
    Look up the GICS sector for a stock ticker via yfinance.

    Returns the sector name matching a SECTOR_ETFS key,
    or ``None`` if the lookup fails.

    Results are cached in memory for the session.
    """
    import yfinance as yf

    ticker = ticker.upper()

    if ticker in _sector_cache:
        return _sector_cache[ticker]

    try:
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", None)
        if yf_sector and yf_sector in _YF_SECTOR_MAP:
            result = _YF_SECTOR_MAP[yf_sector]
        else:
            warnings.warn(
                f"Could not map yfinance sector '{yf_sector}' "
                f"for {ticker}."
            )
            result = None
    except Exception as e:
        warnings.warn(f"Sector lookup failed for {ticker}: {e}")
        result = None

    _sector_cache[ticker] = result
    return result


# ═══════════════════════════════════════════════════════════════
#  MERGE — add sector context to an individual stock DataFrame
# ═══════════════════════════════════════════════════════════════

def merge_sector_context(
    stock_df: pd.DataFrame,
    sector_rs_df: pd.DataFrame,
    sector_name: str,
) -> pd.DataFrame:
    """
    Add sector-level columns to an individual stock's DataFrame.

    Columns added
    ─────────────
    sect_rs_zscore     Sector z-score vs benchmark
    sect_rs_regime     Sector regime
    sect_rs_rank       Sector rank (1 = best)
    sect_rs_pctrank    Sector percentile (1.0 = best)
    sector_tailwind    Score adjustment value
    sector_name        Sector label

    If ``score_composite`` already exists, also creates:

    score_adjusted     score_composite + sector_tailwind, clipped [0, 1]

    Parameters
    ----------
    stock_df : pd.DataFrame
        Date-indexed stock data (with or without score columns).
    sector_rs_df : pd.DataFrame
        MultiIndex (date, sector) from :func:`compute_all_sector_rs`.
    sector_name : str
        Must match a key in SECTOR_ETFS.

    Returns
    -------
    pd.DataFrame  – stock_df with sector columns appended.
    """
    available_sectors = (
        sector_rs_df.index
        .get_level_values("sector")
        .unique()
        .tolist()
    )
    if sector_name not in available_sectors:
        raise ValueError(
            f"Sector '{sector_name}' not found.  "
            f"Available: {available_sectors}"
        )

    # Extract this sector, drop the sector level → date-indexed
    sect = sector_rs_df.xs(sector_name, level="sector")

    merge_cols = [
        "sect_rs_zscore", "sect_rs_regime", "sect_rs_rank",
        "sect_rs_pctrank", "sector_tailwind",
    ]
    merge_cols = [c for c in merge_cols if c in sect.columns]

    out = stock_df.copy()
    out = out.join(sect[merge_cols], how="left")
    out["sector_name"] = sector_name

    # ── Adjusted composite score ─────────────────────────────
    if "score_composite" in out.columns and "sector_tailwind" in out.columns:
        out["score_adjusted"] = (
            out["score_composite"]
            + out["sector_tailwind"].fillna(0)
        ).clip(0, 1)

    return out

#####################################
"""
OUTPUT :
------------
"""
"""
compute/sector_rs.py
--------------------
Sector-level relative-strength analysis.

Answers: "Which sectors are leading the market rotation?"

Pipeline
────────
  1. Fetch OHLCV for 11 GICS sector ETFs + benchmark (SPY).
  2. Compute RS ratio / slope / z-score / regime per sector
     (same math as stock-level RS in relative_strength.py).
  3. Cross-sectionally rank sectors each day — who's strongest?
  4. Derive a tailwind / headwind value per sector for the
     composite-score adjustment.
  5. Merge sector context into individual stock DataFrames.

All tuneable parameters live in common/config.py.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from common.config import (
    BENCHMARK_TICKER,
    SECTOR_ETFS,
    SECTOR_RS_PARAMS,
    SECTOR_SCORE_ADJUSTMENT,
)


# ═══════════════════════════════════════════════════════════════
#  yfinance sector label → our SECTOR_ETFS key
# ═══════════════════════════════════════════════════════════════

_YF_SECTOR_MAP: dict[str, str] = {
    "Technology":             "Technology",
    "Healthcare":             "Healthcare",
    "Financial Services":     "Financials",
    "Consumer Cyclical":      "Consumer Disc",
    "Consumer Defensive":     "Consumer Staples",
    "Energy":                 "Energy",
    "Industrials":            "Industrials",
    "Basic Materials":        "Materials",
    "Utilities":              "Utilities",
    "Real Estate":            "Real Estate",
    "Communication Services": "Communication",
}

# In-memory cache so we only hit yfinance once per ticker
_sector_cache: dict[str, str | None] = {}


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def _clean_download(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance download to lower-case flat columns."""
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """OLS slope over a rolling window."""
    def _slope(arr):
        if np.isnan(arr).any():
            return np.nan
        x = np.arange(len(arr))
        return np.polyfit(x, arr, 1)[0]
    return series.rolling(window, min_periods=window).apply(
        _slope, raw=True,
    )


# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_sector_data(
    period: str = "2y",
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Download OHLCV for every sector ETF **and** the benchmark.

    Parameters
    ----------
    period : str
        yfinance period string ("2y", "5y", "max" …).

    Returns
    -------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV DataFrame} for each sector.
    benchmark_df : pd.DataFrame
        OHLCV for the benchmark (SPY by default).
    """
    import yfinance as yf

    sector_data: dict[str, pd.DataFrame] = {}
    for name, etf in SECTOR_ETFS.items():
        raw = yf.download(etf, period=period, progress=False)
        if raw.empty:
            warnings.warn(f"No data for {etf} ({name}), skipping.")
            continue
        sector_data[name] = _clean_download(raw)

    bench_raw = yf.download(BENCHMARK_TICKER, period=period, progress=False)
    if bench_raw.empty:
        raise ValueError(f"Benchmark {BENCHMARK_TICKER} returned no data.")
    benchmark_df = _clean_download(bench_raw)

    return sector_data, benchmark_df


# ═══════════════════════════════════════════════════════════════
#  SINGLE-SECTOR RS COMPUTATION
# ═══════════════════════════════════════════════════════════════

def _compute_single_sector_rs(
    sector_close: pd.Series,
    bench_close: pd.Series,
) -> pd.DataFrame:
    """
    RS metrics for **one** sector ETF vs the benchmark.

    Same mathematics as relative_strength.py:
      ratio → slope → z-score → regime.

    Returns DataFrame with columns prefixed ``sect_rs_``.
    """
    p = SECTOR_RS_PARAMS

    # ── RS ratio ─────────────────────────────────────────────
    ratio = sector_close / bench_close

    # ── RS slope (direction of relative performance) ─────────
    slope = _rolling_slope(ratio, p["slope_window"])

    # ── Z-score of slope ─────────────────────────────────────
    s_mean = slope.rolling(p["zscore_window"], min_periods=20).mean()
    s_std  = slope.rolling(p["zscore_window"], min_periods=20).std()
    zscore = (slope - s_mean) / s_std.replace(0, np.nan)

    # ── Momentum (acceleration of rotation) ──────────────────
    momentum = slope.diff(p["momentum_window"])

    # ── Regime ───────────────────────────────────────────────
    conditions = [
        (zscore > 0) & (momentum > 0),
        (zscore <= 0) & (momentum > 0),
        (zscore > 0)  & (momentum <= 0),
        (zscore <= 0) & (momentum <= 0),
    ]
    choices = ["leading", "improving", "weakening", "lagging"]
    regime = pd.Series(
        np.select(conditions, choices, default="unknown"),
        index=ratio.index,
    )

    return pd.DataFrame({
        "sect_rs_ratio":    ratio,
        "sect_rs_slope":    slope,
        "sect_rs_zscore":   zscore,
        "sect_rs_momentum": momentum,
        "sect_rs_regime":   regime,
    }, index=ratio.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER COMPUTATION — ALL SECTORS + CROSS-SECTIONAL RANKS
# ═══════════════════════════════════════════════════════════════

def compute_all_sector_rs(
    sector_data: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute RS for every sector and add cross-sectional rankings.

    Parameters
    ----------
    sector_data : dict[str, pd.DataFrame]
        {sector_name: OHLCV} from :func:`fetch_sector_data`.
    benchmark_df : pd.DataFrame
        Benchmark OHLCV.

    Returns
    -------
    pd.DataFrame
        **MultiIndex (date, sector)** with columns:

        ============== ===============================================
        sect_rs_ratio  raw price ratio (sector ETF / benchmark)
        sect_rs_slope  regression slope of the ratio
        sect_rs_zscore standardised slope
        sect_rs_momentum acceleration of the slope
        sect_rs_regime leading / improving / weakening / lagging
        sect_rs_rank   integer rank, 1 = strongest (NaN during warmup)
        sect_rs_pctrank percentile rank 0–1, 1.0 = strongest (smoothed)
        sector_tailwind score adjustment value
        etf            sector ETF ticker symbol
        ============== ===============================================
    """
    frames: list[pd.DataFrame] = []

    for sector_name, sector_df in sector_data.items():
        common = sector_df.index.intersection(benchmark_df.index)
        if len(common) < 60:
            warnings.warn(
                f"{sector_name}: only {len(common)} overlapping dates, "
                f"need ≥60.  Skipping."
            )
            continue

        metrics = _compute_single_sector_rs(
            sector_df.loc[common, "close"],
            benchmark_df.loc[common, "close"],
        )
        metrics["sector"] = sector_name
        metrics["etf"]    = SECTOR_ETFS[sector_name]
        metrics.index.name = "date"
        frames.append(metrics)

    if not frames:
        raise ValueError("No sectors produced valid RS data.")

    combined = pd.concat(frames)

    # ── Cross-sectional rank per date ─────────────────────────
    #    rank 1 = highest z-score = strongest sector
    #    NaN zscore rows → NaN rank (warmup period)
    combined["sect_rs_rank"] = (
        combined
        .groupby(level=0)["sect_rs_zscore"]
        .rank(ascending=False, method="min", na_option="keep")
    )

    # ── Percentile rank (0–1, 1.0 = strongest) ───────────────
    #    Count only non-NaN ranks per date
    n_ranked = (
        combined
        .groupby(level=0)["sect_rs_rank"]
        .transform(lambda s: s.notna().sum())
    )
    combined["sect_rs_pctrank"] = np.where(
        combined["sect_rs_rank"].isna() | (n_ranked <= 1),
        np.nan,
        1.0 - (combined["sect_rs_rank"] - 1) / (n_ranked - 1),
    )

    # ── Smooth the percentile rank over time ──────────────────
    smooth_w = SECTOR_RS_PARAMS.get("rank_smoothing", 5)
    if smooth_w > 1:
        combined["sect_rs_pctrank"] = (
            combined
            .groupby("sector")["sect_rs_pctrank"]
            .transform(lambda s: s.rolling(smooth_w, min_periods=1).mean())
        )

    # ── Sector tailwind / headwind ────────────────────────────
    #    pctrank 1.0 → max_boost,  0.0 → max_penalty,  0.5 → 0
    adj = SECTOR_SCORE_ADJUSTMENT
    if adj["enabled"]:
        combined["sector_tailwind"] = np.where(
            combined["sect_rs_pctrank"].isna(),
            0.0,
            adj["max_penalty"]
            + (adj["max_boost"] - adj["max_penalty"])
            * combined["sect_rs_pctrank"],
        )
    else:
        combined["sector_tailwind"] = 0.0

    # ── Set MultiIndex (date, sector) ─────────────────────────
    combined = (
        combined
        .reset_index()
        .set_index(["date", "sector"])
        .sort_index()
    )

    return combined


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT — one-day cross-sectional view (for dashboards)
# ═══════════════════════════════════════════════════════════════

def sector_snapshot(
    sector_rs_df: pd.DataFrame,
    date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Sector rankings for a single date, sorted strongest → weakest.

    Parameters
    ----------
    sector_rs_df : pd.DataFrame
        Output of :func:`compute_all_sector_rs` (MultiIndex).
    date : str or Timestamp, optional
        Target date.  Defaults to the latest available date.

    Returns
    -------
    pd.DataFrame  – one row per sector, indexed by sector name.
    """
    dates = sector_rs_df.index.get_level_values("date")

    if date is None:
        target = dates.max()
    else:
        target = pd.Timestamp(date)
        if target not in dates:
            available = dates.unique().sort_values()
            mask = available <= target
            if not mask.any():
                raise ValueError(f"No data on or before {date}")
            target = available[mask][-1]

    snap = sector_rs_df.loc[target].copy()
    return snap.sort_values("sect_rs_rank")


# ═══════════════════════════════════════════════════════════════
#  SECTOR LOOKUP — ticker → sector name
# ═══════════════════════════════════════════════════════════════

def lookup_sector(ticker: str) -> str | None:
    """
    Look up the GICS sector for a stock ticker via yfinance.

    Returns the sector name matching a SECTOR_ETFS key,
    or ``None`` if the lookup fails.

    Results are cached in memory for the session.
    """
    import yfinance as yf

    ticker = ticker.upper()

    if ticker in _sector_cache:
        return _sector_cache[ticker]

    try:
        info = yf.Ticker(ticker).info
        yf_sector = info.get("sector", None)
        if yf_sector and yf_sector in _YF_SECTOR_MAP:
            result = _YF_SECTOR_MAP[yf_sector]
        else:
            warnings.warn(
                f"Could not map yfinance sector '{yf_sector}' "
                f"for {ticker}."
            )
            result = None
    except Exception as e:
        warnings.warn(f"Sector lookup failed for {ticker}: {e}")
        result = None

    _sector_cache[ticker] = result
    return result


# ═══════════════════════════════════════════════════════════════
#  MERGE — add sector context to an individual stock DataFrame
# ═══════════════════════════════════════════════════════════════

def merge_sector_context(
    stock_df: pd.DataFrame,
    sector_rs_df: pd.DataFrame,
    sector_name: str,
) -> pd.DataFrame:
    """
    Add sector-level columns to an individual stock's DataFrame.

    Columns added
    ─────────────
    sect_rs_zscore     Sector z-score vs benchmark
    sect_rs_regime     Sector regime
    sect_rs_rank       Sector rank (1 = best)
    sect_rs_pctrank    Sector percentile (1.0 = best)
    sector_tailwind    Score adjustment value
    sector_name        Sector label

    If ``score_composite`` already exists, also creates:

    score_adjusted     score_composite + sector_tailwind, clipped [0, 1]

    Parameters
    ----------
    stock_df : pd.DataFrame
        Date-indexed stock data (with or without score columns).
    sector_rs_df : pd.DataFrame
        MultiIndex (date, sector) from :func:`compute_all_sector_rs`.
    sector_name : str
        Must match a key in SECTOR_ETFS.

    Returns
    -------
    pd.DataFrame  – stock_df with sector columns appended.
    """
    available_sectors = (
        sector_rs_df.index
        .get_level_values("sector")
        .unique()
        .tolist()
    )
    if sector_name not in available_sectors:
        raise ValueError(
            f"Sector '{sector_name}' not found.  "
            f"Available: {available_sectors}"
        )

    # Extract this sector, drop the sector level → date-indexed
    sect = sector_rs_df.xs(sector_name, level="sector")

    merge_cols = [
        "sect_rs_zscore", "sect_rs_regime", "sect_rs_rank",
        "sect_rs_pctrank", "sector_tailwind",
    ]
    merge_cols = [c for c in merge_cols if c in sect.columns]

    out = stock_df.copy()
    out = out.join(sect[merge_cols], how="left")
    out["sector_name"] = sector_name

    # ── Adjusted composite score ─────────────────────────────
    if "score_composite" in out.columns and "sector_tailwind" in out.columns:
        out["score_adjusted"] = (
            out["score_composite"]
            + out["sector_tailwind"].fillna(0)
        ).clip(0, 1)

    return out


########################################################
"""
output/rankings.py
------------------
Daily cross-sectional rankings across the scored ETF / stock universe.

Takes the output of the scoring pipeline — one scored DataFrame per
symbol — and produces ranked tables showing which names have the
strongest composite scores on any given trading day.

This is the bridge between scoring and portfolio: the strategy layer
consumes these rankings to decide what to buy, hold, and sell.

Key Columns Added
─────────────────
  rank              1 = best (highest composite score)
  pct_rank          0–1 percentile within universe
  universe_size     how many symbols are ranked that day
  rank_change       +N = improved N places vs prior day
  pillars_bullish   count of pillar scores > 0.50
  pillar_agreement  fraction of pillars > 0.50  (0–1)
  ret_1d / 5d / 20d recent returns for context

Pipeline
────────
  {ticker: scored_df}
       ↓
  build_rankings_panel()     — stack into MultiIndex panel
       ↓
  rank_universe()            — cross-sectional rank per date
       ↓
  compute_rank_changes()     — day-over-day rank movement
       ↓
  compute_pillar_agreement() — signal agreement across pillars
       ↓
  compute_all_rankings()     — master orchestrator → ranked panel
       ↓
  latest_rankings()          — snapshot for a single date
  filter_top_n()             — top N symbols
  filter_by_regime()         — filter by RS regime
  rank_history()             — single ticker over time
  rankings_summary()         — summary statistics dict
  rankings_report()          — formatted text report
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  COLUMN LISTS
# ═══════════════════════════════════════════════════════════════

_SCORE_COLS = [
    "score_composite",
    "score_adjusted",
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
    "score_percentile",
]

_META_COLS = [
    "rs_regime",
    "rs_zscore",
    "rs_momentum",
    "sect_rs_regime",
    "close",
    "breadth_available",
]

_PILLAR_COLS = [
    "score_rotation",
    "score_momentum",
    "score_volatility",
    "score_microstructure",
    "score_breadth",
]


# ═══════════════════════════════════════════════════════════════
#  PANEL BUILDER
# ═══════════════════════════════════════════════════════════════

def build_rankings_panel(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Stack per-symbol scored DataFrames into a single panel.

    Parameters
    ----------
    scored_universe : dict
        {ticker: DataFrame} where each DataFrame has been through
        compute_all_indicators → compute_all_rs →
        compute_composite_score, and optionally
        strategy.signals.generate_signals().

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) with score, metadata, signal
        gate, and return columns.  Symbols missing
        ``score_composite`` are silently skipped.

    Notes
    -----
    Any column starting with ``sig_`` is automatically carried
    forward so that per-ticker gates from ``strategy/signals.py``
    are available to ``output/signals.py`` for entry qualification.
    """
    frames: list[pd.DataFrame] = []

    for ticker, df in scored_universe.items():
        if df is None or df.empty:
            continue
        if "score_composite" not in df.columns:
            continue

        # ── Core score + metadata columns ─────────────────
        available = [c for c in _SCORE_COLS + _META_COLS
                     if c in df.columns]

        # ── Per-ticker signal gate columns (strategy/) ────
        sig_cols = [c for c in df.columns if c.startswith("sig_")]
        available += sig_cols

        subset = df[available].copy()
        subset["ticker"] = ticker

        # Recent returns for context
        if "close" in df.columns:
            c = df["close"]
            subset["ret_1d"]  = c.pct_change(1)
            subset["ret_5d"]  = c.pct_change(5)
            subset["ret_20d"] = c.pct_change(20)

        frames.append(subset)

    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames)
    panel = panel.set_index("ticker", append=True)
    panel.index.names = ["date", "ticker"]

    return panel.sort_index()


# ═══════════════════════════════════════════════════════════════
#  CROSS-SECTIONAL RANKING
# ═══════════════════════════════════════════════════════════════

def rank_universe(
    panel: pd.DataFrame,
    rank_col: str = "score_composite",
) -> pd.DataFrame:
    """
    Cross-sectional rank on each trading day.

    Higher score = better = rank 1.

    Parameters
    ----------
    panel : pd.DataFrame
        Output of ``build_rankings_panel()``.
    rank_col : str
        Column to rank by (default ``score_composite``).

    Returns
    -------
    pd.DataFrame
        Same frame with ``rank``, ``pct_rank``, ``universe_size``
        added.
    """
    if panel.empty or rank_col not in panel.columns:
        return panel

    result = panel.copy()
    grouped = result.groupby(level="date")[rank_col]

    result["rank"] = grouped.rank(
        ascending=False, method="min",
    ).astype(int)
    result["pct_rank"] = grouped.rank(ascending=False, pct=True)
    result["universe_size"] = grouped.transform("count").astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  RANK CHANGES
# ═══════════════════════════════════════════════════════════════

def compute_rank_changes(
    ranked: pd.DataFrame,
) -> pd.DataFrame:
    """
    Day-over-day rank movement for each ticker.

    rank_change > 0  → symbol moved UP in ranking (improved)
    rank_change < 0  → symbol dropped
    rank_change = 0  → unchanged
    """
    if ranked.empty or "rank" not in ranked.columns:
        return ranked

    result = ranked.copy()

    rank_wide = result["rank"].unstack(level="ticker")
    # previous_rank − current_rank → positive = improved
    change_wide = -rank_wide.diff()

    change_long = change_wide.stack()
    change_long.name = "rank_change"
    change_long.index.names = ["date", "ticker"]

    result = result.join(change_long)
    result["rank_change"] = result["rank_change"].fillna(0).astype(int)

    return result


# ═══════════════════════════════════════════════════════════════
#  PILLAR AGREEMENT
# ═══════════════════════════════════════════════════════════════

def compute_pillar_agreement(
    ranked: pd.DataFrame,
    threshold: float = 0.50,
) -> pd.DataFrame:
    """
    Count how many pillar scores exceed a threshold.

    High agreement (4/5 or 5/5 pillars bullish) signals broad
    confirmation — the composite isn't being carried by a single
    strong pillar masking weakness elsewhere.

    Parameters
    ----------
    ranked : pd.DataFrame
        Ranked panel with pillar score columns.
    threshold : float
        Score above which a pillar counts as bullish (default 0.50).

    Returns
    -------
    pd.DataFrame
        Same frame with ``pillars_bullish`` (int count) and
        ``pillar_agreement`` (0–1 fraction) appended.
    """
    if ranked.empty:
        return ranked

    result = ranked.copy()
    available = [c for c in _PILLAR_COLS if c in result.columns]

    if not available:
        result["pillars_bullish"]  = 0
        result["pillar_agreement"] = 0.0
        return result

    above = (result[available] > threshold).sum(axis=1)
    result["pillars_bullish"]  = above.astype(int)
    result["pillar_agreement"] = above / len(available)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_rankings(
    ranked: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's rankings as a flat ticker-indexed DataFrame.

    Parameters
    ----------
    ranked : pd.DataFrame
        Fully ranked panel (MultiIndex: date, ticker).
    date : pd.Timestamp or None
        Date to extract.  If None, uses the most recent date.

    Returns
    -------
    pd.DataFrame
        Ticker-indexed, sorted by rank (1 = best first).
    """
    if ranked.empty:
        return pd.DataFrame()

    dates = ranked.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snapshot = ranked.xs(date, level="date").copy()
    snapshot["date"] = date

    return snapshot.sort_values("rank")


def filter_top_n(
    snapshot: pd.DataFrame,
    n: int = 5,
) -> pd.DataFrame:
    """Filter a snapshot to the top N ranked symbols."""
    if snapshot.empty or "rank" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rank"] <= n].copy()


def filter_by_regime(
    snapshot: pd.DataFrame,
    regimes: list[str],
) -> pd.DataFrame:
    """Filter a snapshot to symbols in the given RS regimes."""
    if snapshot.empty or "rs_regime" not in snapshot.columns:
        return snapshot
    return snapshot[snapshot["rs_regime"].isin(regimes)].copy()


# ═══════════════════════════════════════════════════════════════
#  RANK HISTORY  (single ticker over time)
# ═══════════════════════════════════════════════════════════════

def rank_history(
    ranked: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """
    Extract one ticker's ranking history.

    Returns a date-indexed DataFrame showing rank, score, and
    change columns over time.
    """
    if ranked.empty:
        return pd.DataFrame()

    tickers = ranked.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return ranked.xs(ticker, level="ticker").copy()


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def rankings_summary(ranked: pd.DataFrame) -> dict:
    """
    Compute summary statistics for the latest day's rankings.

    Returns a dict with keys: date, universe_size, mean_composite,
    median_composite, std_composite, spread, top_ticker, top_score,
    bottom_ticker, bottom_score, regime_distribution.
    """
    snap = latest_rankings(ranked)
    if snap.empty:
        return {}

    comp = snap.get("score_composite")

    summary: dict = {
        "date":             snap["date"].iloc[0] if "date" in snap.columns else None,
        "universe_size":    len(snap),
        "mean_composite":   comp.mean()   if comp is not None else None,
        "median_composite": comp.median() if comp is not None else None,
        "std_composite":    comp.std()    if comp is not None else None,
        "spread":           (comp.max() - comp.min()) if comp is not None else None,
        "top_ticker":       snap.index[0],
        "top_score":        comp.iloc[0]  if comp is not None else None,
        "bottom_ticker":    snap.index[-1],
        "bottom_score":     comp.iloc[-1] if comp is not None else None,
    }

    if "rs_regime" in snap.columns:
        summary["regime_distribution"] = (
            snap["rs_regime"].value_counts().to_dict()
        )

    if "pillar_agreement" in snap.columns:
        summary["mean_agreement"] = snap["pillar_agreement"].mean()

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_rankings(
    scored_universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Full rankings pipeline.

    Parameters
    ----------
    scored_universe : dict
        {ticker: scored_df} — output of the scoring pipeline
        for each symbol in the universe.  May optionally include
        ``sig_*`` columns from ``strategy/signals.py``; these
        are carried forward into the ranked panel.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with all scores,
        ranks, rank changes, pillar agreement metrics, and
        any per-ticker signal gate columns.
    """
    panel = build_rankings_panel(scored_universe)
    if panel.empty:
        return pd.DataFrame()

    ranked = rank_universe(panel)
    ranked = compute_rank_changes(ranked)
    ranked = compute_pillar_agreement(ranked)

    return ranked


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def rankings_report(
    ranked: pd.DataFrame,
    top_n: int | None = None,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
) -> str:
    """
    Formatted text report of the latest rankings.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``.
    top_n : int or None
        If set, only show the top N symbols.  None = show all.
    breadth_regime : str
        Current breadth regime label (for the header).
    breadth_score : float
        Current breadth score (0–1) for the header.

    Returns
    -------
    str
        Human-readable rankings report.
    """
    if ranked.empty:
        return "No rankings data available."

    snap = latest_rankings(ranked)
    if snap.empty:
        return "No rankings data available."

    summary = rankings_summary(ranked)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────────
    ln.append(div)
    ln.append(f"UNIVERSE RANKINGS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Universe:      {summary.get('universe_size', 0)} symbols"
    )
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Mean score:    {summary.get('mean_composite', 0):.3f}"
    )
    ln.append(
        f"  Median score:  {summary.get('median_composite', 0):.3f}"
    )
    ln.append(
        f"  Spread:        {summary.get('spread', 0):.3f}  "
        f"(top {summary.get('top_score', 0):.3f} → "
        f"bottom {summary.get('bottom_score', 0):.3f})"
    )
    if summary.get("mean_agreement") is not None:
        ln.append(
            f"  Mean agree:    {summary['mean_agreement']:.0%}"
        )

    # ── Rankings table ────────────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("RANKINGS")
    ln.append(sub)

    display = filter_top_n(snap, top_n) if top_n else snap

    pillar_present = [c for c in _PILLAR_COLS if c in display.columns]
    short_names = {
        "score_rotation":       "Rot",
        "score_momentum":       "Mom",
        "score_volatility":     "Vol",
        "score_microstructure": "Micro",
        "score_breadth":        "Brdth",
    }

    # Header line
    hdr = f"  {'#':>3}  {'Ticker':<7} {'Comp':>6}"
    for pc in pillar_present:
        hdr += f" {short_names.get(pc, pc[-5:]):>6}"
    hdr += f"  {'Regime':<12} {'1d':>7} {'5d':>7} {'Agree':>5} {'Δ':>3}"
    ln.append(hdr)

    sep = f"  {'───':>3}  {'───────':<7} {'──────':>6}"
    for _ in pillar_present:
        sep += f" {'──────':>6}"
    sep += (
        f"  {'────────────':<12}"
        f" {'───────':>7} {'───────':>7} {'─────':>5} {'───':>3}"
    )
    ln.append(sep)

    for ticker, row in display.iterrows():
        rank_val = int(row.get("rank", 0))
        comp_val = row.get("score_composite", 0)
        regime   = str(row.get("rs_regime", "?"))
        ret_1d   = row.get("ret_1d", np.nan)
        ret_5d   = row.get("ret_5d", np.nan)
        agree    = row.get("pillar_agreement", 0)
        delta    = int(row.get("rank_change", 0))

        line = f"  {rank_val:>3}  {ticker:<7} {comp_val:>6.3f}"
        for pc in pillar_present:
            v = row.get(pc, 0)
            line += f" {v:>6.3f}" if pd.notna(v) else f" {'—':>6}"

        ret_1d_str = f"{ret_1d:>+7.1%}" if pd.notna(ret_1d) else f"{'—':>7}"
        ret_5d_str = f"{ret_5d:>+7.1%}" if pd.notna(ret_5d) else f"{'—':>7}"
        delta_str  = f"{delta:+d}" if delta != 0 else "0"

        line += (
            f"  {regime:<12}"
            f" {ret_1d_str}"
            f" {ret_5d_str}"
            f" {agree:>5.0%}"
            f" {delta_str:>3}"
        )
        ln.append(line)

    # ── Top movers ────────────────────────────────────────────
    if "rank_change" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("TOP MOVERS")
        ln.append(sub)

        risers = snap[snap["rank_change"] > 0].sort_values(
            "rank_change", ascending=False
        )
        fallers = snap[snap["rank_change"] < 0].sort_values(
            "rank_change", ascending=True
        )

        if not risers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in risers["rank_change"].head(5).items()
            ]
            ln.append(f"  Risers:   {', '.join(parts)}")
        else:
            ln.append(f"  Risers:   (none)")

        if not fallers.empty:
            parts = [
                f"{t} ({int(r):+d})"
                for t, r in fallers["rank_change"].head(5).items()
            ]
            ln.append(f"  Fallers:  {', '.join(parts)}")
        else:
            ln.append(f"  Fallers:  (none)")

    # ── Regime distribution ───────────────────────────────────
    if "rs_regime" in snap.columns:
        ln.append("")
        ln.append(sub)
        ln.append("REGIME DISTRIBUTION")
        ln.append(sub)

        n_total = len(snap)
        regime_counts = snap["rs_regime"].value_counts()
        for regime in ["leading", "improving", "weakening", "lagging"]:
            cnt = regime_counts.get(regime, 0)
            frac = cnt / n_total if n_total > 0 else 0
            bar = "█" * int(frac * 30)
            ln.append(
                f"  {regime:<12} {cnt:>2} / {n_total}"
                f"  ({frac:>4.0%})  {bar}"
            )

    # ── Pillar agreement ──────────────────────────────────────
    if "pillars_bullish" in snap.columns and pillar_present:
        ln.append("")
        ln.append(sub)
        ln.append("PILLAR AGREEMENT")
        ln.append(sub)

        n_pillars = len(pillar_present)
        for i in range(n_pillars, -1, -1):
            cnt = (snap["pillars_bullish"] == i).sum()
            if cnt > 0:
                matched = list(snap[snap["pillars_bullish"] == i].index)
                ticker_str = ", ".join(matched[:6])
                if len(matched) > 6:
                    ticker_str += f" (+{len(matched) - 6} more)"
                ln.append(
                    f"  {i}/{n_pillars} bullish:"
                    f"  {cnt} symbol{'s' if cnt != 1 else ''}"
                    f"  ({ticker_str})"
                )

    return "\n".join(ln)


##############################################################    
"""
output/reports.py
-----------------
Comprehensive strategy reports that combine rankings, signals,
breadth, gate diagnostics, and (optional) backtest performance
into a single unified report.

Layers
──────
  daily_report()          One-day strategy snapshot
  transition_report()     Recent signal changes
  breadth_section()       Market breadth analysis section
  strategy_overview()     Static strategy rules reference
  performance_report()    Backtest results (when available)
  generate_full_report()  Master — combines all sections

Each function returns a plain-text string.  The master function
concatenates whichever sections are available, so it works
both during live signal generation (no backtest) and after a
historical simulation.

NOTE: compute/breadth.py already exports ``breadth_report()``
      which provides a historical breadth dump with lookback.
      This module's ``breadth_section()`` is a shorter summary
      intended as one section of the unified strategy report.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from cash.output.signals import (
    SignalConfig,
    BUY, HOLD, SELL, NEUTRAL,
    latest_signals,
    signal_changes,
    active_positions,
    signals_summary,
    compute_turnover,
    _count_gates,
)
from cash.output.rankings import (
    latest_rankings,
    rankings_summary,
)


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

_DIV = "=" * 76
_SUB = "-" * 76
_THIN = "·" * 76

_REGIME_ICON = {
    "leading":   "🟢",
    "improving": "🔵",
    "weakening": "🟡",
    "lagging":   "🔴",
}

_SIG_ICON = {
    BUY:     "🟢 BUY ",
    HOLD:    "🔵 HOLD",
    SELL:    "🔴 SELL",
    NEUTRAL: "⚪ —   ",
}


# ═══════════════════════════════════════════════════════════════
#  DAILY REPORT
# ═══════════════════════════════════════════════════════════════

def daily_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> str:
    """
    Comprehensive single-day strategy report.

    Includes: market context, active positions with gate
    diagnostics, eligible watchlist, exit candidates, recent
    transitions, and turnover metrics.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return "No data available for daily report."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No data available for daily report."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []

    # ── Header ────────────────────────────────────────────
    ln.append(_DIV)
    ln.append(f"  DAILY STRATEGY REPORT — {date_str}")
    ln.append(_DIV)

    # Market context
    br_regime = "unknown"
    br_score = 0.0
    if breadth is not None and not breadth.empty:
        br_regime = breadth["breadth_regime"].iloc[-1]
        if "breadth_score" in breadth.columns:
            br_score = breadth["breadth_score"].iloc[-1]

    ln.append("")
    ln.append("  MARKET CONTEXT")
    ln.append(f"  Breadth regime:  {br_regime} ({br_score:.3f})")
    ln.append(
        f"  Positions:       {summary.get('n_active', 0)}"
        f" / {config.max_positions} max"
    )
    ln.append(
        f"  Entry mode:      "
        f"{'sig_confirmed (6 gates)' if has_gates else 'score threshold (fallback)'}"
    )
    ln.append(
        f"  Signal mix:      "
        f"BUY {summary.get('n_buy', 0)}  "
        f"HOLD {summary.get('n_hold', 0)}  "
        f"SELL {summary.get('n_sell', 0)}  "
        f"NEUTRAL {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean conviction: {summary['mean_strength']:.3f}"
        )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(_SUB)
    ln.append("  ACTIVE POSITIONS")
    ln.append(_SUB)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        for ticker, row in active.iterrows():
            sig = _SIG_ICON.get(row["signal"], row["signal"])
            rank = int(row.get("rank", 0))
            comp = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime = str(row.get("rs_regime", "?"))
            r_icon = _REGIME_ICON.get(regime, "")
            ret_1d = row.get("ret_1d", np.nan)
            ret_5d = row.get("ret_5d", np.nan)

            r1 = f"{ret_1d:+.1%}" if pd.notna(ret_1d) else "—"
            r5 = f"{ret_5d:+.1%}" if pd.notna(ret_5d) else "—"

            ln.append(
                f"  {sig}  {ticker:<6}  "
                f"#{rank}  score={comp:.3f}  "
                f"str={strength:.3f}  "
                f"{r_icon} {regime:<11}  "
                f"1d={r1}  5d={r5}"
            )

            # Gate detail for active positions
            if has_gates:
                gates = _format_gate_line(row)
                ln.append(f"           {gates}")

    # ── Gate diagnostics for full universe ────────────────
    if has_gates:
        ln.append("")
        ln.append(_SUB)
        ln.append("  UNIVERSE GATE DIAGNOSTICS")
        ln.append(_SUB)

        for ticker in snap.sort_values("rank").index:
            row = snap.loc[ticker]
            rank = int(row.get("rank", 0))
            conf = "CONF" if row.get("sig_confirmed") == 1 else "—"
            reason = str(row.get("sig_reason", ""))
            gates = _format_gate_line(row)
            ln.append(
                f"  #{rank:<3} {ticker:<6} [{conf:<4}] "
                f"{gates}  {reason}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        watchlist = snap[
            (snap["signal"] == NEUTRAL)
            & (snap["entry_eligible"])
        ]
        if not watchlist.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  WATCHLIST (eligible, no slot)")
            ln.append(_SUB)
            for ticker, row in watchlist.iterrows():
                ln.append(
                    f"  ○ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}  "
                    f"{row.get('rs_regime', '?')}"
                )

    # ── Exit candidates ───────────────────────────────────
    if "exit_triggered" in snap.columns:
        exits = snap[
            (snap["signal"].isin([BUY, HOLD]))
            & (snap["exit_triggered"])
        ]
        if not exits.empty:
            ln.append("")
            ln.append(_SUB)
            ln.append("  ⚠ EXIT CANDIDATES (threshold breached)")
            ln.append(_SUB)
            for ticker, row in exits.iterrows():
                ln.append(
                    f"  ✕ {ticker:<6}  "
                    f"#{int(row.get('rank', 0))}  "
                    f"score={row.get('score_composite', 0):.3f}"
                )

    # ── Recent transitions ────────────────────────────────
    changes = signal_changes(signals_df)
    if not changes.empty:
        recent = changes.tail(10)
        ln.append("")
        ln.append(_SUB)
        ln.append("  RECENT TRANSITIONS (last 10)")
        ln.append(_SUB)
        for (dt, tkr), row in recent.iterrows():
            dt_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            ln.append(
                f"  {dt_str}  {tkr:<6}  "
                f"{row.get('transition', '?')}"
            )

    # ── Turnover ──────────────────────────────────────────
    turnover = compute_turnover(signals_df, lookback=20)
    if not turnover.empty:
        ln.append("")
        ln.append(_SUB)
        ln.append("  TURNOVER")
        ln.append(_SUB)
        total_buys = int(turnover["buys"].sum())
        total_sells = int(turnover["sells"].sum())
        avg_active = turnover["active"].mean()
        roll_turn = turnover["rolling_turnover"].iloc[-1]
        ln.append(f"  Total entries:    {total_buys}")
        ln.append(f"  Total exits:      {total_sells}")
        ln.append(f"  Avg active:       {avg_active:.1f}")
        ln.append(f"  Rolling turnover: {roll_turn:.3f} (20d)")

    ln.append("")
    ln.append(_DIV)
    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  BREADTH SECTION
# ═══════════════════════════════════════════════════════════════

def breadth_section(
    breadth: pd.DataFrame | None,
) -> str:
    """
    Market breadth analysis section for the unified report.

    This is a *report section formatter* — a short summary of
    the current breadth state suitable for embedding in the
    strategy report.  For a detailed historical breadth dump
    with configurable lookback, use ``compute.breadth.breadth_report()``.
    """
    if breadth is None or breadth.empty:
        return "No breadth data available."

    ln: list[str] = []
    ln.append(_SUB)
    ln.append("  BREADTH ANALYSIS")
    ln.append(_SUB)

    regime = breadth["breadth_regime"].iloc[-1]
    score = (
        breadth["breadth_score"].iloc[-1]
        if "breadth_score" in breadth.columns else 0
    )
    smooth = (
        breadth["breadth_score_smooth"].iloc[-1]
        if "breadth_score_smooth" in breadth.columns else score
    )

    ln.append(f"  Current regime:  {regime}")
    ln.append(f"  Raw score:       {score:.3f}")
    ln.append(f"  Smoothed score:  {smooth:.3f}")

    # Regime history (last 5 unique)
    if "breadth_regime" in breadth.columns:
        regimes = breadth["breadth_regime"].dropna()
        if len(regimes) > 0:
            shifted = regimes != regimes.shift(1)
            transitions = regimes[shifted].tail(5)
            if len(transitions) > 0:
                parts = []
                for dt, r in transitions.items():
                    d = (
                        dt.strftime("%m-%d")
                        if hasattr(dt, "strftime") else str(dt)
                    )
                    parts.append(f"{d}: {r}")
                ln.append(f"  Recent shifts:   {' → '.join(parts)}")

    # Score distribution
    if "breadth_score" in breadth.columns:
        bs = breadth["breadth_score"].dropna()
        if len(bs) > 20:
            ln.append(f"  Score range:     "
                      f"[{bs.min():.3f}, {bs.max():.3f}]")
            ln.append(f"  20d mean:        "
                      f"{bs.tail(20).mean():.3f}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  STRATEGY OVERVIEW
# ═══════════════════════════════════════════════════════════════

def strategy_overview(
    config: SignalConfig | None = None,
) -> str:
    """
    Static description of the strategy rules and parameters.
    Useful as a reference appendix in any report.
    """
    if config is None:
        config = SignalConfig()

    has_gates = True  # describe full system

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  STRATEGY OVERVIEW")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Per-Ticker Quality Gates (strategy/signals.py)")
    ln.append(_THIN)
    ln.append("  1. Score threshold  — score_adjusted ≥ entry_min")
    ln.append("  2. RS regime        — stock in leading/improving")
    ln.append("  3. Sector regime    — sector tide favourable")
    ln.append("  4. Breadth regime   — market not weak")
    ln.append("  5. Momentum streak  — N consecutive days > 0.5")
    ln.append("  6. Cooldown         — not recently exited")
    ln.append("  All six must pass → sig_confirmed = 1")

    ln.append("")
    ln.append("  Portfolio-Level Signals (output/signals.py)")
    ln.append(_THIN)
    ln.append(
        f"  Entry:   sig_confirmed AND rank ≤ "
        f"{config.entry_rank_max}"
    )
    ln.append(
        f"  Exit:    rank > {config.exit_rank_max} OR "
        f"score < {config.exit_score_min}"
    )
    ln.append(
        f"  Max positions:   {config.max_positions}"
    )
    ln.append(
        f"  Rank hysteresis: enter ≤ {config.entry_rank_max}, "
        f"exit > {config.exit_rank_max}"
    )
    ln.append(
        f"  Breadth breaker: "
        f"{config.breadth_bearish_action} when "
        f"regime ∈ {config.breadth_bearish}"
    )

    ln.append("")
    ln.append("  Signal Strength Weights")
    ln.append(_THIN)
    ln.append(f"  Composite score:  {config.w_score:.0%}")
    ln.append(f"  Rank percentile:  {config.w_rank:.0%}")
    ln.append(f"  Pillar agreement: {config.w_agreement:.0%}")
    ln.append(f"  Regime quality:   {config.w_regime:.0%}")
    ln.append(f"  Breadth quality:  {config.w_breadth:.0%}")

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE REPORT
# ═══════════════════════════════════════════════════════════════

def performance_report(
    backtest_result,
) -> str:
    """
    Format backtest results into a text report section.

    Parameters
    ----------
    backtest_result
        A ``BacktestResult`` from ``portfolio.backtest``.
        If None, returns placeholder text.
    """
    if backtest_result is None:
        return "No backtest results available."

    m = backtest_result.metrics
    if not m:
        return "No performance metrics computed."

    ln: list[str] = []
    ln.append(_DIV)
    ln.append("  BACKTEST PERFORMANCE")
    ln.append(_DIV)

    ln.append("")
    ln.append("  Returns")
    ln.append(_THIN)
    ln.append(
        f"  Total return:     "
        f"{m.get('total_return', 0):+.2%}"
    )
    ln.append(
        f"  CAGR:             "
        f"{m.get('cagr', 0):+.2%}"
    )
    ln.append(
        f"  Volatility (ann): "
        f"{m.get('annual_volatility', 0):.2%}"
    )

    ln.append("")
    ln.append("  Risk-Adjusted")
    ln.append(_THIN)
    ln.append(
        f"  Sharpe ratio:     "
        f"{m.get('sharpe_ratio', 0):.3f}"
    )
    ln.append(
        f"  Sortino ratio:    "
        f"{m.get('sortino_ratio', 0):.3f}"
    )
    ln.append(
        f"  Calmar ratio:     "
        f"{m.get('calmar_ratio', 0):.3f}"
    )

    ln.append("")
    ln.append("  Drawdown")
    ln.append(_THIN)
    ln.append(
        f"  Max drawdown:     "
        f"{m.get('max_drawdown', 0):.2%}"
    )
    ln.append(
        f"  Max DD duration:  "
        f"{m.get('max_dd_duration', 0)} days"
    )
    ln.append(
        f"  Current DD:       "
        f"{m.get('current_drawdown', 0):.2%}"
    )

    ln.append("")
    ln.append("  Trading")
    ln.append(_THIN)
    ln.append(
        f"  Total trades:     "
        f"{m.get('total_trades', 0)}"
    )
    ln.append(
        f"  Win rate:         "
        f"{m.get('win_rate', 0):.1%}"
    )
    ln.append(
        f"  Profit factor:    "
        f"{m.get('profit_factor', 0):.2f}"
    )
    ln.append(
        f"  Avg win / loss:   "
        f"{m.get('avg_win', 0):+.2%} / "
        f"{m.get('avg_loss', 0):+.2%}"
    )
    ln.append(
        f"  Total commission: "
        f"${m.get('total_commission', 0):,.2f}"
    )

    ln.append("")
    ln.append("  Capital")
    ln.append(_THIN)
    ln.append(
        f"  Initial:          "
        f"${m.get('initial_capital', 0):,.2f}"
    )
    ln.append(
        f"  Final:            "
        f"${m.get('final_capital', 0):,.2f}"
    )
    ln.append(
        f"  Peak:             "
        f"${m.get('peak_capital', 0):,.2f}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_full_report(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
    backtest_result=None,
    include_strategy: bool = True,
) -> str:
    """
    Combine all report sections into one comprehensive document.

    Includes whichever sections have data: daily signals are
    always included, breadth if provided, backtest performance
    if a result object is passed, and the strategy overview
    if requested.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``compute_all_signals()``.
    breadth : pd.DataFrame or None
        Breadth data.
    config : SignalConfig or None
        Portfolio-level config.
    backtest_result : BacktestResult or None
        Output of ``run_backtest()``.
    include_strategy : bool
        Whether to append the strategy rules reference.

    Returns
    -------
    str
        Full text report.
    """
    if config is None:
        config = SignalConfig()

    sections: list[str] = []

    # Daily snapshot
    sections.append(daily_report(signals_df, breadth, config))

    # Breadth detail
    if breadth is not None and not breadth.empty:
        sections.append(breadth_section(breadth))

    # Backtest performance
    if backtest_result is not None:
        sections.append(performance_report(backtest_result))

    # Strategy reference
    if include_strategy:
        sections.append(strategy_overview(config))

    return "\n\n".join(sections)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _format_gate_line(row: pd.Series) -> str:
    """Format per-ticker gate status as a compact string."""
    gate_cols = [
        ("sig_regime_ok",   "Reg"),
        ("sig_sector_ok",   "Sec"),
        ("sig_breadth_ok",  "Brd"),
        ("sig_momentum_ok", "Mom"),
        ("sig_in_cooldown", "CD"),
    ]

    parts: list[str] = []
    for col, label in gate_cols:
        if col not in row.index:
            continue
        val = row[col]
        if col == "sig_in_cooldown":
            icon = "✕" if val else "✓"
        else:
            icon = "✓" if val else "✕"
        parts.append(f"{icon}{label}")

    return "  ".join(parts) if parts else "—"


"""
output/signals.py
-----------------
Portfolio-level trade signal generation.

Layers on top of ``strategy/signals.py`` (per-ticker quality
gates) and ``output/rankings.py`` (cross-sectional rankings)
to produce final portfolio signals: BUY, HOLD, SELL, NEUTRAL.

Architecture
────────────
  strategy/signals.py answers "Is this ticker trade-worthy?"
    · Six per-ticker gates: regime, sector, breadth, momentum,
      cooldown, score threshold
    · Produces: sig_confirmed (0/1), sig_exit (0/1),
      sig_position_pct, gate diagnostics

  output/rankings.py answers "Where does this ticker rank?"
    · Cross-sectional ranking by composite score
    · Produces: rank, pillar_agreement, universe_size

  output/signals.py answers "Which tickers do we hold?"     ← this file
    · Uses sig_confirmed for entry qualification
    · Adds cross-sectional rank filter with hysteresis
    · Enforces position limits
    · Portfolio-level breadth circuit breaker
    · Conviction scoring for downstream sizing

  When strategy/signals.py has NOT been run (sig_confirmed
  absent from the panel), entry falls back to:
    score_composite ≥ entry_score_min

Signal Types
────────────
  BUY      Enter new position
  HOLD     Maintain existing position
  SELL     Exit position
  NEUTRAL  No position

Entry (AND — all required)
──────────────────────────
  sig_confirmed == 1   (or score ≥ entry_score_min as fallback)
  rank ≤ entry_rank_max
  slots available      (< max_positions)
  breadth not bearish

Exit (OR — any one fires)
─────────────────────────
  rank > exit_rank_max
  score_composite < exit_score_min
  breadth bearish + exit_all mode

Hysteresis
──────────
  Rank band: enter at rank ≤ 5, exit only at rank > 8.
  A symbol entering at rank 3 stays through rank 7 without
  churning.

  Per-ticker hysteresis (cooldown, momentum streak) is
  handled by strategy/signals.py through sig_confirmed.
  The two layers stack: a ticker must survive both the
  per-ticker quality bar AND the cross-sectional rank bar.

Signal Strength
───────────────
  A 0–1 conviction score blending:
    composite score    30%   — raw quality
    rank percentile    20%   — position in universe
    pillar agreement   20%   — breadth of confirmation
    regime quality     15%   — RS regime desirability
    breadth quality    15%   — market health

Pipeline
────────
  ranked_panel (with optional sig_* columns from strategy/)
       ↓
  check_entry_eligible()     — boolean per row
       ↓
  check_exit_triggered()     — boolean per row
       ↓
  generate_signals()         — stateful BUY/HOLD/SELL/NEUTRAL
       ↓
  compute_signal_strength()  — 0–1 conviction score
       ↓
  compute_all_signals()      — master orchestrator
       ↓
  latest_signals()           — single-day snapshot
  signal_changes()           — entries / exits / transitions
  signal_history()           — single ticker over time
  active_positions()         — currently held symbols
  compute_turnover()         — entry/exit frequency
  signals_summary()          — summary statistics dict
  signals_report()           — formatted text report
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

BUY     = "BUY"
HOLD    = "HOLD"
SELL    = "SELL"
NEUTRAL = "NEUTRAL"

_REGIME_QUALITY: dict[str, float] = {
    "leading":    1.00,
    "improving":  0.75,
    "weakening":  0.25,
    "lagging":    0.00,
}

_BREADTH_QUALITY: dict[str, float] = {
    "strong":   1.00,
    "healthy":  0.80,
    "neutral":  0.50,
    "caution":  0.30,
    "weak":     0.10,
    "critical": 0.00,
}


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class SignalConfig:
    """
    Portfolio-level signal thresholds.

    Per-ticker quality thresholds (regime, sector, momentum,
    cooldown, score) live in ``SIGNAL_PARAMS`` and are enforced
    by ``strategy/signals.py``.  This config controls only the
    cross-sectional and portfolio layers.
    the portfolio-level allocator. It takes the full cross-sectional panel (after every ticker has been scored and gated) and 
    answers "which of the trade-worthy tickers do we actually hold?" through rank filtering with hysteresis, position limits, and 
    a portfolio-level breadth circuit breaker.
    """

    # ── Rank thresholds (hysteresis band) ─────────────────
    entry_rank_max: int = 8          # was 5 — enter if ranked in top 8
    exit_rank_max:  int = 20         # was 8 — only exit if falls below 20

    # ── Score thresholds ──────────────────────────────────
    #    exit_score_min always applies as an OR exit trigger.
    #    entry_score_min is used only when sig_confirmed is
    #    absent (strategy/signals.py was not run).
    entry_score_min: float = 0.40
    exit_score_min:  float = 0.30

    # ── Breadth circuit breaker ───────────────────────────
    breadth_bearish:        tuple[str, ...] = ("weak", "critical")
    breadth_bearish_action: str = "reduce"    # "reduce" or "exit_all"

    # ── Position limits ───────────────────────────────────
    max_positions: int = 8

    # ── Signal strength weights ───────────────────────────
    w_score:     float = 0.30
    w_rank:      float = 0.20
    w_agreement: float = 0.20
    w_regime:    float = 0.15
    w_breadth:   float = 0.15


# ═══════════════════════════════════════════════════════════════
#  ENTRY / EXIT ELIGIBILITY
# ═══════════════════════════════════════════════════════════════

def check_entry_eligible(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where entry criteria are met.

    When ``sig_confirmed`` is present (strategy/signals.py
    was run on each ticker before ranking):

        sig_confirmed == 1  AND  rank ≤ entry_rank_max

    Fallback (``sig_confirmed`` absent):

        score_composite ≥ entry_score_min  AND  rank ≤ entry_rank_max
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    rank_ok = ranked["rank"] <= config.entry_rank_max

    if "sig_confirmed" in ranked.columns:
        ticker_ok = ranked["sig_confirmed"] == 1
    else:
        ticker_ok = ranked["score_composite"] >= config.entry_score_min

    return rank_ok & ticker_ok


def check_exit_triggered(
    ranked: pd.DataFrame,
    config: SignalConfig | None = None,
) -> pd.Series:
    """
    Boolean mask: True where any exit threshold is breached.

    Triggers (OR):
      rank              > exit_rank_max
      score_composite   < exit_score_min
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.Series(dtype=bool)

    breach = ranked["rank"] > config.exit_rank_max
    breach = breach | (
        ranked["score_composite"] < config.exit_score_min
    )

    return breach


# ═══════════════════════════════════════════════════════════════
#  STATEFUL SIGNAL GENERATION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Generate BUY / HOLD / SELL / NEUTRAL signals with hysteresis.

    Processes the ranked panel date-by-date, maintaining a set
    of held positions.

    For held positions each day:
      · Force exit if breadth bearish + exit_all mode
      · Exit if any exit trigger fires (rank or score)
      · Otherwise HOLD

    For non-held tickers each day:
      · Skip if breadth bearish (both modes block new entries)
      · BUY if entry eligible and slots available
      · Otherwise NEUTRAL

    New entries are prioritised by composite score (highest
    first) when more candidates than available slots.

    Parameters
    ----------
    ranked : pd.DataFrame
        MultiIndex (date, ticker) panel from
        ``compute_all_rankings()``, optionally containing
        ``sig_confirmed`` / ``sig_exit`` columns from
        ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data with ``breadth_regime`` column, indexed
        by date.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        Input panel with ``signal``, ``entry_eligible``,
        ``exit_triggered``, and ``in_position`` columns added.
    """
    if config is None:
        config = SignalConfig()
    if ranked.empty:
        return pd.DataFrame()

    # Pre-compute masks over the entire panel
    entry_mask = check_entry_eligible(ranked, config)
    exit_mask  = check_exit_triggered(ranked, config)

    dates = ranked.index.get_level_values("date").unique().sort_values()

    held: set[str]                = set()
    signals: dict[tuple, str]     = {}

    for date in dates:

        # ── Day slices ────────────────────────────────────
        try:
            day_data  = ranked.xs(date, level="date")
            day_entry = entry_mask.xs(date, level="date")
            day_exit  = exit_mask.xs(date, level="date")
        except KeyError:
            continue

        day_tickers = set(day_data.index.tolist())

        # ── Breadth circuit breaker ───────────────────────
        breadth_is_bearish = False
        if (
            breadth is not None
            and "breadth_regime" in breadth.columns
            and date in breadth.index
        ):
            br = breadth.loc[date, "breadth_regime"]
            if isinstance(br, pd.Series):
                br = br.iloc[0]
            breadth_is_bearish = br in config.breadth_bearish

        day_signals: dict[str, str] = {}
        sells: set[str] = set()

        # ── 1. Process held positions ─────────────────────
        for ticker in list(held):
            if ticker not in day_tickers:
                # Ticker dropped from panel (no data today)
                held.discard(ticker)
                continue

            force_exit = (
                breadth_is_bearish
                and config.breadth_bearish_action == "exit_all"
            )

            if force_exit or day_exit.loc[ticker]:
                day_signals[ticker] = SELL
                sells.add(ticker)
            else:
                day_signals[ticker] = HOLD

        held -= sells

        # ── 2. New entries ────────────────────────────────
        #    Blocked when breadth is bearish (both modes).
        if not breadth_is_bearish:
            slots = config.max_positions - len(held)
            if slots > 0:
                candidates: list[tuple[str, float]] = []
                for ticker in day_tickers:
                    if ticker in day_signals:
                        continue          # already HOLD or SELL
                    if day_entry.loc[ticker]:
                        score = day_data.loc[
                            ticker, "score_composite"
                        ]
                        candidates.append((ticker, score))

                # Best composite score gets priority
                candidates.sort(key=lambda x: x[1], reverse=True)

                for ticker, _ in candidates[: max(0, slots)]:
                    day_signals[ticker] = BUY
                    held.add(ticker)

        # ── 3. Remainder → NEUTRAL ───────────────────────
        for ticker in day_tickers:
            if ticker not in day_signals:
                day_signals[ticker] = NEUTRAL

        # ── Store ─────────────────────────────────────────
        for ticker, sig in day_signals.items():
            signals[(date, ticker)] = sig

    # ── Assemble result ───────────────────────────────────
    sig_series = pd.Series(signals, name="signal")
    sig_series.index = pd.MultiIndex.from_tuples(
        sig_series.index, names=["date", "ticker"],
    )

    result = ranked.copy()
    result["signal"]         = sig_series
    result["signal"]         = result["signal"].fillna(NEUTRAL)
    result["entry_eligible"] = entry_mask
    result["exit_triggered"] = exit_mask
    result["in_position"]    = result["signal"].isin([BUY, HOLD])

    return result


# ═══════════════════════════════════════════════════════════════
#  SIGNAL STRENGTH  (CONVICTION)
# ═══════════════════════════════════════════════════════════════

def compute_signal_strength(
    signals_df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Compute 0–1 conviction score for each row.

    Components and default weights:
      score_composite    30%  — raw composite quality
      rank_factor        20%  — position in universe
      pillar_agreement   20%  — breadth of confirmation
      regime_quality     15%  — RS regime desirability
      breadth_quality    15%  — market breadth health

    Strength is zeroed for SELL and NEUTRAL signals.

    Parameters
    ----------
    signals_df : pd.DataFrame
        Output of ``generate_signals()``.
    breadth : pd.DataFrame or None
        Breadth data for the breadth quality component.
    config : SignalConfig or None
        Weights.

    Returns
    -------
    pd.DataFrame
        Same frame with ``signal_strength`` column added.
    """
    if config is None:
        config = SignalConfig()
    if signals_df.empty:
        return signals_df

    result = signals_df.copy()
    active = result["signal"].isin([BUY, HOLD])

    # ── 1. Score factor (already 0–1) ─────────────────────
    score_f = result["score_composite"].clip(0, 1)

    # ── 2. Rank factor ────────────────────────────────────
    usize  = result["universe_size"].clip(lower=2)
    rank_f = (1.0 - (result["rank"] - 1) / (usize - 1)).clip(0, 1)

    # ── 3. Agreement factor ───────────────────────────────
    if "pillar_agreement" in result.columns:
        agree_f = result["pillar_agreement"].fillna(0.5)
    else:
        agree_f = pd.Series(0.5, index=result.index)

    # ── 4. Regime factor ──────────────────────────────────
    if "rs_regime" in result.columns:
        regime_f = (
            result["rs_regime"].map(_REGIME_QUALITY).fillna(0.5)
        )
    else:
        regime_f = pd.Series(0.5, index=result.index)

    # ── 5. Breadth factor ─────────────────────────────────
    breadth_f = pd.Series(0.5, index=result.index)
    if breadth is not None and "breadth_regime" in breadth.columns:
        b_quality = breadth["breadth_regime"].map(_BREADTH_QUALITY)
        dates     = result.index.get_level_values("date")
        breadth_f = b_quality.reindex(dates).fillna(0.5)
        breadth_f.index = result.index

    # ── Weighted blend ────────────────────────────────────
    strength = (
        config.w_score     * score_f
        + config.w_rank    * rank_f
        + config.w_agreement * agree_f
        + config.w_regime  * regime_f
        + config.w_breadth * breadth_f
    ).clip(0, 1)

    # Zero out non-active signals
    result["signal_strength"] = strength.where(active, 0.0)

    return result


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT / HISTORY EXTRACTORS
# ═══════════════════════════════════════════════════════════════

def latest_signals(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Extract one day's signals as a flat ticker-indexed DataFrame.

    Sorted: BUY first, then HOLD, then NEUTRAL, then SELL,
    each group sorted by rank.
    """
    if signals_df.empty:
        return pd.DataFrame()

    dates = signals_df.index.get_level_values("date").unique()

    if date is not None:
        if date not in dates:
            prior = dates[dates <= date]
            if prior.empty:
                return pd.DataFrame()
            date = prior[-1]
    else:
        date = dates[-1]

    snap = signals_df.xs(date, level="date").copy()
    snap["date"] = date

    _priority = {BUY: 0, HOLD: 1, NEUTRAL: 2, SELL: 3}
    snap["_sort"] = snap["signal"].map(_priority).fillna(4)
    snap = snap.sort_values(["_sort", "rank"]).drop(columns="_sort")

    return snap


def signal_changes(
    signals_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract rows where the signal changed from the prior day.

    Adds ``prev_signal`` and ``transition`` columns
    (e.g. ``NEUTRAL → BUY``, ``HOLD → SELL``).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    sig_wide  = signals_df["signal"].unstack(level="ticker")
    prev_wide = sig_wide.shift(1)

    changed_wide = sig_wide != prev_wide
    # First date always "changed" — exclude
    changed_wide.iloc[0] = False

    changed_long = changed_wide.stack()
    changed_long.name = "changed"
    changed_long.index.names = ["date", "ticker"]

    mask = changed_long[changed_long]
    if mask.empty:
        return pd.DataFrame()

    changes = signals_df.loc[mask.index].copy()

    prev_long = prev_wide.stack()
    prev_long.name = "prev_signal"
    prev_long.index.names = ["date", "ticker"]

    changes = changes.join(prev_long)
    changes["transition"] = (
        changes["prev_signal"].astype(str) + " → "
        + changes["signal"].astype(str)
    )

    return changes


def signal_history(
    signals_df: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame:
    """Extract one ticker's signal history over time."""
    if signals_df.empty:
        return pd.DataFrame()

    tickers = signals_df.index.get_level_values("ticker").unique()
    if ticker not in tickers:
        return pd.DataFrame()

    return signals_df.xs(ticker, level="ticker").copy()


def active_positions(
    signals_df: pd.DataFrame,
    date: pd.Timestamp | None = None,
) -> list[str]:
    """
    Return tickers with BUY or HOLD on a given date.

    Sorted by rank (best first).
    """
    snap = latest_signals(signals_df, date)
    if snap.empty:
        return []

    active = snap[snap["signal"].isin([BUY, HOLD])]
    return active.index.tolist()


# ═══════════════════════════════════════════════════════════════
#  TURNOVER ANALYSIS
# ═══════════════════════════════════════════════════════════════

def compute_turnover(
    signals_df: pd.DataFrame,
    lookback: int = 20,
) -> pd.DataFrame:
    """
    Compute daily position turnover.

    turnover = (entries + exits) / universe_size per day,
    smoothed over a rolling window.

    Returns a date-indexed DataFrame with buys, sells, active
    positions count, daily turnover, and rolling turnover.
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return pd.DataFrame()

    dates = (
        signals_df.index.get_level_values("date")
        .unique().sort_values()
    )

    buys = signals_df[signals_df["signal"] == BUY].groupby(
        level="date",
    ).size()
    sells = signals_df[signals_df["signal"] == SELL].groupby(
        level="date",
    ).size()
    active = signals_df[
        signals_df["signal"].isin([BUY, HOLD])
    ].groupby(level="date").size()
    universe = signals_df.groupby(level="date").size()

    buys     = buys.reindex(dates, fill_value=0)
    sells    = sells.reindex(dates, fill_value=0)
    active   = active.reindex(dates, fill_value=0)
    universe = universe.reindex(dates, fill_value=1)

    turnover = pd.DataFrame({
        "buys":            buys,
        "sells":           sells,
        "active":          active,
        "daily_turnover":  (buys + sells) / universe.clip(lower=1),
    }, index=dates)

    turnover["rolling_turnover"] = (
        turnover["daily_turnover"]
        .rolling(lookback, min_periods=1)
        .mean()
    )

    return turnover


# ═══════════════════════════════════════════════════════════════
#  SUMMARY STATISTICS
# ═══════════════════════════════════════════════════════════════

def signals_summary(signals_df: pd.DataFrame) -> dict:
    """
    Summary statistics for the latest day's signals.

    Returns dict with: date, n_buy, n_hold, n_sell, n_neutral,
    n_active, positions, mean_strength, total_strength,
    strongest, weakest, regime_mix.
    """
    snap = latest_signals(signals_df)
    if snap.empty:
        return {}

    active_snap = snap[snap["signal"].isin([BUY, HOLD])]

    summary: dict = {
        "date":      (
            snap["date"].iloc[0] if "date" in snap.columns
            else None
        ),
        "n_buy":     int((snap["signal"] == BUY).sum()),
        "n_hold":    int((snap["signal"] == HOLD).sum()),
        "n_sell":    int((snap["signal"] == SELL).sum()),
        "n_neutral": int((snap["signal"] == NEUTRAL).sum()),
        "n_active":  len(active_snap),
        "positions": active_snap.index.tolist(),
    }

    if "signal_strength" in snap.columns and not active_snap.empty:
        summary["mean_strength"]  = float(
            active_snap["signal_strength"].mean()
        )
        summary["total_strength"] = float(
            active_snap["signal_strength"].sum()
        )
        summary["strongest"] = active_snap[
            "signal_strength"
        ].idxmax()
        summary["weakest"] = active_snap[
            "signal_strength"
        ].idxmin()

    if "rs_regime" in active_snap.columns and not active_snap.empty:
        summary["regime_mix"] = (
            active_snap["rs_regime"].value_counts().to_dict()
        )

    return summary


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def compute_all_signals(
    ranked: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
    config: SignalConfig | None = None,
) -> pd.DataFrame:
    """
    Full portfolio signal pipeline.

    Parameters
    ----------
    ranked : pd.DataFrame
        Output of ``compute_all_rankings()``, optionally with
        ``sig_confirmed`` columns from ``strategy/signals.py``.
    breadth : pd.DataFrame or None
        Breadth data for circuit breaker and strength scoring.
    config : SignalConfig or None
        Portfolio-level thresholds.

    Returns
    -------
    pd.DataFrame
        MultiIndex (date, ticker) panel with signal, strength,
        eligibility, and position columns appended.
    """
    if config is None:
        config = SignalConfig()

    signals = generate_signals(ranked, breadth, config)
    if signals.empty:
        return pd.DataFrame()

    signals = compute_signal_strength(signals, breadth, config)

    return signals


# ═══════════════════════════════════════════════════════════════
#  TEXT REPORT
# ═══════════════════════════════════════════════════════════════

def signals_report(
    signals_df: pd.DataFrame,
    breadth_regime: str = "unknown",
    breadth_score: float = 0.0,
    config: SignalConfig | None = None,
) -> str:
    """
    Formatted text report of the latest signals.

    Includes: header summary, active positions table, new
    entries, exits with reasons, watchlist, per-ticker gate
    diagnostics (when available), and config reference.
    """
    if config is None:
        config = SignalConfig()

    if signals_df.empty:
        return "No signal data available."

    snap = latest_signals(signals_df)
    if snap.empty:
        return "No signal data available."

    summary = signals_summary(signals_df)
    date_str = (
        summary["date"].strftime("%Y-%m-%d")
        if summary.get("date") else "?"
    )

    # Track whether per-ticker gates are present
    has_gates = "sig_confirmed" in snap.columns

    ln: list[str] = []
    div = "=" * 72
    sub = "-" * 72

    # ── Header ────────────────────────────────────────────
    ln.append(div)
    ln.append(f"TRADE SIGNALS — {date_str}")
    ln.append(div)
    ln.append(
        f"  Breadth:       {breadth_regime} ({breadth_score:.3f})"
    )
    ln.append(
        f"  Positions:     {summary.get('n_active', 0)} / "
        f"{config.max_positions} max"
    )
    ln.append(
        f"  BUY: {summary.get('n_buy', 0)}  "
        f"HOLD: {summary.get('n_hold', 0)}  "
        f"SELL: {summary.get('n_sell', 0)}  "
        f"NEUTRAL: {summary.get('n_neutral', 0)}"
    )
    if summary.get("mean_strength") is not None:
        ln.append(
            f"  Mean strength: {summary['mean_strength']:.3f}"
        )
    ln.append(
        f"  Ticker gates:  "
        f"{'active (strategy/signals.py)' if has_gates else 'fallback (score only)'}"
    )

    # ── Active positions ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("ACTIVE POSITIONS")
    ln.append(sub)

    active = snap[snap["signal"].isin([BUY, HOLD])]
    if active.empty:
        ln.append("  (no active positions)")
    else:
        hdr = (
            f"  {'Signal':<8} {'Ticker':<7} {'#':>3} "
            f"{'Comp':>6} {'Str':>5} {'Regime':<12} "
            f"{'1d':>7} {'5d':>7}"
        )
        if has_gates:
            hdr += f" {'Gates':>5}"
        ln.append(hdr)
        sep_line = (
            f"  {'────────':<8} {'───────':<7} {'───':>3} "
            f"{'──────':>6} {'─────':>5} {'────────────':<12} "
            f"{'───────':>7} {'───────':>7}"
        )
        if has_gates:
            sep_line += f" {'─────':>5}"
        ln.append(sep_line)

        for ticker, row in active.iterrows():
            sig      = row["signal"]
            rank_val = int(row.get("rank", 0))
            comp     = row.get("score_composite", 0)
            strength = row.get("signal_strength", 0)
            regime   = str(row.get("rs_regime", "?"))
            ret_1d   = row.get("ret_1d", np.nan)
            ret_5d   = row.get("ret_5d", np.nan)

            r1 = (
                f"{ret_1d:>+7.1%}" if pd.notna(ret_1d)
                else f"{'—':>7}"
            )
            r5 = (
                f"{ret_5d:>+7.1%}" if pd.notna(ret_5d)
                else f"{'—':>7}"
            )

            line = (
                f"  {sig:<8} {ticker:<7} {rank_val:>3} "
                f"{comp:>6.3f} {strength:>5.3f} {regime:<12} "
                f"{r1} {r5}"
            )
            if has_gates:
                gates_passed = _count_gates(row)
                line += f" {gates_passed:>5}"
            ln.append(line)

    # ── New entries ───────────────────────────────────────
    new_buys = snap[snap["signal"] == BUY]
    if not new_buys.empty:
        ln.append("")
        ln.append(sub)
        ln.append("NEW ENTRIES")
        ln.append(sub)
        for ticker, row in new_buys.iterrows():
            reason = row.get("sig_reason", "")
            ln.append(
                f"  → BUY  {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"strength={row.get('signal_strength', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
                f"{f'  ({reason})' if reason else ''}"
            )

    # ── Exits ─────────────────────────────────────────────
    exits = snap[snap["signal"] == SELL]
    if not exits.empty:
        ln.append("")
        ln.append(sub)
        ln.append("EXITS")
        ln.append(sub)
        for ticker, row in exits.iterrows():
            reasons: list[str] = []
            if row.get("rank", 0) > config.exit_rank_max:
                reasons.append(
                    f"rank {int(row['rank'])} > "
                    f"{config.exit_rank_max}"
                )
            if (
                row.get("score_composite", 1)
                < config.exit_score_min
            ):
                reasons.append(
                    f"score {row['score_composite']:.3f} < "
                    f"{config.exit_score_min:.3f}"
                )
            reason_str = (
                ", ".join(reasons) if reasons else "forced exit"
            )

            ln.append(
                f"  ✕ SELL {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"({reason_str})"
            )

    # ── Gate diagnostics (when strategy/signals.py active) ─
    if has_gates:
        ln.append("")
        ln.append(sub)
        ln.append("PER-TICKER GATE DIAGNOSTICS")
        ln.append(sub)
        gate_cols = [
            ("sig_regime_ok",   "Regime"),
            ("sig_sector_ok",   "Sector"),
            ("sig_breadth_ok",  "Breadth"),
            ("sig_momentum_ok", "Momentum"),
            ("sig_in_cooldown", "Cooldown"),
        ]
        available_gates = [
            (col, label) for col, label in gate_cols
            if col in snap.columns
        ]

        for ticker, row in snap.sort_values("rank").iterrows():
            flags: list[str] = []
            for col, label in available_gates:
                val = row.get(col, None)
                if val is None:
                    continue
                if col == "sig_in_cooldown":
                    flags.append(
                        f"{'✕' if val else '✓'} {label}"
                    )
                else:
                    flags.append(
                        f"{'✓' if val else '✕'} {label}"
                    )

            conf = (
                "CONF" if row.get("sig_confirmed") == 1
                else "—"
            )
            ln.append(
                f"  #{int(row.get('rank', 0)):<3} "
                f"{ticker:<7} [{conf:<4}] "
                f"{'  '.join(flags)}"
            )

    # ── Watchlist ─────────────────────────────────────────
    if "entry_eligible" in snap.columns:
        neutral_elig = snap[
            (snap["signal"] == NEUTRAL) & (snap["entry_eligible"])
        ]
    else:
        neutral_elig = pd.DataFrame()

    if not neutral_elig.empty:
        ln.append("")
        ln.append(sub)
        ln.append("WATCHLIST (eligible but no slot)")
        ln.append(sub)
        for ticker, row in neutral_elig.iterrows():
            ln.append(
                f"  ○ {ticker:<7}  "
                f"#{int(row.get('rank', 0))}  "
                f"score={row.get('score_composite', 0):.3f}  "
                f"{row.get('rs_regime', '?')}"
            )

    # ── Config reference ──────────────────────────────────
    ln.append("")
    ln.append(sub)
    ln.append("SIGNAL CONFIG (portfolio level)")
    ln.append(sub)
    ln.append(
        f"  Entry:  rank ≤ {config.entry_rank_max}"
        + (
            f", sig_confirmed == 1"
            if has_gates
            else f", score ≥ {config.entry_score_min:.2f}"
        )
    )
    ln.append(
        f"  Exit:   rank > {config.exit_rank_max}, "
        f"score < {config.exit_score_min:.2f}"
    )
    ln.append(
        f"  Max positions: {config.max_positions}  "
        f"Breadth bearish: {config.breadth_bearish_action}"
    )

    return "\n".join(ln)


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _count_gates(row: pd.Series) -> str:
    """Count passed / total per-ticker gates for display."""
    gate_cols = [
        "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok",
        "sig_momentum_ok",
    ]
    cooldown_col = "sig_in_cooldown"

    total = 0
    passed = 0

    for col in gate_cols:
        if col in row.index:
            total += 1
            if row[col]:
                passed += 1

    if cooldown_col in row.index:
        total += 1
        if not row[cooldown_col]:
            passed += 1

    return f"{passed}/{total}" if total > 0 else "—"


####################################################
    
"""    
PIPELINE :
----------------------------
"""
"""
pipeline/orchestrator.py
------------------------
Top-level coordinator for the CASH system.

Ties together every phase of analysis into a single call or
a phase-by-phase interactive workflow:

  Phase 0 — Data Loading
      Load OHLCV for all tickers + benchmark from the
      configured data source.

  Phase 1 — Universe-Level Computations
      Breadth indicators, sector relative strength, and
      breadth-to-pillar-score mapping.  These feed into
      the per-ticker pipeline as contextual inputs.

  Phase 2 — Per-Ticker Pipeline
      Run ``runner.run_batch()`` which chains indicators →
      RS → scoring → sector merge → signals for each ticker.

  Phase 3 — Cross-Sectional Analysis
      Rankings across the scored universe, portfolio
      construction with position sizing, and portfolio-level
      signal reconciliation.

  Phase 4 — Reporting
      Generate recommendation report and optional backtest.

The orchestrator can be run end-to-end via
``run_full_pipeline()`` or phase-by-phase for interactive /
notebook use via the ``Orchestrator`` class.

Typical Usage
─────────────
  # One-shot (CLI / cron)
  result = run_full_pipeline(lookback_days=365)

  # Interactive (notebook)
  orch = Orchestrator(lookback_days=180)
  orch.load_data()
  orch.compute_universe_context()
  orch.run_tickers()
  orch.cross_sectional_analysis()
  result = orch.generate_reports()

Dependencies
────────────
  pipeline/runner.py              — single-ticker pipeline
  compute/breadth.py              — universe breadth
  compute/sector_rs.py            — sector RS panel
  output/rankings.py              — cross-sectional rankings
  output/signals.py               — portfolio-level signals
  strategy/portfolio.py           — position sizing & allocation
  portfolio/backtest.py           — historical backtest
  reports/recommendations.py      — ticker recommendations
  ingest/db/loader.py                — OHLCV data loading
  common/config.py                — all parameters
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional
import numpy as np
import pandas as pd

# ── Convergence ───────────────────────────────────────────────
from cash.strategy_phase1.convergence import (
    run_convergence,
    build_price_matrix,
    enrich_snapshots,
    convergence_report,
    MarketSignalResult,
)
from cash.strategy_phase1.rotation import (
    run_rotation,
    RotationConfig,
    RotationResult,
)

# ── Config ────────────────────────────────────────────────────
from common.config import (
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    SECTOR_ETFS,
    TICKER_SECTOR_MAP,
    UNIVERSE,
    MARKET_CONFIG,
    ACTIVE_MARKETS,
)

# ── Compute ───────────────────────────────────────────────────
from cash.compute.breadth import (
    breadth_to_pillar_scores,
    compute_all_breadth,
)
from cash.compute.sector_rs import compute_all_sector_rs

# ── Data loading ──────────────────────────────────────────────
from ingest.db.loader import load_ohlcv, load_universe_ohlcv

# ── Pipeline ──────────────────────────────────────────────────
from cash.pipeline.runner import (
    TickerResult,
    results_errors,
    results_to_scored_universe,
    results_to_snapshots,
    run_batch,
    run_ticker,
)

# ── Output ────────────────────────────────────────────────────
from cash.output.rankings import compute_all_rankings
from cash.output.signals import compute_all_signals

# ── Strategy ──────────────────────────────────────────────────
from cash.strategy_phase1.portfolio import build_portfolio

# ── Portfolio ─────────────────────────────────────────────────
from cash.portfolio.backtest import run_backtest, BacktestConfig

# ── Reports ───────────────────────────────────────────────────
from cash.reports.recommendations import build_report


logger = logging.getLogger(__name__)

# Extra calendar days fetched before the requested window so        # ← NEW
# long-period indicators (200-day MA, etc.) can warm up.            # ← NEW
_DEFAULT_WARMUP_DAYS = 220                                          # ← NEW


# ═══════════════════════════════════════════════════════════════
#  PIPELINE RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class PipelineResult:
    """
    Complete output of a full pipeline run.

    Every downstream consumer (CLI, web dashboard, notebook,
    report generator) reads from this single object.
    """

    ticker_results: dict[str, TickerResult] = field(default_factory=dict)
    scored_universe: dict[str, pd.DataFrame] = field(default_factory=dict)
    snapshots: list[dict] = field(default_factory=list)
    rankings: pd.DataFrame = field(default_factory=pd.DataFrame)
    portfolio: dict = field(default_factory=dict)
    signals: pd.DataFrame = field(default_factory=pd.DataFrame)

    breadth: Optional[pd.DataFrame] = None
    breadth_scores: Optional[pd.DataFrame] = None
    sector_rs: Optional[pd.DataFrame] = None
    bench_df: Optional[pd.DataFrame] = None

    rotation_result: Optional[Any] = None            # RotationResult
    convergence: Optional[Any] = None                # MarketSignalResult
    market: str = "US"
    lookback_days: int = 365                                        # ← NEW

    recommendation_report: Optional[dict] = None
    backtest: Any = None                          # BacktestResult or None

    errors: list[str] = field(default_factory=list)
    timings: dict[str, float] = field(default_factory=dict)
    run_date: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    as_of: Optional[pd.Timestamp] = None

    @property
    def n_tickers(self) -> int:
        return len(self.scored_universe)

    @property
    def n_errors(self) -> int:
        return len(self.errors)

    @property
    def total_time(self) -> float:
        return sum(self.timings.values())

    def top_n(self, n: int = 10) -> list[dict]:
        return self.snapshots[:n]

    def summary(self) -> str:
        top = self.snapshots[0]["ticker"] if self.snapshots else "N/A"
        return (
            f"CASH Pipeline — {self.run_date.strftime('%Y-%m-%d')} — "
            f"{self.n_tickers} tickers scored, "
            f"{self.n_errors} errors, "
            f"top={top}, "
            f"lookback={self.lookback_days}d, "                     # ← NEW
            f"{self.total_time:.1f}s"
        )


# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR CLASS
# ═══════════════════════════════════════════════════════════════

class Orchestrator:
    """
    Stateful pipeline coordinator.

    Use for fine-grained phase-by-phase control (notebooks,
    debugging).  For one-shot usage, prefer ``run_full_pipeline()``.
    """

    def __init__(                                      # ← FIXED: was nested class
        self,
        *,
        market: str = "US",
        universe: list[str] | None = None,
        benchmark: str | None = None,
        capital: float | None = None,
        lookback_days: int | None = None,              # ← NEW
        as_of: pd.Timestamp | None = None,
        enable_breadth: bool = True,
        enable_sectors: bool = True,
        enable_signals: bool = True,
        enable_backtest: bool = False,
    ):
        # ── Market-aware defaults ─────────────────────────────
        self.market: str = market
        mcfg = MARKET_CONFIG.get(market, {})

        self.tickers: list[str] = universe or list(
            mcfg.get("universe", UNIVERSE)
        )
        self.benchmark: str = benchmark or mcfg.get(
            "benchmark", BENCHMARK_TICKER
        )
        self.capital: float = capital or PORTFOLIO_PARAMS["total_capital"]
        self.as_of: pd.Timestamp | None = as_of

        # ── Lookback days ─────────────────────────────────────  # ← NEW
        # None means "load all available data" (no filter).       # ← NEW
        # When set, load_data() fetches lookback_days +           # ← NEW
        # _DEFAULT_WARMUP_DAYS to allow indicator warm-up.        # ← NEW
        self.lookback_days: int | None = lookback_days             # ← NEW

        # Respect market config for feature flags
        self.enable_breadth: bool = (
            enable_breadth
            and mcfg.get("scoring_weights", {}).get(
                "pillar_breadth", 0.10
            ) > 0
        )
        self.enable_sectors: bool = (
            enable_sectors
            and mcfg.get("sector_rs_enabled", True)
        )
        self.enable_signals: bool = enable_signals
        self.enable_backtest: bool = enable_backtest

        # ── Mutable state (populated phase by phase) ──────────
        self._ohlcv: dict[str, pd.DataFrame] = {}
        self._bench_df: pd.DataFrame = pd.DataFrame()
        self._breadth: pd.DataFrame | None = None
        self._breadth_scores: pd.DataFrame | None = None
        self._sector_rs: pd.DataFrame | None = None
        self._ticker_results: dict[str, TickerResult] = {}
        self._scored_universe: dict[str, pd.DataFrame] = {}
        self._snapshots: list[dict] = []
        self._rankings: pd.DataFrame = pd.DataFrame()
        self._portfolio: dict = {}
        self._signals: pd.DataFrame = pd.DataFrame()
        self._recommendation_report: dict | None = None
        self._backtest: Any = None

        self._rotation_result: Any = None           # RotationResult
        self._convergence_result: Any = None        # MarketSignalResult

        self._timings: dict[str, float] = {}
        self._phases_completed: list[str] = []

    # ───────────────────────────────────────────────────────
    #  Phase 0 — Data Loading
    # ───────────────────────────────────────────────────────

    def load_data(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
    ) -> None:
        """
        Load OHLCV data for all tickers and the benchmark.

        When ``self.lookback_days`` is set, requests
        ``lookback_days + _DEFAULT_WARMUP_DAYS`` calendar days
        from the data source so that long-period indicators
        (200-day SMA, etc.) can initialise before the analysis
        window begins.

        Parameters
        ----------
        preloaded : dict, optional
            ``{ticker: OHLCV DataFrame}`` to skip data loading.
        bench_df : pd.DataFrame, optional
            Pre-loaded benchmark OHLCV.
        """
        t0 = time.perf_counter()

        # ── Compute fetch window ──────────────────────────────  # ← NEW
        fetch_days: int | None = None                              # ← NEW
        if self.lookback_days is not None:                         # ← NEW
            fetch_days = self.lookback_days + _DEFAULT_WARMUP_DAYS # ← NEW
            logger.info(                                           # ← NEW
                f"Phase 0: lookback={self.lookback_days}d "        # ← NEW
                f"+ warmup={_DEFAULT_WARMUP_DAYS}d "               # ← NEW
                f"→ fetching {fetch_days}d from source"            # ← NEW
            )                                                      # ← NEW

        if preloaded is not None:
            self._ohlcv = preloaded
            logger.info(
                f"Phase 0: Using {len(preloaded)} pre-loaded "
                f"ticker DataFrames"
            )
        else:
            all_symbols = list(set(self.tickers + [self.benchmark]))
            self._ohlcv = load_universe_ohlcv(                    # ← CHANGED
                all_symbols,
                days=fetch_days,                                   # ← NEW
            )
            logger.info(
                f"Phase 0: Loaded {len(self._ohlcv)} tickers "
                f"from data source"
                + (f" (last {fetch_days} days)"                    # ← NEW
                   if fetch_days else "")                          # ← NEW
            )

        # ── Extract or load benchmark ─────────────────────────
        if bench_df is not None:
            self._bench_df = bench_df
        elif self.benchmark in self._ohlcv:
            self._bench_df = self._ohlcv[self.benchmark]
        else:
            self._bench_df = load_ohlcv(                           # ← CHANGED
                self.benchmark,
                days=fetch_days,                                   # ← NEW
            )

        if self._bench_df.empty:
            raise ValueError(
                f"Benchmark {self.benchmark} has no data. "
                f"Cannot proceed."
            )

        elapsed = time.perf_counter() - t0
        self._timings["load_data"] = elapsed
        self._phases_completed.append("load_data")
        logger.info(
            f"Phase 0 complete: {len(self._ohlcv)} tickers, "
            f"benchmark={self.benchmark} "
            f"({len(self._bench_df)} bars), "
            f"{elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 1 — Universe-Level Context
    # ───────────────────────────────────────────────────────

    def compute_universe_context(self) -> None:
        """
        Compute universe-level breadth and sector RS.

        Breadth feeds Pillar 5 (scoring) and Gate 3 (signals).
        Sector RS feeds sector tailwind adjustments and Gate 2.
        Both are optional — the per-ticker pipeline degrades
        gracefully without them.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        # ── Breadth ───────────────────────────────────────────
        if self.enable_breadth:
            try:
                self._breadth = compute_all_breadth(self._ohlcv)

                if not self._breadth.empty:
                    symbols = list(self._ohlcv.keys())
                    self._breadth_scores = breadth_to_pillar_scores(
                        self._breadth, symbols
                    )
                    logger.info(
                        f"Phase 1: Breadth computed — "
                        f"{len(self._breadth)} bars, "
                        f"regime="
                        f"{self._breadth['breadth_regime'].iloc[-1]}"
                    )
                else:
                    logger.warning(
                        "Phase 1: Breadth returned empty — "
                        "universe may be too small"
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Breadth computation failed — {e}.  "
                    f"Proceeding without breadth context."
                )
                self._breadth = None
                self._breadth_scores = None
        else:
            logger.info("Phase 1: Breadth disabled — skipping")

        # ── Sector RS ─────────────────────────────────────────
        if self.enable_sectors:
            try:
                sector_ohlcv = _extract_sector_ohlcv(self._ohlcv)
                if sector_ohlcv:
                    self._sector_rs = compute_all_sector_rs(
                        sector_ohlcv, self._bench_df
                    )
                    logger.info(
                        f"Phase 1: Sector RS computed — "
                        f"{len(sector_ohlcv)} sectors"
                    )
                else:
                    logger.info(
                        "Phase 1: No sector ETFs found in "
                        "universe — skipping sector RS"
                    )
            except Exception as e:
                logger.warning(
                    f"Phase 1: Sector RS computation failed — "
                    f"{e}.  Proceeding without sector context."
                )
                self._sector_rs = None
        else:
            logger.info("Phase 1: Sectors disabled — skipping")

        elapsed = time.perf_counter() - t0
        self._timings["universe_context"] = elapsed
        self._phases_completed.append("universe_context")
        logger.info(f"Phase 1 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 2 — Per-Ticker Pipeline
    # ───────────────────────────────────────────────────────

    def run_tickers(self) -> None:
        """
        Run the single-ticker pipeline for every ticker.

        Calls ``runner.run_batch()`` which chains:
          indicators → RS → scoring → sector → signals
        for each ticker.
        """
        self._require_phase("load_data")
        t0 = time.perf_counter()

        self._ticker_results = run_batch(
            universe=self._ohlcv,
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores_panel=self._breadth_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
            skip_benchmark=True,
            benchmark_ticker=self.benchmark,
        )

        self._scored_universe = results_to_scored_universe(
            self._ticker_results
        )
        self._snapshots = results_to_snapshots(self._ticker_results)

        elapsed = time.perf_counter() - t0
        self._timings["run_tickers"] = elapsed
        self._phases_completed.append("run_tickers")

        n_ok = len(self._scored_universe)
        n_err = len(results_errors(self._ticker_results))
        logger.info(
            f"Phase 2 complete: {n_ok} scored, "
            f"{n_err} errors, {elapsed:.1f}s"
        )

    # ───────────────────────────────────────────────────────
    #  Phase 2.5 — Rotation Engine  (US only)
    # ───────────────────────────────────────────────────────

    def run_rotation_engine(
        self,
        current_holdings: list[str] | None = None,
        config: RotationConfig | None = None,
    ) -> None:
        """
        Run the top-down sector rotation engine.

        Only meaningful for US — skipped silently for HK/IN.
        Requires Phase 2 (run_tickers) to have completed so
        that OHLCV data is available.

        When Phase 2 produced scored DataFrames, the indicator
        data is passed to the rotation engine's quality filter.
        This gates and scores candidates within leading sectors
        on six technical dimensions (MA structure, RSI, MACD,
        ADX, volume, volatility) and blends the quality score
        with relative strength for final stock ranking.

        When Phase 2 results are empty (or quality is disabled
        in RotationConfig), the rotation engine falls back to
        RS-only ranking — fully backward compatible.
        """
        self._require_phase("run_tickers")

        mcfg = MARKET_CONFIG.get(self.market, {})
        engines = mcfg.get("engines", ["scoring"])

        if "rotation" not in engines:
            logger.info(
                f"Phase 2.5: Rotation not configured for "
                f"{self.market} — skipping"
            )
            return

        t0 = time.perf_counter()

        # Build wide price matrix from loaded OHLCV
        prices = build_price_matrix(self._ohlcv)

        if prices.empty or self.benchmark not in prices.columns:
            logger.warning(
                "Phase 2.5: Cannot build price matrix for "
                "rotation — skipping"
            )
            return

        # ── Build indicator_data from Phase 2 results ─────
        #
        # self._scored_universe is {ticker: DataFrame} where
        # each DataFrame has all indicator columns produced by
        # compute_all_indicators() in the per-ticker pipeline
        # (ema_30, sma_50, rsi_14, adx_14, macd_hist,
        # obv_slope_10d, relative_volume, atr_14_pct, etc.).
        #
        # The rotation engine's quality filter reads these
        # columns to gate and score each candidate stock.
        #
        # Tickers not in this dict (missing data, ETFs not in
        # the scoring universe, failed tickers) receive neutral
        # quality (0.5) and pass the gate by default — so they
        # participate in ranking but don't get the quality bonus.
        indicator_data: dict[str, pd.DataFrame] | None = None

        if self._scored_universe:
            indicator_data = {
                ticker: df
                for ticker, df in self._scored_universe.items()
                if df is not None and not df.empty
            }
            if not indicator_data:
                indicator_data = None

        try:
            r_cfg = config or RotationConfig(
                benchmark=self.benchmark,
            )

            # Log quality filter status
            quality_enabled = r_cfg.quality.enabled
            n_indicator = len(indicator_data) if indicator_data else 0

            if quality_enabled and n_indicator > 0:
                logger.info(
                    f"Phase 2.5: Quality filter ON — "
                    f"{n_indicator} tickers have indicator data"
                )
            elif quality_enabled and n_indicator == 0:
                logger.info(
                    f"Phase 2.5: Quality filter enabled but "
                    f"no indicator data available — "
                    f"falling back to RS-only"
                )
            else:
                logger.info(
                    f"Phase 2.5: Quality filter OFF "
                    f"(disabled in config)"
                )

            self._rotation_result = run_rotation(
                prices=prices,
                current_holdings=current_holdings or [],
                config=r_cfg,
                indicator_data=indicator_data,
            )

            rr = self._rotation_result
            logger.info(
                f"Phase 2.5: Rotation complete — "
                f"{len(rr.buys)} BUY, "
                f"{len(rr.sells)} SELL, "
                f"{len(rr.reduces)} REDUCE, "
                f"{len(rr.holds)} HOLD  |  "
                f"leading={rr.leading_sectors}"
            )

            # Log quality impact on BUY picks
            if quality_enabled and n_indicator > 0:
                self._log_quality_summary(rr)

        except Exception as e:
            logger.warning(
                f"Phase 2.5: Rotation failed — {e}.  "
                f"Proceeding with scoring only."
            )
            self._rotation_result = None

        elapsed = time.perf_counter() - t0
        self._timings["rotation"] = elapsed
        self._phases_completed.append("rotation")


    def _log_quality_summary(self, rr: RotationResult) -> None:
        """
        Log how the quality filter affected BUY recommendations.

        Called from run_rotation_engine() when quality is active.
        """
        buys = rr.buys
        if not buys:
            return

        q_scores = [
            r.quality_score for r in buys
            if r.quality_score > 0
        ]
        gate_fails = [
            r.ticker for r in buys
            if not r.quality_gate_passed
        ]

        if q_scores:
            avg_q = sum(q_scores) / len(q_scores)
            logger.info(
                f"Phase 2.5: Quality summary — "
                f"{len(buys)} BUYs, "
                f"avg quality {avg_q:.2f}, "
                f"range [{min(q_scores):.2f}, "
                f"{max(q_scores):.2f}]"
            )

        if gate_fails:
            logger.info(
                f"Phase 2.5: {len(gate_fails)} BUY(s) had "
                f"gate failures (included via fallback): "
                f"{gate_fails[:5]}"
            )

    # ───────────────────────────────────────────────────────
    #  Phase 2.75 — Convergence Merge
    # ───────────────────────────────────────────────────────

    def apply_convergence(self) -> None:
        """
        Merge scoring + rotation signals via the convergence layer.

        For US:  dual-list merge (scoring + rotation)
        For HK/IN: scoring passthrough

        Updates ``self._snapshots`` with convergence labels and
        adjusted scores so downstream phases (rankings, portfolio,
        reports) benefit from the convergence intelligence.
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        self._convergence_result = run_convergence(
            market=self.market,
            scoring_snapshots=self._snapshots,
            rotation_result=self._rotation_result,
        )

        # Enrich snapshots in-place with convergence data
        enrich_snapshots(self._snapshots, self._convergence_result)

        n_strong = len(self._convergence_result.strong_buys)
        n_conflict = len(self._convergence_result.conflicts)
        logger.info(
            f"Phase 2.75: Convergence applied — "
            f"{self._convergence_result.n_tickers} tickers, "
            f"{n_strong} STRONG_BUY, "
            f"{n_conflict} CONFLICT"
        )

        elapsed = time.perf_counter() - t0
        self._timings["convergence"] = elapsed
        self._phases_completed.append("convergence")

    # ───────────────────────────────────────────────────────
    #  Phase 3 — Cross-Sectional Analysis
    # ───────────────────────────────────────────────────────

    def cross_sectional_analysis(self) -> None:
        """
        Rank the scored universe, build the portfolio, and
        generate portfolio-level signals.

        Sub-phases
        ──────────
        3a. Rankings — cross-sectional rank per date
        3b. Portfolio — position selection + weight allocation
        3c. Signals — BUY/HOLD/SELL with hysteresis
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        if not self._scored_universe:
            logger.warning(
                "Phase 3: No scored tickers — skipping "
                "cross-sectional analysis"
            )
            self._phases_completed.append("cross_sectional")
            return

        # ── 3a. Rankings ──────────────────────────────────────
        try:
            self._rankings = compute_all_rankings(
                self._scored_universe
            )
            n_rows = len(self._rankings)
            logger.info(
                f"Phase 3a: Rankings computed — "
                f"{n_rows} rows"
            )
        except Exception as e:
            logger.warning(f"Phase 3a: Rankings failed — {e}")
            self._rankings = pd.DataFrame()

        # ── 3b. Portfolio Construction ────────────────────────
        try:
            self._portfolio = build_portfolio(
                universe=self._scored_universe,
                breadth=self._breadth,
            )

            n_pos = self._portfolio.get(
                "metadata", {}
            ).get("num_holdings", 0)
            logger.info(
                f"Phase 3b: Portfolio built — "
                f"{n_pos} positions"
            )

            # Enrich orchestrator snapshots with allocation info
            _enrich_snapshots_with_allocations(
                self._snapshots,
                self._portfolio,
                self.capital,
            )
        except Exception as e:
            logger.warning(
                f"Phase 3b: Portfolio build failed — {e}"
            )
            self._portfolio = {}

        # ── 3c. Portfolio-Level Signal Generation ─────────────
        if self.enable_signals and not self._rankings.empty:
            try:
                self._signals = compute_all_signals(
                    ranked=self._rankings,
                    breadth=self._breadth,
                )
                logger.info(
                    f"Phase 3c: Signals generated — "
                    f"{len(self._signals)} rows"
                )

                # Update snapshots with reconciled signals
                _enrich_snapshots_with_signals(
                    self._snapshots, self._signals
                )
            except Exception as e:
                logger.warning(
                    f"Phase 3c: Signal generation failed — {e}"
                )
                self._signals = pd.DataFrame()
        else:
            if not self.enable_signals:
                logger.info(
                    "Phase 3c: Signals disabled — skipping"
                )
            else:
                logger.warning(
                    "Phase 3c: No rankings — cannot generate "
                    "signals"
                )

        elapsed = time.perf_counter() - t0
        self._timings["cross_sectional"] = elapsed
        self._phases_completed.append("cross_sectional")
        logger.info(f"Phase 3 complete: {elapsed:.1f}s")

    # ───────────────────────────────────────────────────────
    #  Phase 4 — Reports & Optional Backtest
    # ───────────────────────────────────────────────────────

    def generate_reports(self) -> PipelineResult:
        """
        Generate reports and assemble the final PipelineResult.

        Returns
        -------
        PipelineResult
        """
        self._require_phase("run_tickers")
        t0 = time.perf_counter()

        # ── Recommendation Report ─────────────────────────────
        try:
            report_input = self._build_report_input()
            self._recommendation_report = build_report(
                report_input
            )
            logger.info("Phase 4: Recommendation report built")
        except Exception as e:
            logger.warning(
                f"Phase 4: Recommendation report failed — {e}"
            )
            self._recommendation_report = None

        # ── Backtest (optional) ───────────────────────────────
        if self.enable_backtest:
            if not self._signals.empty:
                try:
                    bt_config = BacktestConfig(
                        initial_capital=self.capital,
                    )
                    self._backtest = run_backtest(
                        signals_df=self._signals,
                        config=bt_config,
                    )
                    metrics = (
                        self._backtest.metrics
                        if self._backtest else {}
                    )
                    logger.info(
                        f"Phase 4: Backtest complete — "
                        f"CAGR="
                        f"{metrics.get('cagr', 0):.1%}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Phase 4: Backtest failed — {e}"
                    )
            else:
                logger.warning(
                    "Phase 4: Cannot run backtest — "
                    "no signals generated"
                )

        elapsed = time.perf_counter() - t0
        self._timings["reports"] = elapsed
        self._phases_completed.append("reports")

        # ── Assemble PipelineResult ───────────────────────────
        errors = results_errors(self._ticker_results)

        result = PipelineResult(
            ticker_results=self._ticker_results,
            scored_universe=self._scored_universe,
            snapshots=self._snapshots,
            rankings=self._rankings,
            portfolio=self._portfolio,
            signals=self._signals,
            breadth=self._breadth,
            breadth_scores=self._breadth_scores,
            sector_rs=self._sector_rs,
            bench_df=self._bench_df,
            rotation_result=self._rotation_result,
            convergence=self._convergence_result,
            market=self.market,
            lookback_days=self.lookback_days or 0,                 # ← NEW
            recommendation_report=self._recommendation_report,
            backtest=self._backtest,
            errors=errors,
            timings=self._timings,
            run_date=pd.Timestamp.now(),
            as_of=self.as_of,
        )

        logger.info(result.summary())
        return result

    # ───────────────────────────────────────────────────────
    #  Convenience: Run All Phases
    # ───────────────────────────────────────────────────────

    def run_all(
        self,
        preloaded: dict[str, pd.DataFrame] | None = None,
        bench_df: pd.DataFrame | None = None,
        current_holdings: list[str] | None = None,
    ) -> PipelineResult:
        """Execute all phases in sequence."""
        self.load_data(preloaded=preloaded, bench_df=bench_df)
        self.compute_universe_context()
        self.run_tickers()

        self.run_rotation_engine(
            current_holdings=current_holdings,
        )
        self.apply_convergence()

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Re-run Portfolio with Different Parameters
    # ───────────────────────────────────────────────────────

    def rebuild_portfolio(
        self,
        capital: float | None = None,
    ) -> PipelineResult:
        """
        Re-run Phase 3 + 4 without re-computing indicators.
        """
        self._require_phase("run_tickers")

        if capital is not None:
            self.capital = capital

        self._snapshots = results_to_snapshots(
            self._ticker_results
        )
        self._rankings = pd.DataFrame()
        self._portfolio = {}
        self._signals = pd.DataFrame()
        self._recommendation_report = None

        self._phases_completed = [
            p for p in self._phases_completed
            if p in (
                "load_data", "universe_context", "run_tickers"
            )
        ]

        self.cross_sectional_analysis()
        return self.generate_reports()

    # ───────────────────────────────────────────────────────
    #  Single-Ticker Re-run
    # ───────────────────────────────────────────────────────

    def rerun_ticker(self, ticker: str) -> TickerResult:
        """
        Re-run the pipeline for a single ticker.
        """
        self._require_phase("load_data")

        if ticker not in self._ohlcv:
            return TickerResult(
                ticker=ticker,
                error=f"No OHLCV data for {ticker}",
            )

        b_scores = None
        if (
            self._breadth_scores is not None
            and ticker in self._breadth_scores.columns
        ):
            b_scores = self._breadth_scores[ticker]

        result = run_ticker(
            ticker=ticker,
            ohlcv=self._ohlcv[ticker],
            bench_df=self._bench_df,
            breadth=self._breadth,
            breadth_scores=b_scores,
            sector_rs=self._sector_rs,
            as_of=self.as_of,
        )

        self._ticker_results[ticker] = result
        logger.info(
            f"Re-ran {ticker}: "
            f"{'OK' if result.ok else result.error}"
        )
        return result

    # ───────────────────────────────────────────────────────
    #  Report Input Bridge
    # ───────────────────────────────────────────────────────

    def _build_report_input(self) -> dict:
        """
        Construct the dict format that
        ``reports.recommendations.build_report()`` expects.

        Bridges from the orchestrator's internal state
        (PipelineResult-style) to the legacy dict format with
        keys: summary, regime, risk_flags, portfolio_actions,
        ranked_buys, sells, holds, bucket_weights.
        """
        # ── Regime detection ──────────────────────────────
        regime_label, regime_desc = _detect_regime(
            self._bench_df, self._breadth
        )

        spy_close = 0.0
        spy_sma200 = None
        if not self._bench_df.empty:
            spy_close = float(
                self._bench_df["close"].iloc[-1]
            )
            if len(self._bench_df) >= 200:
                sma = self._bench_df["close"].rolling(200).mean()
                spy_sma200 = float(sma.iloc[-1])

        breadth_label = "unknown"
        if (
            self._breadth is not None
            and not self._breadth.empty
            and "breadth_regime" in self._breadth.columns
        ):
            breadth_label = str(
                self._breadth["breadth_regime"].iloc[-1]
            )

        # ── Split snapshots by signal ─────────────────────
        buys = [
            s for s in self._snapshots
            if s.get("signal") == "BUY"
        ]
        sells = [
            s for s in self._snapshots
            if s.get("signal") == "SELL"
        ]
        holds = [
            s for s in self._snapshots
            if s.get("signal") not in ("BUY", "SELL")
        ]

        # Ensure allocation fields default to 0 (not None)
        for s in buys + sells + holds:
            s.setdefault("shares", 0)
            s.setdefault("dollar_alloc", 0)
            s.setdefault("weight_pct", 0)
            s.setdefault("stop_price", None)
            s.setdefault("risk_per_share", None)
            s.setdefault("themes", [])
            s.setdefault("category", "")
            s.setdefault("bucket", "")
            if s["shares"] is None:
                s["shares"] = 0
            if s["dollar_alloc"] is None:
                s["dollar_alloc"] = 0
            if s["weight_pct"] is None:
                s["weight_pct"] = 0

        # ── Summary values ────────────────────────────────
        total_buy = sum(
            s.get("dollar_alloc", 0) or 0 for s in buys
        )
        cash_rem = self.capital - total_buy
        cash_pct = (
            (cash_rem / self.capital * 100)
            if self.capital > 0 else 100
        )

        date = (
            self._snapshots[0]["date"]
            if self._snapshots
            else pd.Timestamp.now()
        )

        # ── Risk flags ────────────────────────────────────
        risk_flags: list[str] = []
        if breadth_label == "weak":
            risk_flags.append(
                "BREADTH_WEAK: Market breadth is weak — "
                "reduced exposure recommended"
            )
        if regime_label in ("bear_mild", "bear_severe"):
            risk_flags.append(
                f"REGIME: {regime_label} — defensive "
                f"positioning recommended"
            )
        if regime_label == "bear_severe":
            risk_flags.append(
                "CIRCUIT_BREAKER: Severe bear — "
                "consider halting new buys"
            )

        # ── Bucket weights from sector exposure ───────────
        bucket_weights: dict[str, float] = {}
        if self._portfolio:
            se = self._portfolio.get("sector_exposure", {})
            meta = self._portfolio.get("metadata", {})
            for sector, weight in se.items():
                bucket_weights[sector] = weight
            bucket_weights["cash"] = meta.get("cash_pct", 0.05)
        else:
            bucket_weights = {
                "core_equity": 0.70,
                "thematic": 0.20,
                "cash": 0.10,
            }

        return {
            "summary": {
                "date":             date,
                "portfolio_value":  self.capital,
                "regime":           regime_label,
                "regime_desc":      regime_desc,
                "spy_close":        spy_close,
                "bucket_breakdown": {},
                "cash_pct":         cash_pct,
                "tickers_analysed": len(self._snapshots),
                "buy_count":        len(buys),
                "sell_count":       len(sells),
                "hold_count":       len(holds),
                "error_count":      len(
                    results_errors(self._ticker_results)
                ),
                "total_buy_dollar": total_buy,
                "cash_remaining":   cash_rem,
            },
            "regime": {
                "label":       regime_label,
                "description": regime_desc,
                "spy_close":   spy_close,
                "spy_sma200":  spy_sma200,
                "breadth":     breadth_label,
            },
            "risk_flags":        risk_flags,
            "portfolio_actions": [],
            "ranked_buys":       buys,
            "sells":             sells,
            "holds":             holds,
            "bucket_weights":    bucket_weights,
        }

    # ───────────────────────────────────────────────────────
    #  Internal Helpers
    # ───────────────────────────────────────────────────────

    def _require_phase(self, phase: str) -> None:
        if phase not in self._phases_completed:
            raise RuntimeError(
                f"Phase '{phase}' has not been run yet.  "
                f"Completed phases: {self._phases_completed}"
            )


# ═══════════════════════════════════════════════════════════════
#  ONE-SHOT ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_full_pipeline(
    *,
    market: str = "US",
    universe: list[str] | None = None,
    benchmark: str | None = None,
    capital: float | None = None,
    lookback_days: int | None = None,                              # ← NEW
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, pd.DataFrame] | None = None,
    bench_df: pd.DataFrame | None = None,
    current_holdings: list[str] | None = None,
    enable_breadth: bool = True,
    enable_sectors: bool = True,
    enable_signals: bool = True,
    enable_backtest: bool = False,
) -> PipelineResult:
    """
    Run the full CASH pipeline end-to-end for one market.

    Parameters
    ----------
    lookback_days : int, optional
        Calendar days of analysis history.  When set, data
        loading fetches ``lookback_days + 220`` days so that
        long-period indicators can warm up.  When ``None``,
        all available data is loaded (original behaviour).

    This is the main entry point for CLI usage and scheduled
    jobs.  For multi-market, use ``run_multi_market_pipeline()``.
    For interactive control, use ``Orchestrator`` directly.
    """
    orch = Orchestrator(
        market=market,
        universe=universe,
        benchmark=benchmark,
        capital=capital,
        lookback_days=lookback_days,                               # ← NEW
        as_of=as_of,
        enable_breadth=enable_breadth,
        enable_sectors=enable_sectors,
        enable_signals=enable_signals,
        enable_backtest=enable_backtest,
    )

    return orch.run_all(
        preloaded=preloaded,
        bench_df=bench_df,
        current_holdings=current_holdings,
    )


# ═══════════════════════════════════════════════════════════════
#  MULTI-MARKET PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_multi_market_pipeline(
    *,
    active_markets: list[str] | None = None,
    capital: float | None = None,
    lookback_days: int | None = None,                              # ← NEW
    as_of: pd.Timestamp | None = None,
    preloaded: dict[str, dict[str, pd.DataFrame]] | None = None,
    current_holdings: dict[str, list[str]] | None = None,
    enable_backtest: bool = False,
) -> dict[str, PipelineResult]:
    """
    Run the full CASH pipeline for every active market.

    Creates a separate Orchestrator per market, each with the
    correct benchmark, universe, and feature flags.

    Parameters
    ----------
    active_markets : list[str], optional
        Markets to run.  Defaults to ``ACTIVE_MARKETS`` from config
        (typically ``["US", "HK", "IN"]``).
    capital : float, optional
        Portfolio value per market.
    lookback_days : int, optional                                  # ← NEW
        Calendar days of analysis history, applied to every       # ← NEW
        market.  When ``None``, all available data is loaded.     # ← NEW
    as_of : pd.Timestamp, optional
        Cut-off date for backtesting.
    preloaded : dict, optional
        ``{market: {ticker: OHLCV DataFrame}}``.  If provided,
        skips data loading for that market.
    current_holdings : dict, optional
        ``{market: [ticker, ...]}``.  Holdings are passed to
        the rotation engine (US) for sell evaluation.
    enable_backtest : bool
        Run historical backtest for each market.

    Returns
    -------
    dict[str, PipelineResult]
        ``{market_code: PipelineResult}`` for each market that
        ran successfully.

    Example
    -------
    ::

        results = run_multi_market_pipeline(lookback_days=365)
        us = results["US"]
        hk = results["HK"]

        for s in us.convergence.strong_buys:
            print(f"{s.ticker}: STRONG BUY, adj={s.adjusted_score:.3f}")
    """
    markets = active_markets or ACTIVE_MARKETS
    results: dict[str, PipelineResult] = {}

    for market in markets:
        mcfg = MARKET_CONFIG.get(market)
        if mcfg is None:
            logger.warning(
                f"Market '{market}' not in MARKET_CONFIG — skipping"
            )
            continue

        logger.info(f"\n{'=' * 60}")
        logger.info(f"  MARKET: {market}")
        logger.info(f"  Benchmark: {mcfg['benchmark']}")
        logger.info(f"  Universe: {len(mcfg['universe'])} tickers")
        logger.info(f"  Engines: {mcfg['engines']}")
        if lookback_days is not None:                              # ← NEW
            logger.info(f"  Lookback: {lookback_days} days")       # ← NEW
        logger.info(f"{'=' * 60}")

        # Pre-loaded data for this market
        pre = (
            preloaded.get(market) if preloaded else None
        )
        holdings = (
            current_holdings.get(market, [])
            if current_holdings else None
        )

        try:
            orch = Orchestrator(
                market=market,
                capital=capital,
                lookback_days=lookback_days,                       # ← NEW
                as_of=as_of,
                enable_backtest=enable_backtest,
            )

            result = orch.run_all(
                preloaded=pre,
                current_holdings=holdings,
            )
            results[market] = result

            logger.info(
                f"[{market}] Pipeline complete: "
                f"{result.n_tickers} tickers, "
                f"{result.n_errors} errors, "
                f"{result.total_time:.1f}s"
            )

            # Log convergence summary
            if result.convergence:
                logger.info(
                    f"[{market}] {result.convergence.summary()}"
                )

        except Exception as e:
            logger.error(
                f"[{market}] Pipeline failed: {e}",
                exc_info=True,
            )

    logger.info(f"\nMulti-market complete: {list(results.keys())}")
    return results


# ═══════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═══════════════════════════════════════════════════════════════

def _detect_regime(
    bench_df: pd.DataFrame,
    breadth: pd.DataFrame | None,
) -> tuple[str, str]:
    """
    Simple market regime detection from benchmark price action
    and breadth state.

    Returns (label, description) where label is one of:
    bull_confirmed, bull_cautious, bear_mild, bear_severe.
    """
    if bench_df is None or bench_df.empty:
        return "bull_cautious", "Insufficient data for regime"

    close = float(bench_df["close"].iloc[-1])

    # Check SPY vs 200-day SMA
    above_sma200 = True
    if len(bench_df) >= 200:
        sma200 = float(
            bench_df["close"].rolling(200).mean().iloc[-1]
        )
        above_sma200 = close > sma200

    # Check breadth regime
    b_regime = "unknown"
    if (
        breadth is not None
        and not breadth.empty
        and "breadth_regime" in breadth.columns
    ):
        b_regime = str(breadth["breadth_regime"].iloc[-1])

    if above_sma200 and b_regime == "strong":
        return (
            "bull_confirmed",
            "SPY above 200d SMA, breadth strong",
        )
    elif above_sma200:
        return (
            "bull_cautious",
            f"SPY above 200d SMA, breadth {b_regime}",
        )
    elif b_regime == "weak":
        return (
            "bear_severe",
            "SPY below 200d SMA, breadth weak",
        )
    else:
        return (
            "bear_mild",
            f"SPY below 200d SMA, breadth {b_regime}",
        )


def _extract_sector_ohlcv(
    ohlcv: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Extract OHLCV for sector ETFs present in the loaded data.

    Returns ``{sector_name: OHLCV DataFrame}`` for sectors
    whose ETF ticker exists in ``ohlcv``.
    """
    sector_data: dict[str, pd.DataFrame] = {}
    for sector_name, etf_ticker in SECTOR_ETFS.items():
        if etf_ticker in ohlcv:
            sector_data[sector_name] = ohlcv[etf_ticker]
    return sector_data


def _enrich_snapshots_with_allocations(
    snapshots: list[dict],
    portfolio: dict,
    capital: float,
) -> None:
    """
    Merge portfolio allocation fields into ticker snapshots.

    ``build_portfolio()`` returns ``target_weights`` as
    ``{ticker: weight_fraction}``.  This function converts
    those weights to dollar allocations and share counts,
    then writes them into the snapshot dicts in-place.

    Tickers not in the portfolio get zero allocations.
    """
    target_weights = portfolio.get("target_weights", {})

    for snap in snapshots:
        ticker = snap["ticker"]
        weight = target_weights.get(ticker, 0.0)

        if weight > 0:
            dollar_alloc = weight * capital
            close = snap.get("close", 0) or 0
            shares = int(dollar_alloc / close) if close > 0 else 0

            snap["weight_pct"] = round(weight * 100, 2)
            snap["dollar_alloc"] = round(dollar_alloc, 2)
            snap["shares"] = shares
            snap["category"] = "selected"
        else:
            snap["weight_pct"] = 0.0
            snap["dollar_alloc"] = 0.0
            snap["shares"] = 0
            snap["category"] = "not_selected"


def _enrich_snapshots_with_signals(
    snapshots: list[dict],
    signals_df: pd.DataFrame,
) -> None:
    """
    Update snapshot ``signal`` field from the portfolio-level
    signals DataFrame.

    ``compute_all_signals()`` returns a MultiIndex (date, ticker)
    panel.  We extract the latest date's signals and overwrite
    the per-ticker signal (from ``strategy/signals.py``) with
    the portfolio-level signal (which incorporates rank
    hysteresis, position limits, and breadth gating).
    """
    if signals_df.empty or "signal" not in signals_df.columns:
        return

    dates = (
        signals_df.index.get_level_values("date")
        .unique()
        .sort_values()
    )
    if len(dates) == 0:
        return

    latest_date = dates[-1]

    try:
        latest = signals_df.xs(latest_date, level="date")
        sig_map = latest["signal"].to_dict()
    except (KeyError, TypeError):
        return

    for snap in snapshots:
        ticker = snap["ticker"]
        if ticker in sig_map:
            snap["signal"] = sig_map[ticker]
