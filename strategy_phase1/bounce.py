"""
strategy/bounce.py
------------------
Mean-reversion bounce scanner.

Scans the scored universe for "selling exhaustion" setups:
stocks that have drifted lower on declining volume into an
oversold condition, within a longer-term uptrend.

These are NOT momentum entries — they're tactical bounce
trades with a different risk profile:
  • Shorter holding period (days to low single-digit weeks)
  • Tighter stops (below the drift low)
  • Smaller position size (advisory, not auto-allocated)

The scanner is informational.  It produces a watchlist that
sits alongside the main portfolio, not inside it.  The user
decides whether to act on any bounce candidate.

One thing worth noting: the require_above_ma200 filter is the most important safety rail. 
Without it you'll get a lot of structurally broken names that look "oversold" but are really just in a 
downtrend. If you ever want to relax it for more aggressive scanning, I'd suggest replacing it with a 
softer filter like "above 50-day MA" rather than removing it entirely.

Setup anatomy
─────────────
  1. Oversold oscillator   RSI(2) < 10  or  RSI(5) < 25
  2. Volume dry-up         5-day avg vol  <  70% of 20-day avg
  3. Price drift            ≥ 3 consecutive lower closes with
                           compressed daily ranges (not panic)
  4. Trend context         Price above 200-day MA  AND
                           RS regime not "lagging"
  5. Not a falling knife   Drawdown from recent high < 15%
                           (configurable)

Scoring
───────
  Each criterion contributes to a bounce_score (0–1):
    oversold_component    0.30 weight
    volume_component      0.25 weight
    drift_component       0.20 weight
    trend_component       0.15 weight
    proximity_component   0.10 weight  (how close to support)

  Higher bounce_score = more textbook setup.

Usage
─────
  from strategy.bounce import scan_bounce_candidates

  candidates = scan_bounce_candidates(scored_universe)
  print(bounce_report(candidates))
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  DEFAULT PARAMETERS
# ═══════════════════════════════════════════════════════════════

_DEFAULTS: dict[str, float | int] = {
    # ── Oversold thresholds ───────────────────────────────────
    "rsi2_oversold":          10,      # RSI(2) below this
    "rsi5_oversold":          25,      # RSI(5) below this
    "rsi14_max":              40,      # RSI(14) must be below this
                                       # (confirms short-term weakness)

    # ── Volume dry-up ─────────────────────────────────────────
    "vol_ratio_max":          0.70,    # 5d avg vol / 20d avg vol
    "vol_lookback_short":     5,       # short volume window
    "vol_lookback_long":      20,      # long volume window

    # ── Price drift ───────────────────────────────────────────
    "min_consecutive_down":   3,       # min consecutive lower closes
    "max_consecutive_down":   8,       # too many = broken, not drifting
    "atr_compression_max":    0.75,    # recent ATR / 20d ATR ratio
                                       # (< 1.0 = ranges compressing)

    # ── Trend context ─────────────────────────────────────────
    "require_above_ma200":    True,    # price > 200-day MA
    "max_drawdown_pct":       0.15,    # max DD from recent high
    "drawdown_lookback":      60,      # days to look back for high
    "blocked_regimes":        "lagging,weakening",

    # ── Scoring weights ───────────────────────────────────────
    "w_oversold":             0.30,
    "w_volume":               0.25,
    "w_drift":                0.20,
    "w_trend":                0.15,
    "w_proximity":            0.10,

    # ── Output ────────────────────────────────────────────────
    "min_bounce_score":       0.40,    # minimum to qualify
    "max_candidates":         10,      # cap output list
}


def _cfg(key: str, overrides: dict | None = None):
    """Fetch config with optional overrides."""
    if overrides and key in overrides:
        return overrides[key]
    return _DEFAULTS[key]


# ═══════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class BounceCandidate:
    """One ticker's bounce setup assessment."""

    ticker: str
    bounce_score: float          # 0–1 composite

    # ── Components ────────────────────────────────────────────
    oversold_score: float        # 0–1
    volume_score: float          # 0–1
    drift_score: float           # 0–1
    trend_score: float           # 0–1
    proximity_score: float       # 0–1

    # ── Raw metrics ───────────────────────────────────────────
    rsi2: float
    rsi5: float
    rsi14: float
    vol_ratio: float             # 5d / 20d volume ratio
    consecutive_down: int        # days of consecutive lower closes
    atr_ratio: float             # recent ATR / 20d ATR
    pct_from_high: float         # drawdown from lookback high
    pct_above_ma200: float       # distance above 200-MA (neg = below)
    rs_regime: str
    sector_name: str | None
    last_close: float

    # ── Context ───────────────────────────────────────────────
    setup_quality: str           # "A" / "B" / "C"
    notes: list[str] = field(default_factory=list)
    rank: int = 0

    def to_dict(self) -> dict:
        return {
            "ticker":           self.ticker,
            "bounce_score":     round(self.bounce_score, 4),
            "setup_quality":    self.setup_quality,
            "rsi2":             round(self.rsi2, 1),
            "rsi5":             round(self.rsi5, 1),
            "rsi14":            round(self.rsi14, 1),
            "vol_ratio":        round(self.vol_ratio, 2),
            "consecutive_down": self.consecutive_down,
            "atr_ratio":        round(self.atr_ratio, 2),
            "pct_from_high":    round(self.pct_from_high, 4),
            "pct_above_ma200":  round(self.pct_above_ma200, 4),
            "rs_regime":        self.rs_regime,
            "sector_name":      self.sector_name,
            "last_close":       round(self.last_close, 2),
            "rank":             self.rank,
            "notes":            "; ".join(self.notes),
        }


@dataclass
class BounceScanResult:
    """Complete bounce scan output."""

    candidates: list[BounceCandidate] = field(default_factory=list)
    scanned: int = 0
    passed_trend: int = 0
    passed_oversold: int = 0

    @property
    def n_candidates(self) -> int:
        return len(self.candidates)

    def to_dataframe(self) -> pd.DataFrame:
        if not self.candidates:
            return pd.DataFrame()
        return pd.DataFrame([c.to_dict() for c in self.candidates])

    def summary(self) -> str:
        return (
            f"Bounce scan: {self.scanned} scanned → "
            f"{self.passed_trend} passed trend filter → "
            f"{self.passed_oversold} oversold → "
            f"{self.n_candidates} candidates"
        )


# ═══════════════════════════════════════════════════════════════
#  RSI CALCULATOR  (standalone, short-period)
# ═══════════════════════════════════════════════════════════════

def _rsi(closes: pd.Series, period: int) -> float:
    """
    Compute RSI for the last value of a close series.

    Uses exponential (Wilder) smoothing.  Returns NaN if
    insufficient data.
    """
    if len(closes) < period + 1:
        return np.nan

    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period).mean()

    last_gain = avg_gain.iloc[-1]
    last_loss = avg_loss.iloc[-1]

    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return 100.0 - (100.0 / (1.0 + rs))


# ═══════════════════════════════════════════════════════════════
#  PER-TICKER ANALYSIS
# ═══════════════════════════════════════════════════════════════

def _count_consecutive_down(closes: pd.Series) -> int:
    """Count consecutive lower closes from the most recent bar."""
    if len(closes) < 2:
        return 0
    diffs = closes.diff().iloc[1:]   # skip first NaN
    count = 0
    for val in reversed(diffs.values):
        if val < 0:
            count += 1
        else:
            break
    return count


def _analyse_ticker(
    ticker: str,
    df: pd.DataFrame,
    params: dict | None = None,
) -> BounceCandidate | None:
    """
    Analyse a single ticker for bounce setup.

    Returns a BounceCandidate if the ticker passes all hard
    filters, otherwise None.
    """
    if df is None or len(df) < 200:
        return None

    # ── Extract series ────────────────────────────────────────
    close = df["close"] if "close" in df.columns else None
    volume = df["volume"] if "volume" in df.columns else None
    high = df["high"] if "high" in df.columns else None
    low = df["low"] if "low" in df.columns else None

    if close is None or volume is None:
        return None

    last_close = float(close.iloc[-1])
    if last_close <= 0 or np.isnan(last_close):
        return None

    notes: list[str] = []

    # ══════════════════════════════════════════════════════════
    #  HARD FILTERS  (must pass all to be considered)
    # ══════════════════════════════════════════════════════════

    # ── 1. Trend context: above 200-MA ────────────────────────
    ma200 = close.rolling(200).mean().iloc[-1]
    if np.isnan(ma200) or ma200 <= 0:
        return None

    pct_above_ma200 = (last_close - ma200) / ma200

    if _cfg("require_above_ma200", params) and pct_above_ma200 < 0:
        return None

    # ── 2. Trend context: RS regime not blocked ───────────────
    rs_regime = str(df["rs_regime"].iloc[-1]) if "rs_regime" in df.columns else "unknown"
    blocked = [
        r.strip()
        for r in str(_cfg("blocked_regimes", params)).split(",")
    ]
    if rs_regime.lower() in blocked:
        return None

    # ── 3. Drawdown check (not a falling knife) ──────────────
    dd_lookback = int(_cfg("drawdown_lookback", params))
    recent_high = close.iloc[-dd_lookback:].max()
    pct_from_high = (last_close - recent_high) / recent_high

    if abs(pct_from_high) > _cfg("max_drawdown_pct", params):
        return None

    # ── 4. Oversold: at least one RSI threshold met ──────────
    rsi2_val = _rsi(close, 2)
    rsi5_val = _rsi(close, 5)
    rsi14_val = _rsi(close, 14)

    if np.isnan(rsi2_val):
        rsi2_val = 50.0
    if np.isnan(rsi5_val):
        rsi5_val = 50.0
    if np.isnan(rsi14_val):
        rsi14_val = 50.0

    rsi2_oversold = rsi2_val < _cfg("rsi2_oversold", params)
    rsi5_oversold = rsi5_val < _cfg("rsi5_oversold", params)
    rsi14_ok = rsi14_val < _cfg("rsi14_max", params)

    if not (rsi2_oversold or rsi5_oversold):
        return None

    if not rsi14_ok:
        return None

    # ══════════════════════════════════════════════════════════
    #  SOFT METRICS  (contribute to score)
    # ══════════════════════════════════════════════════════════

    # ── Volume dry-up ─────────────────────────────────────────
    vol_short = int(_cfg("vol_lookback_short", params))
    vol_long = int(_cfg("vol_lookback_long", params))

    avg_vol_short = volume.iloc[-vol_short:].mean()
    avg_vol_long = volume.iloc[-vol_long:].mean()

    if avg_vol_long > 0:
        vol_ratio = avg_vol_short / avg_vol_long
    else:
        vol_ratio = 1.0

    # ── Consecutive down days ─────────────────────────────────
    consec_down = _count_consecutive_down(close)

    min_down = int(_cfg("min_consecutive_down", params))
    max_down = int(_cfg("max_consecutive_down", params))

    # ── ATR compression ───────────────────────────────────────
    if high is not None and low is not None:
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)

        atr5 = tr.iloc[-5:].mean()
        atr20 = tr.iloc[-20:].mean()
        atr_ratio = atr5 / atr20 if atr20 > 0 else 1.0
    else:
        atr_ratio = 1.0

    # ══════════════════════════════════════════════════════════
    #  COMPONENT SCORES
    # ══════════════════════════════════════════════════════════

    # ── Oversold (0–1): lower RSI = higher score ──────────────
    # RSI(2) maps: 0 → 1.0, 10 → 0.5, 30 → 0.0
    os_rsi2 = np.clip(1.0 - (rsi2_val / 30.0), 0.0, 1.0)
    # RSI(5) maps: 0 → 1.0, 25 → 0.5, 50 → 0.0
    os_rsi5 = np.clip(1.0 - (rsi5_val / 50.0), 0.0, 1.0)
    oversold_score = max(os_rsi2, os_rsi5)

    if rsi2_oversold and rsi5_oversold:
        oversold_score = min(1.0, oversold_score * 1.15)
        notes.append("both RSI(2) and RSI(5) oversold")

    # ── Volume (0–1): lower ratio = higher score ──────────────
    # ratio 0.3 → 1.0, ratio 0.7 → 0.5, ratio 1.0+ → 0.0
    volume_score = np.clip((1.0 - vol_ratio) / 0.7, 0.0, 1.0)

    if vol_ratio < _cfg("vol_ratio_max", params):
        notes.append(f"volume dry-up ({vol_ratio:.0%} of normal)")

    # ── Drift (0–1): right number of down days + compressed
    #    ranges = higher score ─────────────────────────────────
    if consec_down >= min_down:
        # Sweet spot is 3–5 days; too many is suspicious
        days_score = np.clip(
            (consec_down - min_down + 1) / (max_down - min_down + 1),
            0.0, 1.0,
        )
        # Penalise if too many
        if consec_down > max_down:
            days_score *= 0.5
            notes.append(f"{consec_down} down days (extended)")
        else:
            notes.append(f"{consec_down} consecutive down days")
    else:
        days_score = consec_down / max(min_down, 1)

    # ATR compression bonus
    atr_comp_max = float(_cfg("atr_compression_max", params))
    if atr_ratio < atr_comp_max:
        atr_score = np.clip(
            (atr_comp_max - atr_ratio) / atr_comp_max, 0.0, 1.0
        )
        notes.append(f"ATR compressing ({atr_ratio:.2f}x)")
    else:
        atr_score = 0.0

    drift_score = float(np.clip(
        0.6 * days_score + 0.4 * atr_score, 0.0, 1.0
    ))

    # ── Trend (0–1): how healthy is the bigger picture ────────
    # Distance above MA200: 0% → 0.3, 5% → 0.7, 10%+ → 1.0
    ma_score = np.clip(pct_above_ma200 / 0.10, 0.0, 1.0)

    # RS regime bonus
    regime_scores = {
        "leading": 1.0, "improving": 0.8,
        "neutral": 0.5, "weakening": 0.2, "lagging": 0.0,
    }
    regime_score = regime_scores.get(rs_regime.lower(), 0.4)

    trend_score = float(np.clip(
        0.5 * ma_score + 0.5 * regime_score, 0.0, 1.0
    ))

    # ── Proximity (0–1): how close to a potential support ─────
    # Use distance from 60-day low as a proxy for support
    low_60 = close.iloc[-60:].min()
    if low_60 > 0:
        pct_from_support = (last_close - low_60) / low_60
        # At the low → 1.0, 5% above → 0.5, 10%+ above → 0.0
        proximity_score = float(np.clip(
            1.0 - (pct_from_support / 0.10), 0.0, 1.0
        ))
    else:
        proximity_score = 0.0

    # ══════════════════════════════════════════════════════════
    #  COMPOSITE BOUNCE SCORE
    # ══════════════════════════════════════════════════════════

    w_os = float(_cfg("w_oversold", params))
    w_vol = float(_cfg("w_volume", params))
    w_dft = float(_cfg("w_drift", params))
    w_trd = float(_cfg("w_trend", params))
    w_prx = float(_cfg("w_proximity", params))

    bounce_score = float(np.clip(
        w_os * oversold_score
        + w_vol * volume_score
        + w_dft * drift_score
        + w_trd * trend_score
        + w_prx * proximity_score,
        0.0, 1.0,
    ))

    # ── Minimum score gate ────────────────────────────────────
    if bounce_score < _cfg("min_bounce_score", params):
        return None

    # ── Setup quality grade ───────────────────────────────────
    if bounce_score >= 0.75 and vol_ratio < 0.6 and consec_down >= 3:
        quality = "A"
    elif bounce_score >= 0.55:
        quality = "B"
    else:
        quality = "C"

    # ── Sector ────────────────────────────────────────────────
    sector = None
    if "sector_name" in df.columns:
        s = df["sector_name"].iloc[-1]
        if pd.notna(s):
            sector = str(s)

    return BounceCandidate(
        ticker=ticker,
        bounce_score=bounce_score,
        oversold_score=oversold_score,
        volume_score=volume_score,
        drift_score=drift_score,
        trend_score=trend_score,
        proximity_score=proximity_score,
        rsi2=rsi2_val,
        rsi5=rsi5_val,
        rsi14=rsi14_val,
        vol_ratio=vol_ratio,
        consecutive_down=consec_down,
        atr_ratio=atr_ratio,
        pct_from_high=pct_from_high,
        pct_above_ma200=pct_above_ma200,
        rs_regime=rs_regime,
        sector_name=sector,
        last_close=last_close,
        setup_quality=quality,
        notes=notes,
    )


# ═══════════════════════════════════════════════════════════════
#  MAIN SCANNER
# ═══════════════════════════════════════════════════════════════

def scan_bounce_candidates(
    scored_universe: dict[str, pd.DataFrame],
    params: dict | None = None,
) -> BounceScanResult:
    """
    Scan the scored universe for bounce setups.

    Parameters
    ----------
    scored_universe : dict[str, pd.DataFrame]
        {ticker: DataFrame} — same structure passed to
        ``build_portfolio()``.  Each DataFrame must have
        at least: close, volume, high, low columns.
    params : dict, optional
        Override any default parameter.

    Returns
    -------
    BounceScanResult
        Ranked list of bounce candidates.
    """
    result = BounceScanResult()
    result.scanned = len(scored_universe)

    candidates: list[BounceCandidate] = []

    for ticker, df in scored_universe.items():
        if df is None or df.empty:
            continue

        candidate = _analyse_ticker(ticker, df, params)

        if candidate is not None:
            candidates.append(candidate)

    # ── Sort by bounce_score descending, cap output ───────────
    candidates.sort(key=lambda c: c.bounce_score, reverse=True)

    max_out = int(_cfg("max_candidates", params))
    candidates = candidates[:max_out]

    for i, c in enumerate(candidates, 1):
        c.rank = i

    result.candidates = candidates
    result.passed_oversold = len(candidates)

    logger.info(result.summary())
    return result


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def bounce_report(result: BounceScanResult) -> str:
    """Format bounce scan results as a human-readable report."""
    ln: list[str] = []
    w = 72
    div = "=" * w
    sub = "-" * w

    ln.append(div)
    ln.append("  BOUNCE SCANNER — Selling Exhaustion Setups")
    ln.append(div)
    ln.append(f"  {result.summary()}")
    ln.append("")

    if not result.candidates:
        ln.append("  No bounce setups detected today.")
        ln.append(div)
        return "\n".join(ln)

    # ── Grade A setups ────────────────────────────────────────
    grade_a = [c for c in result.candidates if c.setup_quality == "A"]
    if grade_a:
        ln.append(sub)
        ln.append(f"  ⚡ GRADE A — Textbook Setups  ({len(grade_a)})")
        ln.append(
            f"  Oversold + volume dry-up + drift confirmed"
        )
        ln.append(sub)
        for c in grade_a:
            ln.append(_fmt_bounce(c))
        ln.append("")

    # ── Grade B setups ────────────────────────────────────────
    grade_b = [c for c in result.candidates if c.setup_quality == "B"]
    if grade_b:
        ln.append(sub)
        ln.append(f"  🔶 GRADE B — Developing Setups  ({len(grade_b)})")
        ln.append(
            f"  Most criteria met, one or two soft"
        )
        ln.append(sub)
        for c in grade_b:
            ln.append(_fmt_bounce(c))
        ln.append("")

    # ── Grade C setups ────────────────────────────────────────
    grade_c = [c for c in result.candidates if c.setup_quality == "C"]
    if grade_c:
        ln.append(sub)
        ln.append(f"  ⚪ GRADE C — Early / Marginal  ({len(grade_c)})")
        ln.append(sub)
        for c in grade_c:
            ln.append(_fmt_bounce(c))
        ln.append("")

    ln.append(div)
    return "\n".join(ln)


def _fmt_bounce(c: BounceCandidate) -> str:
    """Format one BounceCandidate for display."""
    lines = [
        f"    #{c.rank}  {c.ticker:<8s}  "
        f"bounce={c.bounce_score:.2f}  "
        f"close={c.last_close:.2f}  "
        f"regime={c.rs_regime}"
    ]
    lines.append(
        f"         RSI(2)={c.rsi2:>5.1f}  "
        f"RSI(5)={c.rsi5:>5.1f}  "
        f"RSI(14)={c.rsi14:>5.1f}  "
        f"vol={c.vol_ratio:.0%} of avg  "
        f"down={c.consecutive_down}d  "
        f"ATR={c.atr_ratio:.2f}x"
    )
    lines.append(
        f"         from_high={c.pct_from_high:+.1%}  "
        f"vs_MA200={c.pct_above_ma200:+.1%}"
    )
    if c.sector_name:
        lines.append(f"         sector={c.sector_name}")
    if c.notes:
        lines.append(f"         → {'; '.join(c.notes)}")
    return "\n".join(lines)