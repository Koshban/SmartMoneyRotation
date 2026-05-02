"""
strategy/signals.py
-------------------
Entry / exit signal generation.

Takes a scored, RS-enriched DataFrame for a single ticker and
produces a column set that tells the portfolio builder what to do.

Signals are *gated*: every candidate must pass a checklist of
conditions before it earns a ``sig_confirmed == 1`` flag.

Gates
─────
  1.  rs_regime       ∈  allowed_rs_regimes       (stock trend)
  1b. rsi_14          ∈  [rsi_entry_min, rsi_entry_max]  (RSI range)
  2.  sect_rs_regime  ∈  allowed_sector_regimes   (sector tide)
  3.  breadth_regime  ∈  allowed_breadth_regimes  (market gate)
  4.  momentum_streak ≥  N consecutive days > 0.5 (persistence)
  5.  NOT in cooldown after recent exit            (anti-churn)
  6.  score_adjusted  ≥  entry_score_min           (quality bar)

Pipeline
────────
  scored_df  →  _gate_regime()
             →  _gate_rsi()          ← NEW: hard RSI 30-70 gate
             →  _gate_sector()
             →  _gate_breadth()
             →  _gate_momentum()
             →  _gate_cooldown()
             →  _gate_entry()
             →  _compute_exits()
             →  _position_sizing()
             →  generate_signals()   ← master function

Each gate adds its own boolean column so downstream analytics
can diagnose *why* a signal was or wasn't generated.

This is the per-ticker quality filter. It runs on a single
ticker's time series and answers "is this ticker trade-worthy
today?" through seven gates (regime, RSI, sector, breadth,
momentum, cooldown, score). Its output is sig_confirmed,
sig_exit, and all the sig_* diagnostic columns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SIGNAL_PARAMS, BREADTH_PORTFOLIO


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _sp(key: str):
    return SIGNAL_PARAMS[key]


def _bpp(key: str):
    return BREADTH_PORTFOLIO[key]


# ═══════════════════════════════════════════════════════════════
#  GATE 1 — STOCK RS REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_regime(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_rs_regimes")

    if "rs_regime" in result.columns:
        result["sig_regime_ok"] = result["rs_regime"].isin(allowed)
    else:
        result["sig_regime_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 1b — RSI RANGE
# ═══════════════════════════════════════════════════════════════

def _gate_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hard RSI gate: only allow entries when RSI is between 30 and 70.

    RSI < 30  → oversold collapse in progress, not momentum
    RSI > 70  → overbought, high risk of mean reversion

    This is a HARD gate — no BUY signal is generated outside
    this range regardless of how strong other indicators are.

    The thresholds are configured via SIGNAL_PARAMS:
      - rsi_entry_min  (default 30)
      - rsi_entry_max  (default 70)
    """
    result = df.copy()
    rsi_min = _sp("rsi_entry_min")
    rsi_max = _sp("rsi_entry_max")

    # The RSI column name from compute/indicators.py
    rsi_col = "rsi_14"

    if rsi_col in result.columns:
        rsi = result[rsi_col].fillna(50.0)
        result["sig_rsi_ok"] = (rsi >= rsi_min) & (rsi <= rsi_max)
    else:
        # No RSI data — pass by default (degrade gracefully)
        result["sig_rsi_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 2 — SECTOR REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_sector(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_sector_regimes")

    if "sect_rs_regime" in result.columns:
        result["sig_sector_ok"] = (
            result["sect_rs_regime"].isin(allowed)
        )
    else:
        result["sig_sector_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 3 — BREADTH REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_breadth(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Market-level breadth gate.

    When breadth data is provided, this gate:
      - Merges breadth_regime and breadth_score onto the
        ticker's DataFrame by date.
      - Sets ``sig_breadth_ok`` = True when breadth_regime
        is NOT weak (or when ``weak_block_new`` is False).
      - Adjusts the effective entry threshold upward in
        neutral / weak regimes.

    Without breadth data the gate passes unconditionally.
    """
    result = df.copy()

    if breadth is None or breadth.empty:
        result["sig_breadth_ok"]       = True
        result["breadth_regime"]       = "unknown"
        result["breadth_score"]        = np.nan
        result["entry_score_adj"]      = 0.0
        return result

    # ── Merge breadth onto ticker dates ───────────────────────
    breadth_cols = ["breadth_regime", "breadth_score", "breadth_score_smooth"]
    available    = [c for c in breadth_cols if c in breadth.columns]

    if not available:
        result["sig_breadth_ok"]  = True
        result["breadth_regime"]  = "unknown"
        result["breadth_score"]   = np.nan
        result["entry_score_adj"] = 0.0
        return result

    bdata = breadth[available].copy()

    # Align on date index
    result = result.join(bdata, how="left")

    # Forward-fill breadth regime for any gaps
    if "breadth_regime" in result.columns:
        result["breadth_regime"] = (
            result["breadth_regime"].ffill().fillna("unknown")
        )
    else:
        result["breadth_regime"] = "unknown"

    if "breadth_score" in result.columns:
        result["breadth_score"] = result["breadth_score"].ffill()

    # ── Set gate ──────────────────────────────────────────────
    block_new = _bpp("weak_block_new")

    if block_new:
        result["sig_breadth_ok"] = (
            result["breadth_regime"] != "weak"
        )
    else:
        result["sig_breadth_ok"] = True

    # ── Entry threshold adjustment ────────────────────────────
    weak_raise    = _bpp("weak_raise_entry")
    neutral_raise = _bpp("neutral_raise_entry")

    conditions = [
        result["breadth_regime"] == "weak",
        result["breadth_regime"] == "neutral",
    ]
    choices = [weak_raise, neutral_raise]

    result["entry_score_adj"] = np.select(
        conditions, choices, default=0.0
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 4 — MOMENTUM PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def _gate_momentum(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    streak = _sp("confirmation_streak")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_momentum_ok"] = False
        return result

    above = result[score_col] >= 0.50
    cumsum = above.cumsum()
    reset  = cumsum - cumsum.where(~above).ffill().fillna(0)

    result["sig_momentum_streak"] = reset.astype(int)
    result["sig_momentum_ok"]     = reset >= streak

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 5 — COOLDOWN
# ═══════════════════════════════════════════════════════════════

def _gate_cooldown(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    cooldown = _sp("cooldown_days")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_in_cooldown"] = False
        return result

    exit_thresh = _sp("exit_score_max")

    was_above = (result[score_col].shift(1) >= exit_thresh)
    now_below = (result[score_col] < exit_thresh)
    exit_event = was_above & now_below

    cooldown_remaining = pd.Series(0, index=result.index, dtype=int)
    counter = 0
    for i in range(len(result)):
        if exit_event.iloc[i]:
            counter = cooldown
        elif counter > 0:
            counter -= 1
        cooldown_remaining.iloc[i] = counter

    result["sig_in_cooldown"]       = cooldown_remaining > 0
    result["sig_cooldown_remaining"] = cooldown_remaining

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 6 — ENTRY CONFIRMATION
# ═══════════════════════════════════════════════════════════════

def _gate_entry(df: pd.DataFrame) -> pd.DataFrame:
    result    = df.copy()
    base_min  = _sp("entry_score_min")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_confirmed"] = 0
        result["sig_reason"]    = "no score"
        return result

    # ── Effective entry threshold (breadth-adjusted) ──────────
    entry_adj = result.get("entry_score_adj", 0.0)
    if isinstance(entry_adj, (int, float)):
        entry_adj = pd.Series(entry_adj, index=result.index)
    effective_min = base_min + entry_adj.fillna(0.0)

    result["sig_effective_entry_min"] = effective_min

    scores = result[score_col]

    # All gates must pass (RSI gate included)
    regime_ok   = result.get("sig_regime_ok", True)
    rsi_ok      = result.get("sig_rsi_ok", True)
    sector_ok   = result.get("sig_sector_ok", True)
    breadth_ok  = result.get("sig_breadth_ok", True)
    momentum_ok = result.get("sig_momentum_ok", False)
    cooldown    = result.get("sig_in_cooldown", False)

    confirmed = (
        (scores >= effective_min)
        & regime_ok
        & rsi_ok
        & sector_ok
        & breadth_ok
        & momentum_ok
        & (~cooldown)
    )

    result["sig_confirmed"] = confirmed.astype(int)

    # ── Reason annotation ─────────────────────────────────────
    # Priority order: RSI → regime → sector → breadth → cooldown
    #                 → momentum → score → fallback
    reasons = pd.Series("", index=result.index)

    reasons = reasons.where(
        confirmed,
        np.where(
            ~(rsi_ok.astype(bool) if not isinstance(
                rsi_ok, bool
            ) else rsi_ok),
            "rsi_out_of_range",
            np.where(
                ~regime_ok.astype(bool) if not isinstance(
                    regime_ok, bool
                ) else ~regime_ok,
                "regime_blocked",
                np.where(
                    ~sector_ok.astype(bool) if not isinstance(
                        sector_ok, bool
                    ) else ~sector_ok,
                    "sector_blocked",
                    np.where(
                        ~breadth_ok.astype(bool) if not isinstance(
                            breadth_ok, bool
                        ) else ~breadth_ok,
                        "breadth_weak",
                        np.where(
                            cooldown.astype(bool) if not isinstance(
                                cooldown, bool
                            ) else cooldown,
                            "cooldown",
                            np.where(
                                ~momentum_ok.astype(bool) if not isinstance(
                                    momentum_ok, bool
                                ) else ~momentum_ok,
                                "momentum_unconfirmed",
                                np.where(
                                    scores < effective_min,
                                    "score_below_entry",
                                    "no_signal",
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    reasons = reasons.where(~confirmed, "LONG")
    result["sig_reason"] = reasons

    return result


# ═══════════════════════════════════════════════════════════════
#  EXIT SIGNALS
# ═══════════════════════════════════════════════════════════════

def _compute_exits(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    exit_max = _sp("exit_score_max")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_exit"] = 0
        return result

    was_confirmed = result["sig_confirmed"].shift(1).fillna(0) == 1
    now_weak      = result[score_col] < exit_max

    result["sig_exit"] = (was_confirmed & now_weak).astype(int)

    # Also flag if breadth turned weak on an existing position
    if "breadth_regime" in result.columns:
        breadth_was_ok  = result["sig_breadth_ok"].shift(1, fill_value=True)
        breadth_now_bad = result["breadth_regime"] == "weak"
        result["sig_exit_breadth"] = (
            was_confirmed & breadth_now_bad & breadth_was_ok
        ).astype(int)
    else:
        result["sig_exit_breadth"] = 0

    return result


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════

def _position_sizing(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    base     = _sp("base_position_pct")
    max_pos  = _sp("max_position_pct")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_position_pct"] = 0.0
        return result

    scores = result[score_col].fillna(0)

    # Scale linearly from base → max as score goes 0.6 → 1.0
    low, high = 0.60, 1.0
    frac = ((scores - low) / (high - low)).clip(0, 1)
    raw  = base + frac * (max_pos - base)

    # ── Breadth-based scaling ─────────────────────────────────
    # In neutral/weak breadth, scale down position sizes
    if "breadth_regime" in result.columns:
        breadth_scale = result["breadth_regime"].map({
            "strong":  1.0,
            "neutral": _bpp("neutral_exposure"),
            "weak":    _bpp("weak_exposure"),
        }).fillna(1.0)
    else:
        breadth_scale = 1.0

    raw = raw * breadth_scale

    result["sig_position_pct"] = np.where(
        result["sig_confirmed"] == 1, raw, 0.0,
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Run the full signal pipeline on a single ticker's scored
    DataFrame.

    Gates (all must pass for sig_confirmed = 1):
      1.  RS regime        — stock in allowed regime
      1b. RSI range        — RSI between 30 and 70
      2.  Sector regime    — sector tide favourable
      3.  Breadth regime   — market not weak
      4.  Momentum streak  — N consecutive days above threshold
      5.  Cooldown         — not recently exited
      6.  Entry score      — composite above threshold

    Parameters
    ----------
    df : pd.DataFrame
        Output of the scoring pipeline, with RS and sector RS
        columns already merged.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()``.  When provided,
        breadth regime gates and position-size scaling are active.

    Returns
    -------
    pd.DataFrame
        Original columns plus all ``sig_*`` columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    result = _gate_regime(df)
    result = _gate_rsi(result)
    result = _gate_sector(result)
    result = _gate_breadth(result, breadth)
    result = _gate_momentum(result)
    result = _gate_cooldown(result)
    result = _gate_entry(result)
    result = _compute_exits(result)
    result = _position_sizing(result)

    return result