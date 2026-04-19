"""
strategy/signals.py
-------------------
Converts composite scores into actionable trade signals.

Pipeline
────────
  scored DataFrame (from compute layer)
       ↓
  regime_filter()       — is the stock in an allowed regime?
  sector_filter()       — is the sector not blocked?
  momentum_filter()     — is momentum confirming?
       ↓
  raw_signal()          — meets entry / exit thresholds?
       ↓
  confirmed_signal()    — held for N consecutive days?
       ↓
  position_size()       — how much capital to allocate?
       ↓
  generate_signals()    — master function, all of the above

Output columns
──────────────
  sig_regime_ok        bool     stock regime filter passes
  sig_sector_ok        bool     sector regime filter passes
  sig_momentum_ok      bool     momentum pillar confirms
  sig_filters_pass     bool     all three filters pass
  sig_raw              int      raw signal: 1 / 0 / -1
  sig_entry_streak     int      consecutive days above entry threshold
  sig_exit_streak      int      consecutive days below exit threshold
  sig_filter_fail_str  int      consecutive days filters are failing
  sig_confirmed        int      confirmed signal after streak check
  sig_entry_trigger    bool     fresh entry (transition 0 → 1)
  sig_exit_trigger     bool     fresh exit  (transition 1 → 0)
  sig_in_cooldown      bool     within cooldown window after exit
  sig_position_pct     float    suggested position size (0.0–max_pct)
  sig_reason           str      human-readable reason for current signal

All thresholds and sizing rules live in common/config.py → SIGNAL_PARAMS.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SIGNAL_PARAMS


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _p(key: str):
    """Fetch parameter from SIGNAL_PARAMS."""
    return SIGNAL_PARAMS[key]


# ═══════════════════════════════════════════════════════════════
#  INDIVIDUAL FILTERS
# ═══════════════════════════════════════════════════════════════

def _regime_filter(df: pd.DataFrame) -> pd.Series:
    """
    Stock-level regime filter.

    Returns True when rs_regime is in the allowed list
    (default: leading, improving).  A stock in a 'lagging'
    regime should not receive new capital regardless of score.
    """
    allowed = _p("stock_regime_allowed")
    if "rs_regime" in df.columns:
        return df["rs_regime"].isin(allowed)
    # If no RS regime computed, pass everything through
    return pd.Series(True, index=df.index)


def _sector_filter(df: pd.DataFrame) -> pd.Series:
    """
    Sector-level regime filter.

    Returns True when the sector regime is NOT in the blocked
    list.  Avoids allocating to stocks in sectors that are
    actively rotating out, even if the stock itself looks ok.
    """
    blocked = _p("sector_regime_blocked")
    if "sect_rs_regime" in df.columns:
        return ~df["sect_rs_regime"].isin(blocked)
    return pd.Series(True, index=df.index)


def _momentum_filter(df: pd.DataFrame) -> pd.Series:
    """
    Momentum pillar confirmation.

    Requires the momentum sub-score to exceed a minimum.
    Prevents entries where rotation is detected but price
    action hasn't confirmed yet (divergence).
    """
    if not _p("entry_momentum_confirm"):
        return pd.Series(True, index=df.index)

    if "score_momentum" in df.columns:
        return df["score_momentum"] > 0.55
    return pd.Series(True, index=df.index)


# ═══════════════════════════════════════════════════════════════
#  RAW SIGNAL GENERATION
# ═══════════════════════════════════════════════════════════════

def _pick_score_column(df: pd.DataFrame) -> str:
    """Use adjusted score if available, else fall back to composite."""
    if "score_adjusted" in df.columns:
        return "score_adjusted"
    if "score_composite" in df.columns:
        return "score_composite"
    raise ValueError(
        "DataFrame must contain 'score_adjusted' or 'score_composite'."
    )


def _raw_signal(df: pd.DataFrame) -> pd.Series:
    """
    Generate raw signal from score thresholds.

      +1  score ≥ entry_score_min AND percentile ≥ entry_percentile_min
      -1  score < exit_score_below OR percentile < exit_percentile_below
       0  everything else (hold / no action)
    """
    score_col = _pick_score_column(df)
    score = df[score_col].fillna(0)

    pctile = (
        df["score_percentile"].fillna(0.5)
        if "score_percentile" in df.columns
        else pd.Series(0.5, index=df.index)
    )

    entry_cond = (
        (score >= _p("entry_score_min"))
        & (pctile >= _p("entry_percentile_min"))
    )

    exit_cond = (
        (score < _p("exit_score_below"))
        | (pctile < _p("exit_percentile_below"))
    )

    signal = pd.Series(0, index=df.index, dtype=int)
    signal[entry_cond] = 1
    signal[exit_cond] = -1

    return signal


# ═══════════════════════════════════════════════════════════════
#  STREAK COUNTING + CONFIRMATION
# ═══════════════════════════════════════════════════════════════

def _consecutive_streak(condition: pd.Series) -> pd.Series:
    """
    Count consecutive True values ending at each row.

    [F, T, T, T, F, T]  →  [0, 1, 2, 3, 0, 1]

    Used for entry/exit confirmation — signal must persist
    for N days before acting.
    """
    groups = (~condition).cumsum()
    return condition.groupby(groups).cumsum().astype(int)


def _confirmed_signal(
    raw: pd.Series,
    filters_pass: pd.Series,
) -> pd.Series:
    """
    Apply confirmation logic:
      - Entry: raw == 1 AND filters pass for entry_confirm_days
      - Exit:  raw == -1 for exit_confirm_days
               OR filters fail for exit_confirm_days (regime flip)
      - Neutral: everything else → hold previous confirmed state

    Returns Series of 1 (long) or 0 (flat).
    """
    entry_days = _p("entry_confirm_days")
    exit_days  = _p("exit_confirm_days")

    entry_streak = _consecutive_streak((raw == 1) & filters_pass)
    exit_streak  = _consecutive_streak(raw == -1)
    filter_fail_streak = _consecutive_streak(~filters_pass)

    confirmed = pd.Series(0, index=raw.index, dtype=int)

    # Walk forward — hold state, flip on confirmed streaks
    state = 0
    values = confirmed.values
    e_vals = entry_streak.values
    x_vals = exit_streak.values
    ff_vals = filter_fail_streak.values

    for i in range(len(values)):
        if state == 0:
            # Looking for entry
            if e_vals[i] >= entry_days:
                state = 1
        else:
            # Currently long — check exit conditions
            if x_vals[i] >= exit_days:
                # Score dropped below exit threshold
                state = 0
            elif ff_vals[i] >= exit_days:
                # Filters failed (regime flipped) → force exit
                state = 0
            elif e_vals[i] >= entry_days:
                # Still confirmed long
                state = 1
            # else: hold position (state stays 1)

        values[i] = state

    return confirmed


# ═══════════════════════════════════════════════════════════════
#  COOLDOWN
# ═══════════════════════════════════════════════════════════════

def _apply_cooldown(confirmed: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    After an exit (1 → 0), block re-entry for cooldown_days.

    Uses the *original* confirmed signal to detect exits so that
    in-place modifications during cooldown don't mask transitions.

    Returns
    -------
    adjusted : pd.Series   confirmed signal with cooldown applied
    in_cooldown : pd.Series  bool, True during cooldown window
    """
    cooldown = _p("cooldown_days")
    if cooldown <= 0:
        return confirmed.copy(), pd.Series(False, index=confirmed.index)

    original = confirmed.values          # read-only reference
    adjusted = confirmed.values.copy()   # mutable copy
    in_cooldown = np.zeros(len(confirmed), dtype=bool)

    cooldown_remaining = 0

    for i in range(1, len(adjusted)):
        # Detect exit in original signal (1 → 0 transition)
        if original[i] == 0 and original[i - 1] == 1:
            cooldown_remaining = cooldown

        if cooldown_remaining > 0:
            in_cooldown[i] = True
            adjusted[i] = 0         # suppress any re-entry
            cooldown_remaining -= 1

    return (
        pd.Series(adjusted, index=confirmed.index),
        pd.Series(in_cooldown, index=confirmed.index),
    )


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════

def _position_size(df: pd.DataFrame, signal: pd.Series) -> pd.Series:
    """
    Compute position size as a fraction of portfolio.

    Logic
    ─────
    1. Start from base_position_pct.
    2. Scale by (adjusted_score - entry_threshold) × score_scale_factor.
       Higher conviction → larger position.
    3. Apply volatility penalty if score_volatility is low.
    4. Clamp to [min_position_pct, max_position_pct].
    5. Zero out if signal != 1.
    """
    score_col = _pick_score_column(df)
    score = df[score_col].fillna(0)

    base = _p("base_position_pct")
    scale = _p("score_scale_factor")
    entry_min = _p("entry_score_min")
    max_pos = _p("max_position_pct")
    min_pos = _p("min_position_pct")

    # ── Conviction scaling ────────────────────────────────────
    excess = (score - entry_min).clip(lower=0)
    size = base + excess * scale

    # ── Volatility penalty ────────────────────────────────────
    if "score_volatility" in df.columns:
        vol_thresh = _p("vol_penalty_threshold")
        vol_factor = _p("vol_penalty_factor")
        vol_penalty = np.where(
            df["score_volatility"].fillna(0.5) < vol_thresh,
            vol_factor,
            1.0,
        )
        size = size * vol_penalty

    # ── Sector boost ──────────────────────────────────────────
    if "sect_rs_pctrank" in df.columns:
        # Top 3 sectors get a 10-20% size boost
        sect_boost = np.where(
            df["sect_rs_pctrank"].fillna(0.5) >= 0.80,
            1.15,
            np.where(
                df["sect_rs_pctrank"].fillna(0.5) <= 0.20,
                0.85,
                1.00,
            ),
        )
        size = size * sect_boost

    # ── Clamp and zero out non-entries ────────────────────────
    size = size.clip(lower=min_pos, upper=max_pos)
    size = np.where(signal == 1, size, 0.0)

    return pd.Series(size, index=df.index)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL REASON (human-readable)
# ═══════════════════════════════════════════════════════════════

def _signal_reason(df: pd.DataFrame) -> pd.Series:
    """
    Build a short reason string for the current signal state.

    Examples:
      "LONG: score 0.72, tech leading, rank 1"
      "FLAT: score below 0.60 threshold"
      "BLOCKED: sector lagging"
      "COOLDOWN: 3 days remaining"
    """
    score_col = _pick_score_column(df)
    reasons = []

    for i in range(len(df)):
        row = df.iloc[i]
        sig = row.get("sig_confirmed", 0)
        score_val = row.get(score_col, 0)

        if row.get("sig_in_cooldown", False):
            reasons.append("COOLDOWN")
            continue

        if sig == 1:
            parts = [f"LONG: score {score_val:.2f}"]
            regime = row.get("rs_regime", "")
            if regime:
                parts.append(regime)
            sect = row.get("sector_name", "")
            sect_regime = row.get("sect_rs_regime", "")
            if sect and sect_regime:
                parts.append(f"{sect} {sect_regime}")
            rank = row.get("sect_rs_rank", None)
            if rank and not pd.isna(rank):
                parts.append(f"rank {int(rank)}")
            reasons.append(", ".join(parts))
        else:
            # Why flat?
            if not row.get("sig_regime_ok", True):
                reasons.append(
                    f"BLOCKED: stock regime "
                    f"{row.get('rs_regime', '?')}"
                )
            elif not row.get("sig_sector_ok", True):
                reasons.append(
                    f"BLOCKED: sector "
                    f"{row.get('sect_rs_regime', '?')}"
                )
            elif not row.get("sig_momentum_ok", True):
                reasons.append(
                    f"WAIT: momentum {row.get('score_momentum', 0):.2f}"
                )
            elif score_val < _p("entry_score_min"):
                reasons.append(
                    f"FLAT: score {score_val:.2f} "
                    f"< {_p('entry_score_min')}"
                )
            else:
                reasons.append(f"FLAT: score {score_val:.2f}")

    return pd.Series(reasons, index=df.index)


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full signal generation pipeline.

    Parameters
    ----------
    df : pd.DataFrame
        Date-indexed stock data with score columns from
        ``compute_composite_score`` and optionally sector
        columns from ``merge_sector_context``.

    Returns
    -------
    pd.DataFrame
        Input DataFrame with signal columns appended:

        ==================== =========================================
        sig_regime_ok        bool — stock regime filter passes
        sig_sector_ok        bool — sector regime filter passes
        sig_momentum_ok      bool — momentum pillar confirms
        sig_filters_pass     bool — all three filters pass
        sig_raw              int  — raw signal (+1 / 0 / -1)
        sig_entry_streak     int  — consecutive entry-condition days
        sig_exit_streak      int  — consecutive exit-condition days
        sig_filter_fail_str  int  — consecutive days filters failing
        sig_confirmed        int  — signal after confirmation + cooldown
        sig_entry_trigger    bool — fresh entry (0 → 1 transition)
        sig_exit_trigger     bool — fresh exit  (1 → 0 transition)
        sig_in_cooldown      bool — within post-exit cooldown
        sig_position_pct     float — suggested allocation (0–max_pct)
        sig_reason           str  — human-readable explanation
        ==================== =========================================

    Raises
    ------
    ValueError
        If no score column found.
    """
    out = df.copy()

    # ── Filters ───────────────────────────────────────────────
    out["sig_regime_ok"]   = _regime_filter(out)
    out["sig_sector_ok"]   = _sector_filter(out)
    out["sig_momentum_ok"] = _momentum_filter(out)
    out["sig_filters_pass"] = (
        out["sig_regime_ok"]
        & out["sig_sector_ok"]
        & out["sig_momentum_ok"]
    )

    # ── Raw signal ────────────────────────────────────────────
    out["sig_raw"] = _raw_signal(out)

    # ── Streaks ───────────────────────────────────────────────
    out["sig_entry_streak"] = _consecutive_streak(
        (out["sig_raw"] == 1) & out["sig_filters_pass"]
    )
    out["sig_exit_streak"] = _consecutive_streak(
        out["sig_raw"] == -1
    )
    out["sig_filter_fail_str"] = _consecutive_streak(
        ~out["sig_filters_pass"]
    )

    # ── Confirmed signal ──────────────────────────────────────
    confirmed = _confirmed_signal(
        out["sig_raw"], out["sig_filters_pass"]
    )

    # ── Cooldown ──────────────────────────────────────────────
    out["sig_confirmed"], out["sig_in_cooldown"] = _apply_cooldown(
        confirmed
    )

    # ── Entry / exit triggers (transitions) ───────────────────
    prev = out["sig_confirmed"].shift(1).fillna(0).astype(int)
    out["sig_entry_trigger"] = (out["sig_confirmed"] == 1) & (prev == 0)
    out["sig_exit_trigger"]  = (out["sig_confirmed"] == 0) & (prev == 1)

    # ── Position sizing ───────────────────────────────────────
    out["sig_position_pct"] = _position_size(out, out["sig_confirmed"])

    # ── Human-readable reason ─────────────────────────────────
    out["sig_reason"] = _signal_reason(out)

    return out


# ═══════════════════════════════════════════════════════════════
#  SIGNAL SUMMARY — quick stats for a single stock
# ═══════════════════════════════════════════════════════════════

def signal_summary(df: pd.DataFrame) -> dict:
    """
    Quick diagnostic summary of signal history.

    Parameters
    ----------
    df : pd.DataFrame
        Output of :func:`generate_signals`.

    Returns
    -------
    dict with keys:
        total_days, days_long, days_flat, pct_invested,
        num_entries, num_exits, avg_hold_days,
        current_signal, current_reason, current_position_pct,
        current_score, current_regime
    """
    if "sig_confirmed" not in df.columns:
        raise ValueError("Run generate_signals() first.")

    sig = df["sig_confirmed"]
    score_col = _pick_score_column(df)
    total = len(sig.dropna())
    long_days = int((sig == 1).sum())
    entries = int(df["sig_entry_trigger"].sum())
    exits = int(df["sig_exit_trigger"].sum())

    # Average holding period
    if entries > 0:
        avg_hold = long_days / entries
    else:
        avg_hold = 0.0

    last = df.iloc[-1]

    return {
        "total_days":           total,
        "days_long":            long_days,
        "days_flat":            total - long_days,
        "pct_invested":         round(long_days / max(total, 1), 3),
        "num_entries":          entries,
        "num_exits":            exits,
        "avg_hold_days":        round(avg_hold, 1),
        "current_signal":       int(last.get("sig_confirmed", 0)),
        "current_reason":       last.get("sig_reason", ""),
        "current_position_pct": round(last.get("sig_position_pct", 0), 4),
        "current_score":        round(last.get(score_col, 0), 4),
        "current_regime":       last.get("rs_regime", ""),
    }