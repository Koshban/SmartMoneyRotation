"""
refactor/strategy/adapters_v2.py

Column contract and default-value guarantees for the v2 pipeline.

Every module in the pipeline reads columns from DataFrames that may or
may not have been populated by an earlier stage.  ``ensure_columns``
is called at key checkpoints to guarantee that every expected column
exists with a sensible typed default, so that downstream code can
safely do ``row.get("sectrsregime")`` or ``df["scorerotation"]``
without guarding against KeyError.

The column spec is defined in a single ``COLUMN_SPEC`` dict, making
it trivial to audit every column the pipeline touches, its expected
type, and its neutral default.

Usage
-----
The pipeline calls ``ensure_columns(df)`` after indicator computation
and again after the leadership snapshot merge.  It is idempotent:
columns that already exist and contain real values are never
overwritten — only genuinely missing columns are created, and only
NaN cells within existing columns are backfilled with the neutral
default.

Design rationale
----------------
- **Single source of truth.**  Every column name, default, and dtype
  lives in one place.  Adding a new column to the pipeline means
  adding one entry to ``COLUMN_SPEC``.

- **Neutral defaults, not optimistic ones.**  Missing RSI defaults to
  50 (midpoint), missing regime to ``"unknown"``, missing score to
  0.0 (not 0.5) — because a missing score should rank last, while a
  missing indicator should be non-committal.

- **No silent type coercion on existing data.**  If ``rsi14`` is
  already a float column, ``ensure_columns`` will only fill NaN cells,
  never re-cast the entire series.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Column specification
# ═══════════════════════════════════════════════════════════════════════════════
#
# Format:  column_name → (default_value, dtype_string)
#
# dtype_string is one of:
#   "float"   → np.float64
#   "int"     → int (via .astype(int) after fillna)
#   "str"     → object / string
#   "bool"    → bool
#
# The default value is used both to create missing columns and to fill
# NaN cells within existing columns.  For "str" columns, NaN cells are
# filled with the string default (e.g. "unknown"), not with np.nan.

COLUMN_SPEC: dict[str, tuple] = {
    # ── OHLCV basics ──────────────────────────────────────────────────────
    "open":                     (0.0,       "float"),
    "high":                     (0.0,       "float"),
    "low":                      (0.0,       "float"),
    "close":                    (0.0,       "float"),
    "volume":                   (0.0,       "float"),

    # ── Trend indicators ──────────────────────────────────────────────────
    "rsi14":                    (50.0,      "float"),
    "adx14":                    (20.0,      "float"),
    "ema30":                    (0.0,       "float"),
    "sma30":                    (0.0,       "float"),
    "sma50":                    (0.0,       "float"),
    "closevsema30pct":          (0.0,       "float"),
    "closevssma50pct":          (0.0,       "float"),
    "macdline":                 (0.0,       "float"),
    "macdsignal":               (0.0,       "float"),
    "macdhist":                 (0.0,       "float"),

    # ── Volatility / risk indicators ──────────────────────────────────────
    "atr14":                    (0.0,       "float"),
    "atr14pct":                 (0.03,      "float"),
    "realizedvol20d":           (0.25,      "float"),
    "realizedvol20dchg5":       (0.0,       "float"),
    "gaprate20":                (0.15,      "float"),

    # ── Volume indicators ─────────────────────────────────────────────────
    "relativevolume":           (1.0,       "float"),
    "volumeavg20":              (0.0,       "float"),
    "dollarvolumeavg20":        (0.0,       "float"),
    "dollarvolume20d":          (0.0,       "float"),

    # ── Relative strength (cross-sectional) ───────────────────────────────
    "rszscore":                 (0.0,       "float"),
    "rsaccel20":                (0.0,       "float"),
    "rsregime":                 ("unknown", "str"),

    # ── Sector rotation ───────────────────────────────────────────────────
    "sectrsregime":             ("unknown", "str"),

    # ── Breadth & dispersion ──────────────────────────────────────────────
    "breadthregime":            ("unknown", "str"),
    "breadthscore":             (0.5,       "float"),
    "dispersion20":             (None,      "float"),
    "dispersion":               (None,      "float"),

    # ── Volatility regime ─────────────────────────────────────────────────
    "volregime":                ("calm",    "str"),
    "volregimescore":           (0.5,       "float"),

    # ── Classification / grouping ─────────────────────────────────────────
    "sector":                   ("Unknown", "str"),
    "theme":                    ("Unknown", "str"),
    "instrument_type":          ("stock",   "str"),

    # ── Leadership ────────────────────────────────────────────────────────
    "leadership_strength":      (0.0,       "float"),

    # ── Scoring sub-components ────────────────────────────────────────────
    "scoretrend":               (0.0,       "float"),
    "scoreparticipation":       (0.0,       "float"),
    "scorerisk":                (0.0,       "float"),
    "scoreregime":              (0.0,       "float"),
    "scorerotation":            (0.0,       "float"),
    "scorepenalty":             (0.0,       "float"),
    "scorecomposite_v2":        (0.0,       "float"),

    # ── Signals ───────────────────────────────────────────────────────────
    "sigconfirmed_v2":          (0,         "int"),
    "sigexit_v2":               (0,         "int"),
    "sigeffectiveentrymin_v2":  (0.60,      "float"),

    # ── Convergence ───────────────────────────────────────────────────────
    "scoreadjusted_v2":         (0.0,       "float"),

    # ── Actions ───────────────────────────────────────────────────────────
    "action_v2":                ("SELL",    "str"),
    "conviction_v2":            ("low",     "str"),
    "action_reason_v2":         ("",        "str"),
    "action_sort_key_v2":       (0.0,       "float"),
    "score_percentile_v2":      (0.0,       "float"),

    # ── Scoreability annotations ──────────────────────────────────────────
    "scoreable_v2":             (True,      "bool"),
    "missing_critical_count_v2": (0,        "int"),
    "missing_critical_fields_v2": ("",      "str"),
    "scoreability_reason_v2":   ("ok",      "str"),

    # ── Portfolio ─────────────────────────────────────────────────────────
    "portfolio_weight":         (0.0,       "float"),
    "portfolio_weight_pct":     (0.0,       "float"),
    "selection_rank":           (0,         "int"),
    "effective_sector_cap":     (0.0,       "float"),
    "selection_reason":         ("",        "str"),
}


# Columns that ensure_columns should create if missing but should
# NOT backfill NaN cells — because NaN has legitimate meaning (e.g.
# "we could not compute dispersion" is different from "dispersion = 0").
_NO_BACKFILL: frozenset[str] = frozenset({
    "dispersion20",
    "dispersion",
})


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_columns(
    df: pd.DataFrame,
    subset: list[str] | None = None,
) -> pd.DataFrame:
    """
    Guarantee that expected columns exist with typed defaults.

    Parameters
    ----------
    df : DataFrame
        Input frame (modified in-place on a copy).
    subset : list of str, optional
        If provided, only ensure these column names (must be keys in
        ``COLUMN_SPEC``).  If ``None``, ensures **all** columns in the
        spec.  Passing a subset is useful in tight loops where only a
        handful of columns are needed and the full spec would be
        wasteful.

    Returns
    -------
    DataFrame
        Same data with missing columns added and NaN cells backfilled.
    """
    if df.empty:
        return df

    out = df.copy()
    columns_to_check = subset if subset is not None else list(COLUMN_SPEC.keys())

    created: list[str] = []
    backfilled: list[str] = []

    for col in columns_to_check:
        if col not in COLUMN_SPEC:
            continue

        default, dtype_str = COLUMN_SPEC[col]

        if col not in out.columns:
            # Create column with the default value
            if dtype_str == "float":
                if default is None:
                    out[col] = pd.Series(np.nan, index=out.index, dtype=float)
                else:
                    out[col] = pd.Series(
                        float(default), index=out.index, dtype=float,
                    )
            elif dtype_str == "int":
                out[col] = pd.Series(
                    int(default), index=out.index, dtype=int,
                )
            elif dtype_str == "bool":
                out[col] = pd.Series(
                    bool(default), index=out.index, dtype=bool,
                )
            else:  # str / object
                out[col] = pd.Series(
                    str(default) if default is not None else "",
                    index=out.index,
                    dtype=object,
                )
            created.append(col)
            continue

        # Column exists — optionally backfill NaN cells
        if col in _NO_BACKFILL:
            continue

        if default is None:
            continue

        if dtype_str == "float":
            mask = out[col].isna()
            if mask.any():
                out.loc[mask, col] = float(default)
                backfilled.append(col)
        elif dtype_str == "int":
            mask = out[col].isna()
            if mask.any():
                out[col] = out[col].fillna(int(default))
                backfilled.append(col)
        elif dtype_str == "bool":
            mask = out[col].isna()
            if mask.any():
                out[col] = out[col].fillna(bool(default))
                backfilled.append(col)
        else:  # str / object
            # For string columns, also treat empty strings and literal
            # "nan" as missing.
            mask = (
                out[col].isna()
                | out[col].astype(str).str.lower().isin(["nan", "none", ""])
            )
            if mask.any():
                out.loc[mask, col] = str(default)
                backfilled.append(col)

    if created or backfilled:
        logger.debug(
            "ensure_columns: created=%d (%s)  backfilled=%d (%s)",
            len(created),
            ", ".join(created[:15]) + ("..." if len(created) > 15 else ""),
            len(backfilled),
            ", ".join(backfilled[:15]) + ("..." if len(backfilled) > 15 else ""),
        )

    return out


# ═══════════════════════════════════════════════════════════════════════════════
# Utilities for other modules
# ═══════════════════════════════════════════════════════════════════════════════

def get_column_default(col: str):
    """
    Return the neutral default for a column, or None if not in the spec.
    """
    entry = COLUMN_SPEC.get(col)
    return entry[0] if entry is not None else None


def get_column_dtype(col: str) -> str | None:
    """
    Return the dtype string for a column, or None if not in the spec.
    """
    entry = COLUMN_SPEC.get(col)
    return entry[1] if entry is not None else None


def list_columns(category: str | None = None) -> list[str]:
    """
    List column names, optionally filtered by a prefix-based category.

    Categories (prefix matching):
        "score"     → scoring sub-components and composite
        "sig"       → signal columns
        "action"    → action columns
        "breadth"   → breadth-related
        "vol"       → volatility regime
        "rs"        → relative strength
        "sect"      → sector rotation
        "portfolio" → portfolio columns

    If *category* is None, returns all columns.
    """
    if category is None:
        return list(COLUMN_SPEC.keys())
    prefix = category.lower()
    return [k for k in COLUMN_SPEC if k.lower().startswith(prefix)]


def validate_frame(
    df: pd.DataFrame,
    required: list[str] | None = None,
) -> dict:
    """
    Check a DataFrame against the column spec and return a diagnostic
    dict.

    Parameters
    ----------
    df : DataFrame
        Frame to validate.
    required : list of str, optional
        Columns that must be present and non-null in the last row.
        Defaults to all columns in ``COLUMN_SPEC``.

    Returns
    -------
    dict
        Keys: ``ok`` (bool), ``missing_columns`` (list),
        ``null_in_last_row`` (list), ``total_checked`` (int).
    """
    cols_to_check = required if required is not None else list(COLUMN_SPEC.keys())

    missing_cols = [c for c in cols_to_check if c not in df.columns]
    null_last = []

    if not df.empty:
        last = df.iloc[-1]
        for c in cols_to_check:
            if c in df.columns:
                val = last.get(c)
                if val is None:
                    null_last.append(c)
                elif isinstance(val, float) and np.isnan(val):
                    # Allow NaN for no-backfill columns
                    if c not in _NO_BACKFILL:
                        null_last.append(c)

    ok = len(missing_cols) == 0 and len(null_last) == 0
    return {
        "ok": ok,
        "missing_columns": missing_cols,
        "null_in_last_row": null_last,
        "total_checked": len(cols_to_check),
    }

####################################################
"""
refactor/strategy/breadth_v2.py

Cross-sectional market-breadth computation and regime classification.

Builds a date x symbol close-price panel from the full universe and
computes the following daily cross-sectional metrics:

    pct_above_sma20   - fraction of symbols above their 20-day SMA
    pct_above_sma50   - fraction above 50-day SMA
    pct_above_sma200  - fraction above 200-day SMA
    pct_advancing     - fraction with a positive daily return
    net_new_highs_pct - (20-d new highs - 20-d new lows) / universe size
    dispersion_daily  - cross-sectional std-dev of daily returns
    dispersion20      - 20-day rolling mean of dispersion_daily

A weighted composite ``breadthscore`` (0-1) is EMA-smoothed and
classified into one of four regimes:

    strong   - breadthscore >= regime_strong
    moderate - regime_moderate <= breadthscore < regime_strong
    weak     - regime_weak <= breadthscore < regime_moderate
    critical - breadthscore < regime_weak

All thresholds and weights are configurable via params dict.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Module-level defaults (used when no params dict is supplied) ──────────────
BREADTH_MIN_SYMBOLS = 5
BREADTH_MIN_HISTORY = 55
BREADTH_EMA_SPAN = 5

REGIME_STRONG = 0.65
REGIME_MODERATE = 0.45
REGIME_WEAK = 0.25

DEFAULT_COMPOSITE_WEIGHTS = {
    "pct_above_sma50": 0.30,
    "pct_above_sma200": 0.20,
    "pct_above_sma20": 0.15,
    "pct_advancing": 0.15,
    "net_new_highs": 0.20,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def compute_breadth(
    symbol_frames: dict[str, pd.DataFrame],
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Compute market breadth from the universe of symbol frames.

    Parameters
    ----------
    symbol_frames : dict[str, DataFrame]
        Mapping of ticker -> OHLCV-style DataFrame (must contain ``close``).
    params : dict, optional
        Configuration overrides.  Recognised keys:
            min_symbols, min_history, ema_span,
            regime_strong, regime_moderate, regime_weak,
            composite_weights (sub-dict with pct_above_sma50, etc.)

    Returns
    -------
    DataFrame
        Indexed by date with breadth metrics, ``breadthscore``, and
        ``breadthregime``.  Empty if fewer than ``min_symbols``
        symbols qualify.
    """
    if not symbol_frames:
        logger.warning("compute_breadth: no symbol frames provided")
        return pd.DataFrame()

    # ── unpack config (FIX 8: every threshold from params) ────────────────────
    p = params or {}
    min_symbols = p.get("min_symbols", BREADTH_MIN_SYMBOLS)
    min_history = p.get("min_history", BREADTH_MIN_HISTORY)
    ema_span = p.get("ema_span", BREADTH_EMA_SPAN)
    regime_strong = p.get("regime_strong", REGIME_STRONG)
    regime_moderate = p.get("regime_moderate", REGIME_MODERATE)
    regime_weak = p.get("regime_weak", REGIME_WEAK)

    cw = p.get("composite_weights", DEFAULT_COMPOSITE_WEIGHTS)
    w_sma50 = cw.get("pct_above_sma50", 0.30)
    w_sma200 = cw.get("pct_above_sma200", 0.20)
    w_sma20 = cw.get("pct_above_sma20", 0.15)
    w_adv = cw.get("pct_advancing", 0.15)
    w_highs = cw.get("net_new_highs", 0.20)

    logger.info(
        "compute_breadth params: min_symbols=%d min_history=%d ema_span=%d "
        "regime_thresholds=(%.2f/%.2f/%.2f) "
        "weights=(sma50=%.2f sma200=%.2f sma20=%.2f adv=%.2f highs=%.2f)",
        min_symbols, min_history, ema_span,
        regime_strong, regime_moderate, regime_weak,
        w_sma50, w_sma200, w_sma20, w_adv, w_highs,
    )

    # ── 1. close-price panel ──────────────────────────────────────────────────
    close_dict: dict[str, pd.Series] = {}
    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        if len(df) < min_history:
            continue
        close_dict[ticker] = pd.to_numeric(df["close"], errors="coerce")

    if len(close_dict) < min_symbols:
        logger.warning(
            "compute_breadth: only %d symbols with >= %d rows (need >= %d)",
            len(close_dict), min_history, min_symbols,
        )
        return pd.DataFrame()

    close = pd.DataFrame(close_dict)
    n_valid = close.notna().sum(axis=1).replace(0, np.nan)
    logger.info(
        "compute_breadth: panel shape=(%d dates, %d symbols)",
        close.shape[0], close.shape[1],
    )

    # ── 2. moving-average breadth ─────────────────────────────────────────────
    sma20 = close.rolling(20, min_periods=15).mean()
    sma50 = close.rolling(50, min_periods=40).mean()
    sma200 = close.rolling(200, min_periods=150).mean()

    pct_above_sma20 = (close > sma20).sum(axis=1) / n_valid
    pct_above_sma50 = (close > sma50).sum(axis=1) / n_valid
    pct_above_sma200 = (close > sma200).sum(axis=1) / n_valid

    # ── 3. advance / decline ──────────────────────────────────────────────────
    daily_ret = close.pct_change(fill_method=None)
    pct_advancing = (daily_ret > 0).sum(axis=1) / n_valid

    # ── 4. new-high / new-low (20-day rolling) ───────────────────────────────
    high20 = close.rolling(20, min_periods=15).max()
    low20 = close.rolling(20, min_periods=15).min()
    at_high = (close >= high20 - 1e-8).sum(axis=1) / n_valid
    at_low = (close <= low20 + 1e-8).sum(axis=1) / n_valid
    net_new_highs_pct = at_high - at_low

    # ── 5. cross-sectional dispersion ─────────────────────────────────────────
    dispersion_daily = daily_ret.std(axis=1, ddof=1)
    dispersion20 = dispersion_daily.rolling(20, min_periods=15).mean()

    # ── 6. composite score (0-1) — all weights from config ────────────────────
    raw_score = (
        w_sma50 * pct_above_sma50.fillna(0.5)
        + w_sma200 * pct_above_sma200.fillna(0.5)
        + w_sma20 * pct_above_sma20.fillna(0.5)
        + w_adv * pct_advancing.fillna(0.5)
        + w_highs * ((net_new_highs_pct.fillna(0.0) + 1.0) / 2.0)
    ).clip(0.0, 1.0)

    breadthscore = raw_score.ewm(
        span=ema_span, min_periods=max(3, ema_span // 2),
    ).mean()

    # ── 7. regime classification — all thresholds from config ─────────────────
    classification = pd.Series(
        np.select(
            [
                breadthscore >= regime_strong,
                breadthscore >= regime_moderate,
                breadthscore >= regime_weak,
                breadthscore < regime_weak,
            ],
            ["strong", "moderate", "weak", "critical"],
            default="unknown",
        ),
        index=close.index,
    )
    breadthregime = classification.where(breadthscore.notna(), "unknown")

    # ── 8. assemble output ────────────────────────────────────────────────────
    breadth = pd.DataFrame(
        {
            "pct_above_sma20": pct_above_sma20,
            "pct_above_sma50": pct_above_sma50,
            "pct_above_sma200": pct_above_sma200,
            "pct_advancing": pct_advancing,
            "pct_new_high_20": at_high,
            "pct_new_low_20": at_low,
            "net_new_highs_pct": net_new_highs_pct,
            "dispersion_daily": dispersion_daily,
            "dispersion20": dispersion20,
            "breadthscore_raw": raw_score,
            "breadthscore": breadthscore,
            "breadthregime": breadthregime,
        },
        index=close.index,
    )

    # ── 9. diagnostics ───────────────────────────────────────────────────────
    if not breadth.empty:
        last = breadth.iloc[-1]
        logger.info(
            "compute_breadth last-date: score=%.4f regime=%s "
            "above_sma50=%.1f%% above_sma200=%.1f%% advancing=%.1f%% "
            "net_highs=%.1f%% dispersion20=%.6f",
            float(last.get("breadthscore", 0)),
            str(last.get("breadthregime", "unknown")),
            float(last.get("pct_above_sma50", 0)) * 100,
            float(last.get("pct_above_sma200", 0)) * 100,
            float(last.get("pct_advancing", 0)) * 100,
            float(last.get("net_new_highs_pct", 0)) * 100,
            float(last.get("dispersion20", 0)),
        )
        recent_regimes = breadth["breadthregime"].tail(20).value_counts().to_dict()
        logger.info("compute_breadth last-20-day regime dist: %s", recent_regimes)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "compute_breadth tail(5):\n%s",
                breadth[
                    ["breadthscore", "breadthregime", "pct_above_sma50",
                     "pct_advancing", "dispersion20"]
                ].tail(5).to_string(),
            )

    return breadth

###################################################
"""refactor/strategy/enrich_v2.py – Enrich scored tickers with blended rotation data.

Revision notes
--------------
- **Compressed regime scores**: lagging raised from 0.00 → 0.15,
  weakening from 0.30 → 0.40, improving from 0.65 → 0.70.
  This reduces the punitive spread while still meaningfully
  differentiating sectors.
- **Incremental composite update**: instead of recomputing the full
  composite from sub-scores (which silently drops the leadership
  boost applied in pipeline_v2), only the rotation delta is applied.
  This preserves all prior adjustments and keeps the enrichment
  delta small and predictable.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import pandas as pd

from common.sector_map import get_sector_or_class

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  REGIME → scorerotation mapping
# ═══════════════════════════════════════════════════════════════
#  Old behaviour: leading=1.0, everything else=0.0
#  V2 behaviour:  graded so "improving" and "weakening" get partial
#                 credit; lagging has a non-zero floor so the
#                 rotation penalty doesn't dominate the composite.
#
#  These values are intentionally aligned with the initial rotation
#  scores set in scoring_v2.py so that the enrichment delta is
#  small (dominated by the ETF composite boost, not a regime
#  reclassification).

DEFAULT_REGIME_SCORES: dict[str, float] = {
    "leading":    1.00,
    "improving":  0.70,       # ← CHANGED from 0.65
    "weakening":  0.40,       # ← CHANGED from 0.30
    "lagging":    0.15,       # ← CHANGED from 0.00
    "unknown":    0.30,       # ← CHANGED from 0.15
}


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  Sector lookup helper
# ═══════════════════════════════════════════════════════════════

def _resolve_sector(row: dict | pd.Series, ticker: str) -> str:
    """
    Try multiple column names to find the sector for a ticker row.
    Falls back to the common.sector_map lookup.
    """
    for col in ("sector", "gics_sector", "industry_sector", "sector_name"):
        val = row.get(col) if isinstance(row, dict) else getattr(row, col, None)
        if val is not None and str(val).strip() and str(val).strip().lower() != "nan":
            return str(val).strip()
    # Fallback to static map
    mapped = get_sector_or_class(ticker)
    if mapped:
        return mapped
    return "Unknown"


# ═══════════════════════════════════════════════════════════════
#  ETF composite boost
# ═══════════════════════════════════════════════════════════════

def _compute_etf_boost(
    sector: str,
    sector_summary: pd.DataFrame,
    etf_ranking: pd.DataFrame,
) -> float:
    """
    Optional small boost/penalty from the ETF composite score for the
    sector's canonical ETF.  Returns a value in [-0.10, +0.10] that
    can be added to scorerotation.

    If the sector ETF composite is above the universe median → positive.
    If below → negative.  Magnitude scaled by distance from median.
    """
    if sector_summary is None or sector_summary.empty:
        return 0.0

    # Find this sector's row in sector_summary
    match = sector_summary[sector_summary["sector"] == sector]
    if match.empty:
        return 0.0

    etf_composite = _safe_float(match.iloc[0].get("etf_composite"), 0.5)

    # Universe median from etf_ranking
    if etf_ranking is not None and not etf_ranking.empty and "etf_composite" in etf_ranking.columns:
        median = etf_ranking["etf_composite"].median()
    else:
        median = 0.50

    # Scale: distance from median, capped at ±0.10
    raw = (etf_composite - median) * 0.50  # half the distance
    return max(-0.10, min(0.10, raw))


# ═══════════════════════════════════════════════════════════════
#  Main enrichment function
# ═══════════════════════════════════════════════════════════════

def enrich_with_rotation(
    scored_df: pd.DataFrame,
    rotation_result: dict[str, Any],
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Enrich a scored DataFrame with blended sector rotation data.

    Reads from rotation_result:
      - sector_regimes   : {sector_name: regime_str}
      - ticker_regimes   : {ticker: regime_str}
      - sector_summary   : DataFrame with per-sector detail
      - etf_ranking      : DataFrame with per-ETF scores

    Updates / adds columns on scored_df:
      - rotation_regime      : str  ("leading", "improving", …)
      - scorerotation        : float (graded 0.0–1.0, replaces old binary)
      - etf_boost            : float (±0.10 from ETF composite vs median)
      - rotation_blended     : float (blended score from sector_summary)
      - scorecomposite_v2    : float (incrementally updated)

    Returns the enriched DataFrame (modified in place for efficiency,
    but also returned for chaining).
    """
    params = params or {}
    regime_scores = params.get("regime_scores", DEFAULT_REGIME_SCORES)
    apply_etf_boost = params.get("apply_etf_boost", True)
    recompute_composite = params.get("recompute_composite", True)

    sector_regimes: dict[str, str] = rotation_result.get("sector_regimes", {})
    ticker_regimes: dict[str, str] = rotation_result.get("ticker_regimes", {})
    sector_summary: pd.DataFrame = rotation_result.get("sector_summary", pd.DataFrame())
    etf_ranking: pd.DataFrame = rotation_result.get("etf_ranking", pd.DataFrame())

    if scored_df is None or scored_df.empty:
        logger.warning("enrich_with_rotation: scored_df is empty, nothing to enrich")
        return scored_df

    n = len(scored_df)
    ticker_col = None
    for col in ("ticker", "symbol"):
        if col in scored_df.columns:
            ticker_col = col
            break
    if ticker_col is None:
        logger.warning("enrich_with_rotation: no ticker column found in scored_df")
        return scored_df

    # ── Save old scorerotation for incremental update ─────────────────── # ← CHANGED
    if "scorerotation" in scored_df.columns:
        old_rotation = scored_df["scorerotation"].copy()
        old_mean = float(old_rotation.mean())
        old_median = float(old_rotation.median())
    else:
        old_rotation = pd.Series(0.0, index=scored_df.index)
        old_mean = old_median = 0.0

    # ── Build sector lookup from sector_summary for ETF boost ─────────────
    sector_etf_composite: dict[str, float] = {}
    if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
        for _, srow in sector_summary.iterrows():
            sec = str(srow.get("sector", ""))
            if sec:
                sector_etf_composite[sec] = _safe_float(srow.get("etf_composite"), 0.5)

    # ── Enrich each row ───────────────────────────────────────────────────
    regimes: list[str] = []
    rot_scores: list[float] = []
    etf_boosts: list[float] = []
    blended_vals: list[float] = []

    for _, row in scored_df.iterrows():
        ticker = str(row[ticker_col])

        # 1. Resolve regime
        regime = ticker_regimes.get(ticker)
        if regime is None:
            sector = _resolve_sector(row, ticker)
            regime = sector_regimes.get(sector, "unknown")
        regimes.append(regime)

        # 2. Graded scorerotation
        base_score = regime_scores.get(regime, regime_scores.get("unknown", 0.30))

        # 3. ETF composite boost
        if apply_etf_boost:
            sector = _resolve_sector(row, ticker)
            boost = _compute_etf_boost(sector, sector_summary, etf_ranking)
        else:
            boost = 0.0
        etf_boosts.append(round(boost, 4))

        final_rot = max(0.0, min(1.0, base_score + boost))
        rot_scores.append(round(final_rot, 4))

        # 4. Blended score from sector_summary (for diagnostics)
        sector = _resolve_sector(row, ticker)
        blended = 0.0
        if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
            match = sector_summary[sector_summary["sector"] == sector]
            if not match.empty:
                blended = _safe_float(match.iloc[0].get("blended_score"), 0.0)
        blended_vals.append(round(blended, 4))

    scored_df["rotation_regime"] = regimes
    scored_df["scorerotation"] = rot_scores
    scored_df["etf_boost"] = etf_boosts
    scored_df["rotation_blended"] = blended_vals

    # ── Incremental composite update ──────────────────────────────────── # ← CHANGED
    #
    # Instead of recomputing the full composite from sub-scores (which
    # drops the leadership boost applied in pipeline_v2), apply only
    # the rotation delta to the existing composite.  This preserves
    # all prior adjustments and keeps the enrichment effect predictable.
    if recompute_composite and "scorecomposite_v2" in scored_df.columns:
        _incremental_composite_update(scored_df, old_rotation, params)

    # ── Post-enrichment stats ─────────────────────────────────────────────
    new_mean = scored_df["scorerotation"].mean()
    new_median = scored_df["scorerotation"].median()

    regime_dist: dict[str, int] = {}
    for r in regimes:
        regime_dist[r] = regime_dist.get(r, 0) + 1

    logger.info(
        "enrich_with_rotation: n=%d  "
        "scorerotation old(mean=%.3f median=%.3f) → new(mean=%.3f median=%.3f)  "
        "regime_dist=%s  etf_boost(mean=%.4f)",
        n,
        old_mean, old_median,
        new_mean, new_median,
        regime_dist,
        sum(etf_boosts) / max(len(etf_boosts), 1),
    )

    return scored_df


# ═══════════════════════════════════════════════════════════════
#  Incremental composite update                    ← CHANGED
# ═══════════════════════════════════════════════════════════════

def _incremental_composite_update(
    scored_df: pd.DataFrame,
    old_rotation: pd.Series,
    params: dict,
) -> None:
    """
    Apply the scorerotation delta to scorecomposite_v2 *incrementally*.

    Instead of recomputing the entire weighted sum (which drops the
    leadership boost and any other post-scoring adjustments), this
    computes::

        delta = (new_scorerotation − old_scorerotation) × rotation_weight
        scorecomposite_v2 += delta

    This preserves all prior adjustments and only changes the
    composite by the rotation enrichment amount.
    """
    rotation_weight = params.get("composite_weights", {}).get(
        "scorerotation", 0.20,
    )

    old_composite_mean = float(scored_df["scorecomposite_v2"].mean())

    delta = (scored_df["scorerotation"] - old_rotation) * rotation_weight
    scored_df["scorecomposite_v2"] = (
        scored_df["scorecomposite_v2"] + delta
    ).clip(0.0, 1.0)

    new_composite_mean = float(scored_df["scorecomposite_v2"].mean())

    logger.info(
        "enrich_with_rotation: recomputed scorecomposite_v2  "
        "mean %.4f → %.4f  (delta=%+.4f)  "
        "[incremental: rotation_weight=%.2f avg_rot_delta=%+.4f]",
        old_composite_mean,
        new_composite_mean,
        new_composite_mean - old_composite_mean,
        rotation_weight,
        float(delta.mean()),
    )

######################################################
"""
refactor/strategy/portfolio_v2.py

Portfolio construction for the v2 pipeline.

Takes the scored-and-actioned universe and selects a concentrated
portfolio of up to ``max_positions`` names, subject to:

    1.  Only STRONG_BUY and BUY actions are eligible.
    2.  Sector concentration caps are *dynamic* — sectors in the
        ``leading`` or ``improving`` rotation quadrant receive the full
        ``max_sector_weight``, while ``weakening`` sectors get 60 % of
        the cap and ``lagging`` sectors get 30 %.
    3.  Theme diversification: no more than ``max_theme_names`` names
        from any single theme.
    4.  Position sizing is inverse-ATR weighted (lower volatility →
        larger weight), then capped per name at ``max_single_weight``.
    5.  Total exposure is scaled by a market-regime multiplier derived
        from the breadth and volatility regime columns already present
        on every row.

Output
------
``build_portfolio_v2`` returns a dict with:

    selected   – DataFrame of the final portfolio with per-name
                 weights, risk metrics, and selection reasoning.
    meta       – dict with portfolio-level summary statistics:
                 selected_count, candidate_count, target_exposure,
                 breadth_regime, vol_regime, sector_tilt, etc.

Design notes
------------
The builder is intentionally *stateless* — it takes a single snapshot
and produces a target portfolio.  It does not track prior holdings,
turnover cost, or rebalancing frequency.  Those concerns belong in
an execution layer that compares today's target with yesterday's
holdings.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MAX_POSITIONS = 8
DEFAULT_MAX_SECTOR_WEIGHT = 0.35
DEFAULT_MAX_THEME_NAMES = 2
DEFAULT_MAX_SINGLE_WEIGHT = 0.20
DEFAULT_MIN_WEIGHT = 0.04

# ── Rotation-aware sector cap multipliers ─────────────────────────────────────
ROTATION_CAP_MULT: dict[str, float] = {
    "leading":   1.00,
    "improving": 1.00,
    "weakening": 0.60,
    "lagging":   0.30,
    "unknown":   0.70,
}

# ── Exposure scaling by market regime ─────────────────────────────────────────
# The product of breadth and vol multipliers gives target gross
# exposure.  In a strong/calm environment the portfolio can be fully
# invested; in a weak/chaotic one it scales down significantly.
BREADTH_EXPOSURE: dict[str, float] = {
    "strong": 1.00, "healthy": 0.95, "moderate": 0.88,
    "neutral": 0.80, "mixed": 0.72, "narrow": 0.65,
    "weak": 0.50, "critical": 0.35, "unknown": 0.75,
}
VOL_EXPOSURE: dict[str, float] = {
    "calm": 1.00, "low": 0.97, "normal": 0.92, "moderate": 0.85,
    "elevated": 0.72, "stressed": 0.55, "chaotic": 0.38,
    "unknown": 0.80,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        f = float(value)
        return default if np.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _effective_sector_cap(
    sector_regime: str,
    base_cap: float,
) -> float:
    """Return the dynamic sector weight cap given the rotation regime."""
    mult = ROTATION_CAP_MULT.get(
        str(sector_regime).lower(),
        ROTATION_CAP_MULT["unknown"],
    )
    return base_cap * mult


def _target_exposure(breadth: str, vol: str) -> float:
    """
    Compute target gross exposure from breadth and volatility regimes.

    Returns a value in roughly [0.13, 1.00].
    """
    b = BREADTH_EXPOSURE.get(str(breadth).lower(), BREADTH_EXPOSURE["unknown"])
    v = VOL_EXPOSURE.get(str(vol).lower(), VOL_EXPOSURE["unknown"])
    return round(b * v, 4)


def _inverse_atr_weights(atr_pct_series: pd.Series) -> pd.Series:
    """
    Compute raw inverse-ATR weights.

    Names with lower ATR% get proportionally larger weights.
    A floor of 0.005 prevents division by zero for ultra-low-vol names.
    """
    clamped = atr_pct_series.clip(lower=0.005).fillna(0.03)
    inv = 1.0 / clamped
    total = inv.sum()
    if total <= 0:
        return pd.Series(1.0 / len(clamped), index=clamped.index)
    return inv / total


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def build_portfolio_v2(
    action_table: pd.DataFrame,
    max_positions: int = DEFAULT_MAX_POSITIONS,
    max_sector_weight: float = DEFAULT_MAX_SECTOR_WEIGHT,
    max_theme_names: int = DEFAULT_MAX_THEME_NAMES,
    max_single_weight: float = DEFAULT_MAX_SINGLE_WEIGHT,
    min_weight: float = DEFAULT_MIN_WEIGHT,
) -> dict:
    """
    Build a concentrated target portfolio from the actioned universe.

    Parameters
    ----------
    action_table : DataFrame
        Output of the action generator.  Must contain ``action_v2``,
        ``ticker``, and scoring columns.  Expected but optional:
        ``scoreadjusted_v2``, ``score_percentile_v2``, ``atr14pct``,
        ``sector``, ``theme``, ``sectrsregime``, ``breadthregime``,
        ``volregime``, ``conviction_v2``.
    max_positions : int
        Maximum number of names in the final portfolio.
    max_sector_weight : float
        Base maximum weight for any single GICS sector (before
        rotation-regime adjustment).
    max_theme_names : int
        Maximum number of names from any single theme.
    max_single_weight : float
        Hard cap on any individual position.
    min_weight : float
        Minimum weight for inclusion.  Names that would receive less
        than this after all caps are applied are dropped.

    Returns
    -------
    dict
        ``selected`` (DataFrame) and ``meta`` (dict).
    """
    # ── 0. empty guard ────────────────────────────────────────────────────────
    breadth_regime = "unknown"
    vol_regime = "unknown"

    if action_table.empty or "action_v2" not in action_table.columns:
        logger.info("build_portfolio_v2: empty action table, returning empty portfolio")
        return _empty_portfolio(breadth_regime, vol_regime)

    # Resolve market regimes from the first row (uniform across rows)
    breadth_regime = str(
        action_table["breadthregime"].iloc[0]
        if "breadthregime" in action_table.columns
        else "unknown"
    ).lower()
    vol_regime = str(
        action_table["volregime"].iloc[0]
        if "volregime" in action_table.columns
        else "unknown"
    ).lower()

    # ── 1. filter to eligible actions ─────────────────────────────────────────
    eligible_mask = action_table["action_v2"].isin(["STRONG_BUY", "BUY"])
    candidates = action_table.loc[eligible_mask].copy()

    if candidates.empty:
        logger.info(
            "build_portfolio_v2: no STRONG_BUY or BUY candidates "
            "(total rows=%d)",
            len(action_table),
        )
        return _empty_portfolio(breadth_regime, vol_regime)

    logger.info(
        "Portfolio candidates: %d STRONG_BUY + BUY out of %d total",
        len(candidates), len(action_table),
    )

    # ── 2. rank candidates ────────────────────────────────────────────────────
    # Primary sort: action tier (STRONG_BUY first), then adjusted score
    tier_map = {"STRONG_BUY": 1, "BUY": 2}
    candidates["_tier"] = candidates["action_v2"].map(tier_map).fillna(3).astype(int)

    score_col = (
        "scoreadjusted_v2"
        if "scoreadjusted_v2" in candidates.columns
        else "scorecomposite_v2"
    )
    candidates["_sort_score"] = (
        pd.to_numeric(candidates.get(score_col, 0.0), errors="coerce")
        .fillna(0.0)
    )
    candidates = candidates.sort_values(
        ["_tier", "_sort_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    # ── 3. greedy selection with constraints ──────────────────────────────────
    selected_indices: list[int] = []
    sector_weight_used: dict[str, float] = {}
    theme_count: dict[str, int] = {}

    # Pre-compute inverse-ATR raw weights for the full candidate pool
    atr_col = (
        pd.to_numeric(candidates.get("atr14pct", 0.03), errors="coerce")
        .fillna(0.03)
    )
    raw_weights = _inverse_atr_weights(atr_col)

    for idx in candidates.index:
        if len(selected_indices) >= max_positions:
            break

        row = candidates.loc[idx]
        ticker = str(row.get("ticker", ""))
        sector = str(row.get("sector", "Unknown"))
        theme = str(row.get("theme", "Unknown"))
        sect_regime = str(row.get("sectrsregime", "unknown")).lower()
        raw_w = float(raw_weights.loc[idx])

        # ── sector cap (rotation-aware) ───────────────────────────────────
        effective_cap = _effective_sector_cap(sect_regime, max_sector_weight)
        current_sector_w = sector_weight_used.get(sector, 0.0)
        proposed_w = min(raw_w, max_single_weight)

        if current_sector_w + proposed_w > effective_cap:
            room = effective_cap - current_sector_w
            if room < min_weight:
                logger.debug(
                    "Portfolio: skipping %s — sector %s at %.1f%% "
                    "(cap %.1f%% for %s regime)",
                    ticker, sector,
                    current_sector_w * 100, effective_cap * 100,
                    sect_regime,
                )
                continue
            proposed_w = room

        # ── theme diversification ─────────────────────────────────────────
        if theme != "Unknown":
            current_theme_n = theme_count.get(theme, 0)
            if current_theme_n >= max_theme_names:
                logger.debug(
                    "Portfolio: skipping %s — theme %s already has %d names",
                    ticker, theme, current_theme_n,
                )
                continue

        # ── accept ────────────────────────────────────────────────────────
        selected_indices.append(idx)
        sector_weight_used[sector] = current_sector_w + proposed_w
        if theme != "Unknown":
            theme_count[theme] = theme_count.get(theme, 0) + 1

    if not selected_indices:
        logger.info("build_portfolio_v2: all candidates filtered by constraints")
        return _empty_portfolio(breadth_regime, vol_regime)

    selected = candidates.loc[selected_indices].copy()

    # ── 4. final weight calculation ───────────────────────────────────────────
    # Re-derive inverse-ATR weights among selected names only
    sel_atr = (
        pd.to_numeric(selected.get("atr14pct", 0.03), errors="coerce")
        .fillna(0.03)
    )
    sel_raw_w = _inverse_atr_weights(sel_atr)

    # Apply per-name and per-sector caps iteratively
    final_w = sel_raw_w.clip(upper=max_single_weight).copy()

    for _ in range(5):  # iterate to redistribute excess
        # enforce rotation-aware sector caps
        for sector in selected["sector"].unique():
            sect_mask = selected["sector"] == sector
            sector_total = float(final_w.loc[sect_mask].sum())
            # all names in this sector share the same regime in practice,
            # but take the mode for safety
            regimes = (
                selected.loc[sect_mask, "sectrsregime"]
                .astype(str).str.lower()
            )
            regime_mode = regimes.mode().iloc[0] if not regimes.empty else "unknown"
            cap = _effective_sector_cap(regime_mode, max_sector_weight)

            if sector_total > cap and sector_total > 0:
                scale = cap / sector_total
                final_w.loc[sect_mask] *= scale

        # enforce single-name cap
        final_w = final_w.clip(upper=max_single_weight)

        # renormalise so total = 1
        total = final_w.sum()
        if total > 0:
            final_w = final_w / total
        else:
            final_w = pd.Series(
                1.0 / len(final_w), index=final_w.index,
            )

    # ── 5. apply exposure scaling ─────────────────────────────────────────────
    gross_target = _target_exposure(breadth_regime, vol_regime)
    final_w = final_w * gross_target

    # Drop names below minimum weight
    keep_mask = final_w >= min_weight
    if not keep_mask.all():
        dropped = selected.loc[~keep_mask, "ticker"].tolist()
        logger.info(
            "Portfolio: dropping %d names below min_weight %.2f%%: %s",
            len(dropped), min_weight * 100, dropped,
        )
        selected = selected.loc[keep_mask].copy()
        final_w = final_w.loc[keep_mask]

        # Renormalise to target exposure
        if final_w.sum() > 0:
            final_w = final_w / final_w.sum() * gross_target

    selected["portfolio_weight"] = final_w.values
    selected["portfolio_weight_pct"] = (final_w.values * 100).round(2)

    # ── 6. enrich with selection metadata ─────────────────────────────────────
    selected["selection_rank"] = range(1, len(selected) + 1)

    # per-name effective sector cap for transparency
    selected["effective_sector_cap"] = selected.apply(
        lambda r: _effective_sector_cap(
            str(r.get("sectrsregime", "unknown")).lower(),
            max_sector_weight,
        ),
        axis=1,
    )

    # selection reason
    def _reason(r):
        parts = [
            str(r.get("action_v2", "")),
            f"score={_safe_float(r.get(score_col), 0):.3f}",
            f"atr={_safe_float(r.get('atr14pct'), 0):.4f}",
            f"sect={r.get('sector', 'Unknown')}({r.get('sectrsregime', 'unknown')})",
            f"w={r.get('portfolio_weight_pct', 0):.1f}%",
        ]
        return " | ".join(parts)

    selected["selection_reason"] = selected.apply(_reason, axis=1)

    # Clean up internal sort columns
    selected = selected.drop(columns=["_tier", "_sort_score"], errors="ignore")
    selected = selected.reset_index(drop=True)

    # ── 7. build sector tilt summary ──────────────────────────────────────────
    sector_tilt = _build_sector_tilt(selected, max_sector_weight)

    # ── 8. build rotation exposure summary ────────────────────────────────────
    rotation_exposure = _build_rotation_exposure(selected)

    # ── 9. meta ───────────────────────────────────────────────────────────────
    meta = {
        "selected_count": len(selected),
        "candidate_count": len(candidates),
        "total_universe": len(action_table),
        "target_exposure": gross_target,
        "actual_exposure": round(float(selected["portfolio_weight"].sum()), 4),
        "cash_reserve": round(
            1.0 - float(selected["portfolio_weight"].sum()), 4,
        ),
        "breadth_regime": breadth_regime,
        "vol_regime": vol_regime,
        "max_positions": max_positions,
        "max_sector_weight_base": max_sector_weight,
        "max_single_weight": max_single_weight,
        "max_theme_names": max_theme_names,
        "sector_tilt": sector_tilt,
        "rotation_exposure": rotation_exposure,
    }

    # ── 10. logging ───────────────────────────────────────────────────────────
    logger.info(
        "Portfolio built: %d names  exposure=%.1f%%  cash=%.1f%%  "
        "breadth=%s  vol=%s",
        meta["selected_count"],
        meta["actual_exposure"] * 100,
        meta["cash_reserve"] * 100,
        breadth_regime,
        vol_regime,
    )

    if sector_tilt:
        logger.info("Sector tilt:")
        for entry in sector_tilt:
            logger.info(
                "  %-20s  regime=%-10s  weight=%5.1f%%  cap=%5.1f%%  "
                "names=%d",
                entry["sector"],
                entry["regime"],
                entry["weight_pct"],
                entry["effective_cap_pct"],
                entry["count"],
            )

    if rotation_exposure:
        logger.info("Rotation quadrant exposure:")
        for entry in rotation_exposure:
            logger.info(
                "  %-10s  weight=%5.1f%%  names=%d",
                entry["quadrant"],
                entry["weight_pct"],
                entry["count"],
            )

    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [c for c in [
            "selection_rank", "ticker", "action_v2", "portfolio_weight_pct",
            score_col, "atr14pct", "sector", "sectrsregime",
            "effective_sector_cap", "theme", "conviction_v2",
            "selection_reason",
        ] if c in selected.columns]
        logger.debug(
            "Portfolio detail:\n%s",
            selected[preview_cols].to_string(index=False),
        )

    return {
        "selected": selected,
        "meta": meta,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Sector tilt summary
# ═══════════════════════════════════════════════════════════════════════════════

def _build_sector_tilt(
    selected: pd.DataFrame,
    base_cap: float,
) -> list[dict]:
    """
    Build a summary of portfolio weight by sector, including the
    effective rotation-adjusted cap for each.
    """
    if selected.empty or "sector" not in selected.columns:
        return []

    rows = []
    for sector in sorted(selected["sector"].unique()):
        mask = selected["sector"] == sector
        subset = selected.loc[mask]
        weight = float(subset["portfolio_weight"].sum())
        count = len(subset)

        # Determine the sector's rotation regime (mode among selected names)
        if "sectrsregime" in subset.columns:
            regimes = subset["sectrsregime"].astype(str).str.lower()
            regime = regimes.mode().iloc[0] if not regimes.empty else "unknown"
        else:
            regime = "unknown"

        cap = _effective_sector_cap(regime, base_cap)

        rows.append({
            "sector": sector,
            "regime": regime,
            "weight": round(weight, 4),
            "weight_pct": round(weight * 100, 1),
            "effective_cap": round(cap, 4),
            "effective_cap_pct": round(cap * 100, 1),
            "count": count,
            "headroom_pct": round((cap - weight) * 100, 1),
        })

    return sorted(rows, key=lambda r: r["weight"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Rotation exposure summary
# ═══════════════════════════════════════════════════════════════════════════════

def _build_rotation_exposure(selected: pd.DataFrame) -> list[dict]:
    """
    Summarise portfolio weight by rotation quadrant.

    This gives a single-glance view of how the portfolio is positioned
    relative to the sector rotation cycle.
    """
    if selected.empty or "sectrsregime" not in selected.columns:
        return []

    rows = []
    for quadrant in ("leading", "improving", "weakening", "lagging", "unknown"):
        mask = selected["sectrsregime"].astype(str).str.lower() == quadrant
        subset = selected.loc[mask]
        if subset.empty:
            continue
        weight = float(subset["portfolio_weight"].sum())
        rows.append({
            "quadrant": quadrant,
            "weight": round(weight, 4),
            "weight_pct": round(weight * 100, 1),
            "count": len(subset),
            "tickers": sorted(subset["ticker"].tolist()),
        })

    return sorted(rows, key=lambda r: r["weight"], reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Empty result
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_portfolio(breadth_regime: str, vol_regime: str) -> dict:
    return {
        "selected": pd.DataFrame(),
        "meta": {
            "selected_count": 0,
            "candidate_count": 0,
            "total_universe": 0,
            "target_exposure": _target_exposure(breadth_regime, vol_regime),
            "actual_exposure": 0.0,
            "cash_reserve": 1.0,
            "breadth_regime": breadth_regime,
            "vol_regime": vol_regime,
            "max_positions": DEFAULT_MAX_POSITIONS,
            "max_sector_weight_base": DEFAULT_MAX_SECTOR_WEIGHT,
            "max_single_weight": DEFAULT_MAX_SINGLE_WEIGHT,
            "max_theme_names": DEFAULT_MAX_THEME_NAMES,
            "sector_tilt": [],
            "rotation_exposure": [],
        },
    }

####################################################
"""refactor/strategy/regime_v2.py"""
from __future__ import annotations

import numpy as np
import pandas as pd

from refactor.common.config_refactor import VOLREGIMEPARAMS


def _clip01(x):
    return np.clip(x, 0.0, 1.0)


def classify_volatility_regime(
    bench: pd.DataFrame,
    dispersion: pd.Series | None = None,
    params: dict | None = None,
) -> pd.DataFrame:
    if bench is None or bench.empty:
        raise ValueError("Benchmark dataframe cannot be empty")
    if "close" not in bench.columns:
        raise ValueError("Benchmark dataframe must contain a close column")

    p = params if params is not None else VOLREGIMEPARAMS
    df = bench.copy()
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df.get("high", close), errors="coerce")
    low = pd.to_numeric(df.get("low", close), errors="coerce")
    prev = close.shift(1)

    tr = pd.concat([(high - low).abs(), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    atrp = tr.rolling(p["atrp_window"], min_periods=5).mean() / close.replace(0, np.nan)
    rv = close.pct_change().rolling(p["realized_vol_window"], min_periods=5).std() * np.sqrt(252)
    gap = ((close / prev - 1.0).abs() > 0.02).rolling(p["gap_window"], min_periods=5).mean()
    dispersion = pd.Series(index=df.index, data=np.nan) if dispersion is None else dispersion.reindex(df.index)

    atrp_s = _clip01((atrp - p["calm_atrp_max"]) / (p["volatile_atrp_max"] - p["calm_atrp_max"]))
    rv_s = _clip01((rv - p["calm_rvol_max"]) / (p["volatile_rvol_max"] - p["calm_rvol_max"]))
    gap_s = _clip01((gap - p["volatile_gap_rate"]) / (p["chaotic_gap_rate"] - p["volatile_gap_rate"]))
    disp_s = _clip01((dispersion - p["calm_dispersion_max"]) / (p["volatile_dispersion_max"] - p["calm_dispersion_max"]))

    w = p["score_weights"]
    score = (
        w["atrp"] * pd.Series(atrp_s, index=df.index).fillna(0)
        + w["realized_vol"] * pd.Series(rv_s, index=df.index).fillna(0)
        + w["gap_rate"] * pd.Series(gap_s, index=df.index).fillna(0)
        + w["dispersion"] * pd.Series(disp_s, index=df.index).fillna(0)
    )

    # FIX: regime label thresholds from config instead of hardcoded
    chaotic_thresh = p.get("chaotic_threshold", 0.75)
    volatile_thresh = p.get("volatile_threshold", 0.35)

    label = np.select(
        [score >= chaotic_thresh, score >= volatile_thresh],
        ["chaotic", "volatile"],
        default="calm",
    )
    return pd.DataFrame(
        {
            "volregime": label,
            "volregimescore": score.clip(0, 1),
            "atrp_bench": atrp,
            "realizedvol_bench": rv,
            "gaprate_bench": gap,
            "dispersion_bench": dispersion,
        },
        index=df.index,
    )

#######################################################
"""refactor/strategy/rotation_v2.py – Sector rotation with ETF composite scoring.

US markets:     Uses sector SPDR ETFs + thematic ETFs for RRG + composite blend.
Non-US markets: Computes sector rotation from constituent stocks grouped by
                sector, using an equal-weighted synthetic close per sector
                vs the local benchmark.  ETF scoring is skipped.
"""
from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd

from common.sector_map import get_sector_or_class

from refactor.common.config_refactor import ROTATIONPARAMS

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Market classification
# ═══════════════════════════════════════════════════════════════════════════════

# Markets where US sector ETFs (XLK, XLF, …) are present in the data.
# All other markets use constituent-based rotation.
_US_LIKE_MARKETS = {"US"}

# ═══════════════════════════════════════════════════════════════════════════════
# ETF ↔ Sector / Theme mappings  (US only)
# ═══════════════════════════════════════════════════════════════════════════════

SECTOR_ETF = {
    "Technology":              "XLK",
    "Financials":              "XLF",
    "Energy":                  "XLE",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Communication Services":  "XLC",
    "Consumer Discretionary":  "XLY",
    "Consumer Staples":        "XLP",
    "Utilities":               "XLU",
    "Real Estate":             "XLRE",
    "Materials":               "XLB",
}

ETF_TO_SECTOR = {v: k for k, v in SECTOR_ETF.items()}

THEMATIC_ETF_SECTOR = {
    "SOXX": "Technology",   "SMH": "Technology",
    "IGV": "Technology",    "SKYY": "Technology",
    "HACK": "Technology",   "CIBR": "Technology",
    "BOTZ": "Technology",   "AIQ": "Technology",
    "QTUM": "Technology",   "FINX": "Financials",
    "XBI": "Healthcare",    "IBB": "Healthcare",
    "ARKG": "Healthcare",
    "TAN": "Energy",        "ICLN": "Energy",
    "URA": "Energy",        "NLR": "Energy",
    "URNM": "Energy",       "LIT": "Materials",
    "DRIV": "Consumer Discretionary",
    "IBIT": "Financials",   "BLOK": "Technology",
    "MTUM": "Broad",        "ITA": "Industrials",
    "ARKK": "Technology",
}

ETF_THEME = {
    "SOXX": "Semiconductors",    "SMH": "Semiconductors",
    "IGV": "Software",           "SKYY": "Cloud Computing",
    "HACK": "Cybersecurity",     "CIBR": "Cybersecurity",
    "BOTZ": "Robotics & AI",     "AIQ": "AI & Big Data",
    "QTUM": "Quantum Computing", "FINX": "Fintech",
    "XBI": "Biotech",            "IBB": "Biotech",
    "ARKG": "Genomics",
    "TAN": "Solar",              "ICLN": "Clean Energy",
    "LIT": "Lithium & Battery",  "URA": "Uranium",
    "NLR": "Nuclear",            "URNM": "Uranium",
    "DRIV": "Autonomous & EV",
    "IBIT": "Bitcoin",           "BLOK": "Blockchain",
    "MTUM": "Momentum Factor",   "ITA": "Defense & Aerospace",
    "ARKK": "Innovation",
    "XLK": "Technology",         "XLF": "Financials",
    "XLE": "Energy",             "XLV": "Healthcare",
    "XLI": "Industrials",        "XLC": "Communication Services",
    "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
    "XLU": "Utilities",          "XLRE": "Real Estate",
    "XLB": "Materials",
}

BROAD_ETFS = {"SPY", "QQQ", "IWM", "DIA", "MDY"}
REGIONAL_ETFS = {
    "KWEB", "EEM", "EFA", "VWO", "FXI", "EWJ",
    "EWZ", "INDA", "EWG", "EWT", "EWY",
}
FIXED_INCOME_ETFS = {"TLT", "IEF", "HYG", "LQD", "TIP", "AGG"}
COMMODITY_ETFS = {"GLD", "SLV", "USO", "UNG", "DBA", "DBC"}

ROTATION_ETFS = set(ETF_TO_SECTOR) | set(THEMATIC_ETF_SECTOR)
ALL_TRACKED_ETFS = ROTATION_ETFS | BROAD_ETFS | REGIONAL_ETFS


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        f = float(val)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _get(row: dict, *keys, default=None):
    """Return the first non-null value found among *keys*."""
    for k in keys:
        v = row.get(k)
        if v is not None:
            try:
                if not math.isnan(float(v)):
                    return v
            except (TypeError, ValueError):
                return v
    return default


def _normalize(val: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.5
    return max(0.0, min(1.0, (val - lo) / (hi - lo)))


# ═══════════════════════════════════════════════════════════════════════════════
# Inline indicator computation for ETF frames (OHLCV → indicators)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_etf_indicators(df: pd.DataFrame) -> dict:
    """
    Compute technical indicators from a raw OHLCV DataFrame.

    Called when ETF frames have not been through the stock indicator
    pipeline (or when ensure_columns has backfilled meaningless defaults).

    Each indicator is independently try/excepted so a failure in one
    (e.g. ADX) does not prevent the others from being computed.
    """
    out: dict[str, float] = {}
    if df is None or len(df) < 2:
        return out

    try:
        close = pd.to_numeric(df["close"], errors="coerce") if "close" in df.columns else None
    except Exception as e:
        logger.error("_compute_etf_indicators: failed to read close column: %s", e)
        return out

    if close is None or close.dropna().empty:
        logger.warning(
            "_compute_etf_indicators: close is None or all-NaN "
            "(columns=%s rows=%d)",
            list(df.columns)[:8], len(df),
        )
        return out

    high = (
        pd.to_numeric(df["high"], errors="coerce")
        if "high" in df.columns else close
    )
    low = (
        pd.to_numeric(df["low"], errors="coerce")
        if "low" in df.columns else close
    )
    open_ = (
        pd.to_numeric(df["open"], errors="coerce")
        if "open" in df.columns else close
    )
    volume = (
        pd.to_numeric(df["volume"], errors="coerce")
        if "volume" in df.columns else None
    )

    n = len(close)
    alpha14 = 1.0 / 14

    # ── RSI-14 (Wilder smoothing) ─────────────────────────────────────────
    try:
        if n >= 16:
            delta = close.diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()
            avg_loss = loss.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)
            rsi = 100.0 - 100.0 / (1.0 + rs)
            val = rsi.iloc[-1]
            if pd.notna(val):
                out["rsi14"] = float(np.clip(val, 0, 100))
    except Exception as e:
        logger.error("_compute_etf_indicators: RSI computation failed: %s", e)

    # ── ADX-14 + ATR-14 pct ──────────────────────────────────────────────
    try:
        if n >= 30:
            prev_close = close.shift(1)
            tr = pd.concat(
                [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
                axis=1,
            ).max(axis=1)

            up_move = high - high.shift(1)
            dn_move = low.shift(1) - low

            plus_dm = pd.Series(
                np.where((up_move > dn_move) & (up_move > 0), up_move, 0.0),
                index=close.index,
            )
            minus_dm = pd.Series(
                np.where((dn_move > up_move) & (dn_move > 0), dn_move, 0.0),
                index=close.index,
            )

            atr_s = tr.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()
            plus_di = (
                100
                * plus_dm.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()
                / atr_s.replace(0, np.nan)
            )
            minus_di = (
                100
                * minus_dm.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()
                / atr_s.replace(0, np.nan)
            )

            di_sum = plus_di + minus_di
            dx = 100 * (plus_di - minus_di).abs() / di_sum.replace(0, np.nan)
            adx = dx.ewm(alpha=alpha14, min_periods=14, adjust=False).mean()

            adx_val = adx.iloc[-1]
            if pd.notna(adx_val):
                out["adx14"] = float(np.clip(adx_val, 0, 100))

            atr_val = atr_s.iloc[-1]
            c_val = close.iloc[-1]
            if pd.notna(atr_val) and pd.notna(c_val) and c_val > 0:
                out["atr14pct"] = float(atr_val / c_val)
    except Exception as e:
        logger.error("_compute_etf_indicators: ADX/ATR computation failed: %s", e)

    # ── Close vs EMA-30 % ────────────────────────────────────────────────
    try:
        if n >= 30:
            ema30 = close.ewm(span=30, min_periods=30, adjust=False).mean()
            e_val = ema30.iloc[-1]
            c_val = close.iloc[-1]
            if pd.notna(e_val) and pd.notna(c_val) and e_val > 0:
                out["closevsema30pct"] = float(c_val / e_val - 1.0)
    except Exception as e:
        logger.error("_compute_etf_indicators: EMA-30 computation failed: %s", e)

    # ── Close vs SMA-50 % ────────────────────────────────────────────────
    try:
        if n >= 50:
            sma50 = close.rolling(50).mean()
            s_val = sma50.iloc[-1]
            c_val = close.iloc[-1]
            if pd.notna(s_val) and pd.notna(c_val) and s_val > 0:
                out["closevssma50pct"] = float(c_val / s_val - 1.0)
    except Exception as e:
        logger.error("_compute_etf_indicators: SMA-50 computation failed: %s", e)

    # ── MACD histogram (12, 26, 9) ───────────────────────────────────────
    try:
        if n >= 35:
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal = macd_line.ewm(span=9, adjust=False).mean()
            hist = macd_line - signal
            val = hist.iloc[-1]
            if pd.notna(val):
                out["macdhist"] = float(val)
    except Exception as e:
        logger.error("_compute_etf_indicators: MACD computation failed: %s", e)

    # ── Relative volume (current bar vs 20d average) ─────────────────────
    try:
        if volume is not None and n >= 21:
            vol_clean = volume.dropna()
            if len(vol_clean) >= 21:
                avg_20 = vol_clean.iloc[-21:-1].mean()
                cur = vol_clean.iloc[-1]
                if pd.notna(avg_20) and avg_20 > 0 and pd.notna(cur):
                    out["relativevolume"] = float(cur / avg_20)
    except Exception as e:
        logger.error("_compute_etf_indicators: RVOL computation failed: %s", e)

    # ── Dollar volume 20d average ────────────────────────────────────────
    try:
        if volume is not None and close is not None and n >= 20:
            dv = volume * close
            dv_avg = dv.rolling(20, min_periods=15).mean()
            val = dv_avg.iloc[-1]
            if pd.notna(val) and val > 0:
                out["dollarvolume20d"] = float(val)
    except Exception as e:
        logger.error("_compute_etf_indicators: dollar volume computation failed: %s", e)

    # ── Realized volatility 20d (annualized) ─────────────────────────────
    try:
        if n >= 22:
            log_ret = np.log(close / close.shift(1)).dropna()
            if len(log_ret) >= 20:
                out["realizedvol20d"] = float(
                    log_ret.iloc[-20:].std() * np.sqrt(252)
                )
    except Exception as e:
        logger.error("_compute_etf_indicators: realized vol computation failed: %s", e)

    # ── Gap rate 20 (fraction of days with |gap| > 1%) ───────────────────
    try:
        if n >= 22 and "open" in df.columns:
            prev_c = close.shift(1)
            gap_pct = ((open_ - prev_c) / prev_c.replace(0, np.nan)).abs()
            last_20 = gap_pct.iloc[-20:]
            valid = last_20.dropna()
            if len(valid) > 0:
                out["gaprate20"] = float((valid > 0.01).sum() / len(valid))
    except Exception as e:
        logger.error("_compute_etf_indicators: gap rate computation failed: %s", e)

    if out:
        logger.debug(
            "_compute_etf_indicators: n=%d computed=%d indicators: %s",
            n, len(out), {k: round(v, 4) for k, v in out.items()},
        )
    else:
        logger.warning(
            "_compute_etf_indicators: n=%d but produced ZERO indicators "
            "(close_valid=%d high_in_cols=%s low_in_cols=%s vol_in_cols=%s)",
            n,
            int(close.notna().sum()),
            "high" in df.columns,
            "low" in df.columns,
            "volume" in df.columns,
        )

    return out


# ═══════════════════════════════════════════════════════════════════════════════
# ETF composite scoring  (US only — skipped for non-US markets)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_etf_composite(row: dict, params: dict) -> dict[str, float]:
    """
    Score a single ETF on a 0-1 composite from its latest-bar indicators.

    Returns dict with sub-scores and the final composite for diagnostics.
    """
    sp = params.get("etf_scoring", {})
    w_trend = sp.get("trend_weight", 0.35)
    w_mom   = sp.get("momentum_weight", 0.30)
    w_part  = sp.get("participation_weight", 0.20)
    w_risk  = sp.get("risk_weight", 0.15)

    # ── trend ─────────────────────────────────────────────────────────────────
    rsi     = _safe_float(_get(row, "rsi14", "rsi_14"), 50.0)
    adx     = _safe_float(_get(row, "adx14", "adx_14"), 15.0)
    ema_pct = _safe_float(_get(row, "closevsema30pct", "close_vs_ema_30_pct"), 0.0)
    sma_pct = _safe_float(_get(row, "closevssma50pct", "close_vs_sma_50_pct"), 0.0)

    rsi_sc = _normalize(rsi, 30.0, 70.0)
    adx_sc = _normalize(adx, 10.0, 40.0)
    ema_sc = _normalize(ema_pct, -0.05, 0.10)
    sma_sc = _normalize(sma_pct, -0.08, 0.15)

    trend = 0.35 * rsi_sc + 0.30 * adx_sc + 0.20 * ema_sc + 0.15 * sma_sc

    # ── momentum ──────────────────────────────────────────────────────────────
    rs_z      = _safe_float(_get(row, "rszscore"), 0.0)
    ret_20d   = _safe_float(_get(row, "return20d"), 0.0)
    macd_hist = _safe_float(_get(row, "macdhist", "macd_hist"), 0.0)

    rs_z_sc  = _normalize(rs_z, -2.0, 2.0)
    ret_sc   = _normalize(ret_20d, -0.10, 0.15)
    macd_sc  = _normalize(macd_hist, -0.5, 0.5)

    momentum = 0.50 * rs_z_sc + 0.30 * ret_sc + 0.20 * macd_sc

    # ── participation ─────────────────────────────────────────────────────────
    rvol = _safe_float(_get(row, "relativevolume", "relative_volume"), 0.0)
    rvol_sc = _normalize(rvol, 0.05, 1.5)

    dvol_raw = _safe_float(
        _get(row, "dollarvolume20d", "dollarvolumeavg20",
             "dollar_volume_avg_20"),
        0.0,
    )
    log_dvol = math.log1p(dvol_raw) if dvol_raw > 0 else 0.0
    dvol_sc = _normalize(log_dvol, 14.0, 22.0)

    participation = 0.60 * rvol_sc + 0.40 * dvol_sc

    # ── risk adjustment (higher = lower risk = better) ────────────────────────
    real_vol = _safe_float(_get(row, "realizedvol20d", "realized_vol_20d"), 0.20)
    gap_rate = _safe_float(_get(row, "gaprate20", "gap_rate_20"), 0.30)
    atr_pct  = _safe_float(_get(row, "atr14pct", "atr_14_pct"), 0.02)

    vol_sc = 1.0 - _normalize(real_vol, 0.10, 0.50)
    gap_sc = 1.0 - _normalize(gap_rate, 0.10, 0.70)
    atr_sc = 1.0 - _normalize(atr_pct, 0.01, 0.05)

    risk_adj = 0.40 * vol_sc + 0.30 * gap_sc + 0.30 * atr_sc

    composite = w_trend * trend + w_mom * momentum + w_part * participation + w_risk * risk_adj
    composite = max(0.0, min(1.0, composite))

    return {
        "trend": round(trend, 4),
        "momentum": round(momentum, 4),
        "participation": round(participation, 4),
        "risk_adj": round(risk_adj, 4),
        "composite": round(composite, 4),
    }


def _extract_etf_row(df: pd.DataFrame) -> dict | None:
    """
    Extract the last row of an ETF frame as a dict.

    ALWAYS computes indicators from OHLCV and overwrites whatever is
    in the row.  This is necessary because ensure_columns backfills
    neutral defaults (RSI=50.0, ADX=20.0, RVOL=1.0 …) that are
    indistinguishable from real computed values — if we only compute
    when values are "missing", we never compute at all.
    """
    if df is None or df.empty:
        return None
    row = df.iloc[-1].to_dict()

    # ── 20d return (always computed from close) ───────────────────────────
    if "close" in df.columns and len(df) >= 20:
        close = pd.to_numeric(df["close"], errors="coerce")
        c_now = close.iloc[-1]
        c_20 = close.iloc[-20] if len(close) >= 20 else close.iloc[0]
        if pd.notna(c_now) and pd.notna(c_20) and c_20 > 0:
            row["return20d"] = float(c_now / c_20 - 1.0)

    # ── ALWAYS compute indicators from OHLCV ─────────────────────────────
    computed = _compute_etf_indicators(df)
    ticker = row.get("ticker", row.get("symbol", "?"))

    if computed:
        for k, v in computed.items():
            row[k] = v
        logger.debug(
            "_extract_etf_row(%s): computed %d indicators inline "
            "(rsi=%.1f adx=%.1f rvol=%.2f ema_pct=%.4f atr_pct=%.4f dvol=%.0f)",
            ticker, len(computed),
            _safe_float(computed.get("rsi14"), -1),
            _safe_float(computed.get("adx14"), -1),
            _safe_float(computed.get("relativevolume"), -1),
            _safe_float(computed.get("closevsema30pct"), -1),
            _safe_float(computed.get("atr14pct"), -1),
            _safe_float(computed.get("dollarvolume20d"), -1),
        )
    else:
        n = len(df) if df is not None else 0
        has_close = "close" in df.columns if df is not None else False
        logger.warning(
            "_extract_etf_row(%s): _compute_etf_indicators returned EMPTY "
            "(rows=%d has_close=%s) — ETF will use adapter defaults",
            ticker, n, has_close,
        )

    return row


def score_etf_universe(
    all_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    params: dict | None = None,
) -> pd.DataFrame:
    """
    Score every ETF found in *all_frames* on a 0-1 composite.

    Returns DataFrame sorted descending by etf_composite with columns:
      ticker, theme, parent_sector, is_sector_etf, is_broad, etf_composite,
      sub_trend, sub_momentum, sub_participation, sub_risk_adj,
      rsi14, adx14, rszscore, relativevolume, closevsema30pct, return20d,
      realizedvol20d, dollarvolume20d
    """
    params = params or {}
    scoreable = ALL_TRACKED_ETFS

    # ── Optionally compute RS z-score for each ETF vs benchmark ───────────
    etf_rs_z: dict[str, float] = {}
    if bench_df is not None and not bench_df.empty and "close" in bench_df.columns:
        bench_close = pd.to_numeric(bench_df["close"], errors="coerce").dropna()
        if len(bench_close) >= 60:
            bench_ret_20 = bench_close.iloc[-1] / bench_close.iloc[-20] - 1.0
            bench_ret_60 = bench_close.iloc[-1] / bench_close.iloc[-60] - 1.0
            rets_20 = {}
            rets_60 = {}
            for tk in scoreable:
                if tk not in all_frames:
                    continue
                edf = all_frames[tk]
                if edf is None or edf.empty or "close" not in edf.columns:
                    continue
                ec = pd.to_numeric(edf["close"], errors="coerce").dropna()
                if len(ec) >= 60:
                    rets_20[tk] = float(ec.iloc[-1] / ec.iloc[-20] - 1.0)
                    rets_60[tk] = float(ec.iloc[-1] / ec.iloc[-60] - 1.0)

            if len(rets_20) >= 5:
                excess_20 = {t: r - bench_ret_20 for t, r in rets_20.items()}
                excess_60 = {t: r - bench_ret_60 for t, r in rets_60.items()}
                blended = {
                    t: 0.6 * excess_20.get(t, 0) + 0.4 * excess_60.get(t, 0)
                    for t in excess_20
                }
                vals = list(blended.values())
                mu = np.mean(vals)
                sigma = np.std(vals)
                if sigma > 1e-8:
                    etf_rs_z = {t: (v - mu) / sigma for t, v in blended.items()}

    rows = []
    default_counts = {"rsi14": 0, "adx14": 0, "relativevolume": 0}
    for ticker in scoreable:
        if ticker not in all_frames:
            continue
        raw = _extract_etf_row(all_frames[ticker])
        if raw is None:
            continue

        if ticker in etf_rs_z and _get(raw, "rszscore") is None:
            raw["rszscore"] = etf_rs_z[ticker]

        if _get(raw, "rsi14", "rsi_14") is None:
            default_counts["rsi14"] += 1
        if _get(raw, "adx14", "adx_14") is None:
            default_counts["adx14"] += 1
        if _get(raw, "relativevolume", "relative_volume") is None:
            default_counts["relativevolume"] += 1

        scores = _compute_etf_composite(raw, params)
        parent_sector = ETF_TO_SECTOR.get(
            ticker, THEMATIC_ETF_SECTOR.get(ticker, "Other")
        )
        theme = ETF_THEME.get(ticker, "Other")

        rows.append({
            "ticker":            ticker,
            "theme":             theme,
            "parent_sector":     parent_sector,
            "is_sector_etf":     ticker in ETF_TO_SECTOR,
            "is_broad":          ticker in BROAD_ETFS,
            "is_regional":       ticker in REGIONAL_ETFS,
            "etf_composite":     scores["composite"],
            "sub_trend":         scores["trend"],
            "sub_momentum":      scores["momentum"],
            "sub_participation": scores["participation"],
            "sub_risk_adj":      scores["risk_adj"],
            "rsi14":             _safe_float(_get(raw, "rsi14", "rsi_14"), 50.0),
            "adx14":             _safe_float(_get(raw, "adx14", "adx_14"), 15.0),
            "rszscore":          _safe_float(_get(raw, "rszscore"), 0.0),
            "relativevolume":    _safe_float(
                _get(raw, "relativevolume", "relative_volume"), 1.0
            ),
            "closevsema30pct":   _safe_float(
                _get(raw, "closevsema30pct", "close_vs_ema_30_pct"), 0.0
            ),
            "return20d":         _safe_float(_get(raw, "return20d"), 0.0),
            "realizedvol20d":    _safe_float(
                _get(raw, "realizedvol20d", "realized_vol_20d"), 0.20
            ),
            "dollarvolume20d":   _safe_float(
                _get(raw, "dollarvolume20d", "dollarvolumeavg20"), 0.0
            ),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("score_etf_universe: no ETFs found in frames")
        return df

    n_etfs = len(df)
    for col, cnt in default_counts.items():
        if cnt == n_etfs:
            logger.error(
                "⚠️  score_etf_universe: %s is DEFAULT for all %d ETFs — "
                "inline computation failed or frames have <16 bars",
                col, n_etfs,
            )
        elif cnt > 0:
            logger.warning(
                "score_etf_universe: %s defaulted for %d / %d ETFs",
                col, cnt, n_etfs,
            )

    for sub in ("sub_trend", "sub_participation", "sub_risk_adj"):
        if sub in df.columns and df[sub].std() < 1e-6:
            logger.error(
                "⚠️  score_etf_universe: %s is constant (%.4f) across "
                "all ETFs — composite is effectively blind on this dimension",
                sub, df[sub].iloc[0],
            )

    df = df.sort_values("etf_composite", ascending=False).reset_index(drop=True)
    logger.info(
        "ETF universe scored: n=%d  top=%s(%.3f)  bottom=%s(%.3f)  mean=%.3f",
        len(df),
        df.iloc[0]["ticker"],  df.iloc[0]["etf_composite"],
        df.iloc[-1]["ticker"], df.iloc[-1]["etf_composite"],
        df["etf_composite"].mean(),
    )
    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [
            "ticker", "theme", "parent_sector", "etf_composite",
            "sub_trend", "sub_momentum", "sub_participation", "sub_risk_adj",
            "rsi14", "adx14", "rszscore", "relativevolume", "return20d",
        ]
        logger.debug(
            "ETF ranking:\n%s",
            df[[c for c in preview_cols if c in df.columns]]
            .head(30)
            .to_string(index=False),
        )
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# RS-based analysis (traditional RRG)  –  DATE-ALIGNED
# ═══════════════════════════════════════════════════════════════════════════════

def _align_close_series(
    etf_df: pd.DataFrame,
    bench_df: pd.DataFrame,
) -> tuple[pd.Series, pd.Series] | None:
    """
    Extract 'close' from both frames and align by date index.

    Returns (etf_close, bench_close) on the common DatetimeIndex,
    or None if alignment produces fewer than 2 rows.
    """
    if etf_df is None or etf_df.empty or bench_df is None or bench_df.empty:
        return None
    if "close" not in etf_df.columns or "close" not in bench_df.columns:
        return None

    etf_close = pd.to_numeric(etf_df["close"], errors="coerce").dropna()
    bench_close = pd.to_numeric(bench_df["close"], errors="coerce").dropna()

    if etf_close.empty or bench_close.empty:
        return None

    common_idx = etf_close.index.intersection(bench_close.index)
    if len(common_idx) < 2:
        return None

    common_idx = common_idx.sort_values()
    return etf_close.loc[common_idx], bench_close.loc[common_idx]


def _compute_sector_rs(
    sector_label: str,
    all_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    lookback: int = 20,
    smooth: int = 5,
    override_df: pd.DataFrame | None = None,
) -> dict[str, float]:
    """
    Compute RS level, RS momentum, and excess return for a sector.

    Uses DATE-ALIGNED close series to prevent positional misalignment
    when frames have different row counts or trading calendars.

    Parameters
    ----------
    sector_label : str
        Ticker or descriptive label (used for logging only).
    all_frames : dict
        Ticker → DataFrame mapping.  Looked up by *sector_label*
        unless *override_df* is provided.
    bench_df : pd.DataFrame
        Benchmark OHLCV with DatetimeIndex.
    override_df : pd.DataFrame | None
        If provided, used directly instead of looking up *sector_label*
        in *all_frames*.  Enables synthetic sector close series.
    """
    null = {"rs_level": 0.0, "rs_mom": 0.0, "excess_20d": 0.0}

    etf_df = override_df if override_df is not None else all_frames.get(sector_label)
    aligned = _align_close_series(etf_df, bench_df)
    if aligned is None:
        logger.warning(
            "_compute_sector_rs(%s): date alignment failed — "
            "etf_rows=%s bench_rows=%s",
            sector_label,
            len(etf_df) if etf_df is not None else 0,
            len(bench_df) if bench_df is not None else 0,
        )
        return null

    etf_c, bench_c = aligned
    n = len(etf_c)
    min_required = lookback + smooth

    if n < min_required:
        logger.warning(
            "_compute_sector_rs(%s): too few aligned bars: "
            "n=%d required=%d",
            sector_label, n, min_required,
        )
        return null

    # Operate on numpy to avoid any pandas index re-alignment
    etf_vals = etf_c.values.astype(float)
    bench_vals = bench_c.values.astype(float)

    with np.errstate(divide="ignore", invalid="ignore"):
        rs_raw = etf_vals / bench_vals

    rs_ratio = pd.Series(rs_raw).replace([np.inf, -np.inf], np.nan).ffill().bfill()
    rs_smooth = rs_ratio.rolling(smooth, min_periods=1).mean()
    rs_mean = rs_smooth.rolling(lookback, min_periods=max(lookback // 2, 3)).mean()

    if pd.isna(rs_mean.iloc[-1]) or rs_mean.iloc[-1] <= 0:
        logger.warning(
            "_compute_sector_rs(%s): rs_mean[-1] invalid (%.6f)",
            sector_label,
            float(rs_mean.iloc[-1]) if pd.notna(rs_mean.iloc[-1]) else float("nan"),
        )
        return null

    rs_level = float(rs_smooth.iloc[-1] / rs_mean.iloc[-1] - 1.0)

    # ── RS Momentum ───────────────────────────────────────────────────────
    half = max(lookback // 2, 3)
    rs_mom = 0.0
    if (
        n > half
        and pd.notna(rs_mean.iloc[-half])
        and rs_mean.iloc[-half] > 0
    ):
        rs_level_prev = float(rs_smooth.iloc[-half] / rs_mean.iloc[-half] - 1.0)
        rs_mom = rs_level - rs_level_prev
    else:
        logger.warning(
            "_compute_sector_rs(%s): rs_mom fallback — n=%d half=%d",
            sector_label, n, half,
        )

    # ── Excess return ─────────────────────────────────────────────────────
    excess = 0.0
    if n >= lookback:
        e_ret = etf_vals[-1] / etf_vals[-lookback] - 1.0
        b_ret = bench_vals[-1] / bench_vals[-lookback] - 1.0
        if np.isfinite(e_ret) and np.isfinite(b_ret):
            excess = e_ret - b_ret

    logger.debug(
        "_compute_sector_rs(%s): n=%d rs_level=%.6f rs_mom=%.6f excess=%.6f",
        sector_label, n, rs_level, rs_mom, excess,
    )

    return {"rs_level": rs_level, "rs_mom": rs_mom, "excess_20d": excess}


def _rrg_quadrant(rs_level: float, rs_mom: float) -> str:
    """Classic RRG quadrant from sign of RS level and momentum."""
    if rs_level >= 0 and rs_mom >= 0:
        return "leading"
    if rs_level < 0 and rs_mom >= 0:
        return "improving"
    if rs_level >= 0 and rs_mom < 0:
        return "weakening"
    return "lagging"


def _rrg_to_score(rs_level: float, rs_mom: float) -> float:
    """Map RS level + momentum to a 0-1 score for blending with ETF composite."""
    level_norm = _normalize(rs_level, -0.08, 0.08)
    mom_norm   = _normalize(rs_mom,   -0.05, 0.05)
    return 0.60 * level_norm + 0.40 * mom_norm


# ═══════════════════════════════════════════════════════════════════════════════
# Blended regime classification
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_blended_regime(
    blended: float,
    rs_mom: float,
    etf_composite: float,
    thresholds: dict,
) -> str:
    """
    Strength × Direction matrix:

        strength tier      accelerating    decelerating
        ─────────────      ────────────    ────────────
        strong  (≥0.60)    leading         weakening
        moderate(≥0.42)    improving       weakening
        weak    (≥0.30)    improving       weakening
        very_weak(<0.30)   lagging         lagging
    """
    leading_min  = thresholds.get("leading_min",  0.60)
    moderate_min = thresholds.get("moderate_min",  0.42)
    weak_min     = thresholds.get("weak_min",      0.30)
    mom_thresh   = thresholds.get("mom_threshold", -0.008)
    etf_override = thresholds.get("etf_accel_override", 0.55)

    accelerating = rs_mom >= mom_thresh or (
        rs_mom >= -0.02 and etf_composite >= etf_override
    )

    if blended >= leading_min:
        return "leading" if accelerating else "weakening"
    if blended >= moderate_min:
        return "improving" if accelerating else "weakening"
    if blended >= weak_min:
        return "improving" if accelerating else "weakening"
    return "lagging"


# ═══════════════════════════════════════════════════════════════════════════════
# Constituent-based rotation  (non-US markets)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_sector_synthetic_close(
    tickers: list[str],
    all_frames: dict[str, pd.DataFrame],
    min_tickers: int = 2,
    min_bars: int = 25,
) -> pd.DataFrame | None:
    """
    Build an equal-weighted synthetic close for a group of tickers.

    Each constituent is normalized to base-100 before averaging so that
    different price levels don't dominate.  The result is a DataFrame
    with a single 'close' column and a DatetimeIndex — compatible with
    ``_align_close_series`` and ``_compute_sector_rs``.
    """
    normed: dict[str, pd.Series] = {}
    for t in tickers:
        df = all_frames.get(t)
        if df is None or df.empty or "close" not in df.columns:
            continue
        c = pd.to_numeric(df["close"], errors="coerce").dropna()
        if len(c) < min_bars:
            continue
        first_valid = c.iloc[0]
        if not np.isfinite(first_valid) or first_valid <= 0:
            continue
        normed[t] = c / first_valid * 100.0

    if len(normed) < min_tickers:
        return None

    combined = pd.DataFrame(normed)
    # Equal-weighted average across available tickers per date.
    # NaN columns on dates where a ticker doesn't trade are ignored.
    synthetic = combined.mean(axis=1).dropna()

    if len(synthetic) < min_bars:
        return None

    return pd.DataFrame({"close": synthetic})


def _compute_rotation_from_constituents(
    all_symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    market: str,
    params: dict,
) -> dict:
    """
    Compute sector rotation for non-US markets by grouping constituent
    stocks by sector, building an equal-weighted synthetic close per
    sector, and running the same RRG RS math against the local benchmark.

    ETF scoring is skipped entirely — ``etf_ranking`` is returned empty.
    """
    lookback   = params.get("rs_lookback", 20)
    smooth     = params.get("rs_smooth", 5)
    thresholds = params.get("regime_thresholds", {})

    # ── Group tickers by sector ───────────────────────────────────────────
    _skip_sectors = {"Unknown", "ETF", "Index", "Cash", "Other"}
    sector_tickers: dict[str, list[str]] = {}
    for ticker in all_symbol_frames:
        sector = get_sector_or_class(ticker) or "Unknown"
        if sector in _skip_sectors:
            continue
        sector_tickers.setdefault(sector, []).append(ticker)

    logger.info(
        "Constituent-based rotation for %s: %d sectors from %d tickers",
        market, len(sector_tickers), len(all_symbol_frames),
    )
    for sec in sorted(sector_tickers):
        logger.debug("  %s: %d tickers", sec, len(sector_tickers[sec]))

    # ── Per-sector: build synthetic close → RS → regime ───────────────────
    sector_rows: list[dict] = []
    sector_regimes: dict[str, str] = {}
    zero_mom_count = 0

    for sector in sorted(sector_tickers):
        tickers = sector_tickers[sector]
        synthetic_df = _build_sector_synthetic_close(
            tickers, all_symbol_frames, min_tickers=2, min_bars=lookback + smooth,
        )
        if synthetic_df is None:
            logger.debug(
                "Sector '%s' (%s): insufficient data for synthetic close "
                "(%d tickers, need ≥2 with ≥%d bars)",
                sector, market, len(tickers), lookback + smooth,
            )
            sector_regimes[sector] = "unknown"
            continue

        label = f"[{market}:{sector}]"
        rs = _compute_sector_rs(
            label, {}, bench_df, lookback, smooth,
            override_df=synthetic_df,
        )

        rrg_quad  = _rrg_quadrant(rs["rs_level"], rs["rs_mom"])
        rrg_score = _rrg_to_score(rs["rs_level"], rs["rs_mom"])

        # No ETF component — blended = pure RRG score.
        # Pass neutral etf_composite so the etf_accel_override doesn't fire.
        regime = _classify_blended_regime(
            rrg_score, rs["rs_mom"], 0.50, thresholds,
        )
        sector_regimes[sector] = regime

        if rs["rs_mom"] == 0.0:
            zero_mom_count += 1

        sector_rows.append({
            "sector":          sector,
            "etf":             f"({len(tickers)} names)",
            "regime":          regime,
            "rrg_quadrant":    rrg_quad,
            "blended_score":   round(rrg_score, 4),
            "rs_level":        round(rs["rs_level"], 4),
            "rs_mom":          round(rs["rs_mom"], 4),
            "excess_20d":      round(rs["excess_20d"], 4),
            "etf_composite":   float("nan"),
            "theme_avg_score": float("nan"),
            "n_constituents":  len(tickers),
        })

    n_sectors = len([s for s in sector_tickers if sector_regimes.get(s) != "unknown"])
    if zero_mom_count == n_sectors and n_sectors > 0:
        logger.error(
            "⚠️  ALL %d constituent sectors have rs_mom=0.0 — "
            "check frame alignment (%s)",
            n_sectors, market,
        )

    sector_summary = pd.DataFrame(sector_rows)
    if not sector_summary.empty:
        sector_summary = sector_summary.sort_values(
            "blended_score", ascending=False,
        ).reset_index(drop=True)

    # ── Map every ticker to its sector regime ─────────────────────────────
    ticker_regimes: dict[str, str] = {}
    for ticker in all_symbol_frames:
        sector = get_sector_or_class(ticker) or "Unknown"
        ticker_regimes[ticker] = sector_regimes.get(sector, "unknown")

    # ── Logging ───────────────────────────────────────────────────────────
    regime_counts: dict[str, int] = {}
    for r in sector_regimes.values():
        regime_counts[r] = regime_counts.get(r, 0) + 1
    logger.info(
        "Constituent rotation regimes (%s): %s", market, regime_counts,
    )
    if not sector_summary.empty:
        display_cols = [
            c for c in [
                "sector", "etf", "regime", "rrg_quadrant", "blended_score",
                "rs_level", "rs_mom", "excess_20d", "n_constituents",
            ] if c in sector_summary.columns
        ]
        logger.info(
            "Sector summary (%s):\n%s",
            market, sector_summary[display_cols].to_string(index=False),
        )

    return {
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
        "sector_summary": sector_summary,
        "etf_ranking":    pd.DataFrame(),   # no ETFs for non-US
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════════

def compute_sector_rotation(
    all_symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    market: str = "US",
    params: dict | None = None,
) -> dict:
    """
    Enhanced sector rotation combining:
      1. Traditional RRG-style RS analysis per sector ETF  (US)
         — or per synthetic sector close from constituents  (non-US)
      2. Composite scoring of the full ETF universe  (US only)
      3. Blended regime classification via strength × direction matrix

    Returns dict with keys:
      sector_regimes  – {sector: regime_str}
      ticker_regimes  – {ticker: regime_str}
      sector_summary  – DataFrame with full detail per sector
      etf_ranking     – DataFrame with every ETF scored and ranked
                        (empty for non-US markets)
    """
    params = params or ROTATIONPARAMS or {}

    # ── Dispatch: non-US markets use constituent-based rotation ───────────
    if market.upper() not in _US_LIKE_MARKETS:
        logger.info(
            "Market %s is not in %s — using constituent-based sector rotation",
            market, _US_LIKE_MARKETS,
        )
        return _compute_rotation_from_constituents(
            all_symbol_frames, bench_df, market, params,
        )

    # ══════════════════════════════════════════════════════════════════════
    #  US path (unchanged)
    # ══════════════════════════════════════════════════════════════════════
    lookback   = params.get("rs_lookback", 20)
    smooth     = params.get("rs_smooth", 5)
    etf_weight = params.get("etf_score_weight", 0.35)
    rs_weight  = params.get("rs_weight", 0.65)
    thresholds = params.get("regime_thresholds", {})

    logger.info(
        "compute_sector_rotation: market=%s lookback=%d smooth=%d "
        "rs_weight=%.2f etf_weight=%.2f bench_rows=%d",
        market, lookback, smooth, rs_weight, etf_weight,
        len(bench_df) if bench_df is not None else 0,
    )

    # ── 1. Score the full ETF universe ────────────────────────────────────────
    etf_ranking = score_etf_universe(all_symbol_frames, bench_df, params)

    etf_score_map: dict[str, float] = {}
    if not etf_ranking.empty:
        etf_score_map = dict(
            zip(etf_ranking["ticker"], etf_ranking["etf_composite"])
        )

    # Average thematic-ETF composite per parent sector
    sector_theme_avg: dict[str, float] = {}
    if not etf_ranking.empty:
        thematic = etf_ranking[
            ~etf_ranking["is_broad"]
            & ~etf_ranking["is_sector_etf"]
            & ~etf_ranking["is_regional"]
        ]
        if not thematic.empty:
            sector_theme_avg = (
                thematic.groupby("parent_sector")["etf_composite"]
                .mean()
                .to_dict()
            )

    logger.info(
        "Sector theme-ETF averages: %s",
        {k: round(v, 3) for k, v in sector_theme_avg.items()},
    )

    # ── 2. Per-sector: RS + ETF composite → blended regime ───────────────────
    sector_rows = []
    sector_regimes: dict[str, str] = {}
    zero_mom_count = 0

    for sector, etf_ticker in SECTOR_ETF.items():
        rs = _compute_sector_rs(
            etf_ticker, all_symbol_frames, bench_df, lookback, smooth
        )
        rrg_quad  = _rrg_quadrant(rs["rs_level"], rs["rs_mom"])
        rrg_score = _rrg_to_score(rs["rs_level"], rs["rs_mom"])

        etf_own   = etf_score_map.get(etf_ticker, 0.50)
        theme_avg = sector_theme_avg.get(sector, etf_own)

        etf_signal = 0.65 * etf_own + 0.35 * theme_avg

        blended = rs_weight * rrg_score + etf_weight * etf_signal

        regime = _classify_blended_regime(
            blended, rs["rs_mom"], etf_own, thresholds
        )
        sector_regimes[sector] = regime

        if rs["rs_mom"] == 0.0:
            zero_mom_count += 1

        sector_rows.append({
            "sector":          sector,
            "etf":             etf_ticker,
            "regime":          regime,
            "rrg_quadrant":    rrg_quad,
            "blended_score":   round(blended, 4),
            "rs_level":        round(rs["rs_level"], 4),
            "rs_mom":          round(rs["rs_mom"], 4),
            "excess_20d":      round(rs["excess_20d"], 4),
            "etf_composite":   round(etf_own, 4),
            "theme_avg_score": round(theme_avg, 4),
        })

    n_sectors = len(SECTOR_ETF)
    if zero_mom_count == n_sectors and n_sectors > 0:
        logger.error(
            "⚠️  ALL %d sectors have rs_mom=0.0 — check frame alignment",
            n_sectors,
        )

    sector_summary = pd.DataFrame(sector_rows)
    if not sector_summary.empty:
        sector_summary = sector_summary.sort_values(
            "blended_score", ascending=False
        ).reset_index(drop=True)

    # ── 3. Map every ticker to its sector regime ──────────────────────────────
    ticker_regimes: dict[str, str] = {}
    for ticker in all_symbol_frames:
        if ticker in ETF_TO_SECTOR:
            sec = ETF_TO_SECTOR[ticker]
        elif ticker in THEMATIC_ETF_SECTOR:
            sec = THEMATIC_ETF_SECTOR[ticker]
        else:
            sec = get_sector_or_class(ticker) or "Unknown"
        ticker_regimes[ticker] = sector_regimes.get(sec, "unknown")

    # ── 4. Logging ────────────────────────────────────────────────────────────
    regime_counts: dict[str, int] = {}
    for r in sector_regimes.values():
        regime_counts[r] = regime_counts.get(r, 0) + 1
    logger.info(
        "Sector rotation regimes: %s  (rs_weight=%.2f etf_weight=%.2f)",
        regime_counts, rs_weight, etf_weight,
    )

    if not sector_summary.empty:
        logger.info(
            "Sector summary:\n%s",
            sector_summary[[
                "sector", "etf", "regime", "rrg_quadrant", "blended_score",
                "rs_level", "rs_mom", "etf_composite", "theme_avg_score",
            ]].to_string(index=False),
        )

    return {
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
        "sector_summary": sector_summary,
        "etf_ranking":    etf_ranking,
    }

###############################################
"""
refactor/strategy/rs_v2.py

Cross-sectional relative-strength z-score computation and RRG-style regime classification.

Step 1 – compute_rs_zscores():
    Builds a date × symbol panel of rolling log returns, subtracts the
    benchmark return to get relative returns, then z-scores across the
    full cross-section on every date.  Writes rszscore, rsaccel20, and
    sectrszscore back into each per-symbol frame.

Step 2 – enrich_rs_regimes():
    Applies quadrant logic (level × momentum) to classify each symbol's
    rszscore trajectory into one of four regimes: leading, improving,
    weakening, lagging.  Same for sectrszscore → sectrsregime.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
RS_RETURN_LOOKBACK = 20     # rolling return window (trading days)
RS_ACCEL_DIFF = 5           # window for Δ(rszscore) → rsaccel20
RS_REGIME_MA_WINDOW = 10    # rolling-MA window for quadrant classification
RS_MIN_HISTORY = 30         # minimum rows for a symbol to enter the panel


# ═══════════════════════════════════════════════════════════════════════════════
# Step 1 – cross-sectional z-scores
# ═══════════════════════════════════════════════════════════════════════════════

def compute_rs_zscores(
    symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    lookback: int = RS_RETURN_LOOKBACK,
    accel_diff: int = RS_ACCEL_DIFF,
    min_history: int = RS_MIN_HISTORY,
) -> dict[str, pd.DataFrame]:
    """
    Compute cross-sectional relative-strength z-scores for every symbol.

    For each trading date with sufficient data:
      1. Compute ``lookback``-period log return for the benchmark.
      2. Compute ``lookback``-period log return for each symbol.
      3. relative_return = symbol_return − benchmark_return
      4. Z-score relative returns across the symbol cross-section.
      5. rsaccel20 = diff(rszscore, ``accel_diff``) — short-term RS momentum.
      6. If ≥ 2 real sectors exist, compute sector-level z-scores.

    Returns a new dict with the same keys.  Each frame is a copy of the
    original with ``rszscore``, ``rsaccel20``, and ``sectrszscore``
    columns added or overwritten.
    """
    if not symbol_frames:
        logger.warning("compute_rs_zscores: no symbol frames provided; returning unchanged")
        return symbol_frames

    # ── 1. benchmark return series ────────────────────────────────────────────
    bench_close = pd.to_numeric(bench_df["close"], errors="coerce")
    bench_ret = np.log(bench_close / bench_close.shift(lookback))
    logger.info(
        "compute_rs_zscores: bench_rows=%d lookback=%d bench_ret_valid=%d",
        len(bench_df), lookback, int(bench_ret.notna().sum()),
    )

    # ── 2. build close-price panel and sector map ─────────────────────────────
    close_dict: dict[str, pd.Series] = {}
    sector_dict: dict[str, str] = {}

    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            continue
        if len(df) < min_history:
            logger.debug(
                "compute_rs_zscores: excluding %s (rows=%d < min_history=%d)",
                ticker, len(df), min_history,
            )
            continue
        close_dict[ticker] = pd.to_numeric(df["close"], errors="coerce")
        if "sector" in df.columns:
            vals = df["sector"].dropna().astype(str)
            sector_dict[ticker] = str(vals.iloc[-1]) if not vals.empty else "Unknown"
        else:
            sector_dict[ticker] = "Unknown"

    if not close_dict:
        logger.warning("compute_rs_zscores: no symbols with valid close data; returning unchanged")
        return {t: f.copy() for t, f in symbol_frames.items()}

    close_panel = pd.DataFrame(close_dict)
    logger.info(
        "compute_rs_zscores: close_panel shape=(%d dates, %d symbols)",
        close_panel.shape[0], close_panel.shape[1],
    )

    # ── 3. symbol log-returns and relative returns ────────────────────────────
    symbol_ret = np.log(close_panel / close_panel.shift(lookback))
    bench_ret_aligned = bench_ret.reindex(symbol_ret.index)
    relative_ret = symbol_ret.sub(bench_ret_aligned, axis=0)

    # ── 4. cross-sectional z-score per date ───────────────────────────────────
    cross_mean = relative_ret.mean(axis=1)
    cross_std = relative_ret.std(axis=1, ddof=1)
    cross_std = cross_std.where(cross_std > 1e-10, np.nan)
    zscore_panel = relative_ret.sub(cross_mean, axis=0).div(cross_std, axis=0)

    # ── 5. RS acceleration ────────────────────────────────────────────────────
    rs_accel_panel = zscore_panel.diff(accel_diff)

    # ── 6. sector-level z-scores ──────────────────────────────────────────────
    sector_zscore_panel = _compute_sector_zscores(relative_ret, sector_dict)

    # ── 7. write back into per-symbol frames ──────────────────────────────────
    enriched: dict[str, pd.DataFrame] = {}
    for ticker, df in symbol_frames.items():
        out = df.copy()
        if ticker in zscore_panel.columns:
            out["rszscore"] = zscore_panel[ticker].reindex(out.index)
            out["rsaccel20"] = rs_accel_panel[ticker].reindex(out.index)
        else:
            if "rszscore" not in out.columns:
                out["rszscore"] = np.nan
            if "rsaccel20" not in out.columns:
                out["rsaccel20"] = 0.0

        if sector_zscore_panel is not None and ticker in sector_zscore_panel.columns:
            out["sectrszscore"] = sector_zscore_panel[ticker].reindex(out.index)
        elif "sectrszscore" not in out.columns:
            out["sectrszscore"] = np.nan

        enriched[ticker] = out

    # ── diagnostics ───────────────────────────────────────────────────────────
    valid_per_date = zscore_panel.notna().sum(axis=1)
    last_zscores = (
        zscore_panel.iloc[-1].dropna()
        if not zscore_panel.empty
        else pd.Series(dtype=float)
    )
    logger.info(
        "compute_rs_zscores: panel_dates=%d symbols_in_panel=%d "
        "avg_valid_per_date=%.1f last_date_valid=%d",
        len(zscore_panel),
        len(close_dict),
        float(valid_per_date.mean()) if not valid_per_date.empty else 0.0,
        len(last_zscores),
    )
    if not last_zscores.empty:
        logger.info(
            "compute_rs_zscores last-date z-scores: "
            "min=%.4f p25=%.4f median=%.4f p75=%.4f max=%.4f std=%.4f",
            float(last_zscores.min()),
            float(last_zscores.quantile(0.25)),
            float(last_zscores.median()),
            float(last_zscores.quantile(0.75)),
            float(last_zscores.max()),
            float(last_zscores.std()),
        )
        if logger.isEnabledFor(logging.DEBUG):
            ranked = last_zscores.sort_values(ascending=False)
            logger.debug("compute_rs_zscores top-10 RS:\n%s", ranked.head(10).to_string())
            logger.debug("compute_rs_zscores bottom-10 RS:\n%s", ranked.tail(10).to_string())

    last_accel = (
        rs_accel_panel.iloc[-1].dropna()
        if not rs_accel_panel.empty
        else pd.Series(dtype=float)
    )
    if not last_accel.empty:
        logger.info(
            "compute_rs_zscores last-date rsaccel: "
            "min=%.4f median=%.4f max=%.4f",
            float(last_accel.min()),
            float(last_accel.median()),
            float(last_accel.max()),
        )

    return enriched


def _compute_sector_zscores(
    relative_ret: pd.DataFrame,
    sector_dict: dict[str, str],
) -> pd.DataFrame | None:
    """
    For each date, average the relative returns within each sector, then
    z-score the sector averages across sectors.  Map each symbol to its
    sector's z-score.

    Returns ``None`` if fewer than 2 real (non-Unknown) sectors exist.
    """
    if not sector_dict:
        return None

    sectors = pd.Series(sector_dict)
    real_sectors = sorted(
        s for s in sectors.unique() if s not in ("Unknown", "unknown", "")
    )
    if len(real_sectors) < 2:
        logger.info(
            "_compute_sector_zscores: %d real sector(s) found; need >= 2, skipping",
            len(real_sectors),
        )
        return None

    # per-sector average relative return per date
    sector_avg_dict: dict[str, pd.Series] = {}
    for sector in sectors.unique():
        members = sectors[sectors == sector].index.tolist()
        members_in_panel = [m for m in members if m in relative_ret.columns]
        if members_in_panel:
            sector_avg_dict[sector] = relative_ret[members_in_panel].mean(axis=1)

    sector_avg = pd.DataFrame(sector_avg_dict)
    if sector_avg.shape[1] < 2:
        return None

    sect_mean = sector_avg.mean(axis=1)
    sect_std = sector_avg.std(axis=1, ddof=1).replace(0, np.nan)
    sector_zscore = sector_avg.sub(sect_mean, axis=0).div(sect_std, axis=0)

    # map each symbol to its sector's z-score
    result = pd.DataFrame(index=relative_ret.index)
    for ticker, sector in sector_dict.items():
        if sector in sector_zscore.columns:
            result[ticker] = sector_zscore[sector]
        else:
            result[ticker] = np.nan

    logger.info(
        "_compute_sector_zscores: sectors=%d symbols_mapped=%d",
        len(sector_zscore.columns), len(result.columns),
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# Step 2 – RRG-style regime classification
# ═══════════════════════════════════════════════════════════════════════════════

def classify_rs_regime(
    df: pd.DataFrame,
    rs_col: str = "rszscore",
    regime_col: str = "rsregime",
    ma_window: int = RS_REGIME_MA_WINDOW,
) -> pd.DataFrame:
    """
    Classify a single symbol's RS trajectory using quadrant logic.

    The two axes are *level* (above/below zero) and *momentum*
    (above/below a rolling MA of the z-score):

    ============  =============================  ==============================
    Quadrant      Condition                      Interpretation
    ============  =============================  ==============================
    leading       rs > 0  AND  rs > MA(rs)       strong & strengthening
    improving     rs <= 0 AND  rs > MA(rs)       weak but strengthening
    weakening     rs > 0  AND  rs <= MA(rs)      strong but fading
    lagging       rs <= 0 AND  rs <= MA(rs)      weak & fading
    ============  =============================  ==============================

    Rows where the z-score or its MA is NaN are labelled ``'unknown'``.
    """
    out = df.copy()

    if rs_col not in out.columns:
        out[regime_col] = "unknown"
        return out

    rs = pd.to_numeric(out[rs_col], errors="coerce")
    rs_ma = rs.rolling(ma_window, min_periods=max(3, ma_window // 2)).mean()

    above_zero = rs > 0
    above_ma = rs > rs_ma

    regime = pd.Series(
        np.select(
            [
                above_zero & above_ma,
                ~above_zero & above_ma,
                above_zero & ~above_ma,
                ~above_zero & ~above_ma,
            ],
            ["leading", "improving", "weakening", "lagging"],
            default="unknown",
        ),
        index=out.index,
    )

    # rows where the underlying data isn't ready → unknown
    regime = regime.where(rs.notna(), "unknown")
    regime = regime.where(rs_ma.notna(), "unknown")

    out[regime_col] = regime
    return out


def enrich_rs_regimes(
    symbol_frames: dict[str, pd.DataFrame],
    ma_window: int = RS_REGIME_MA_WINDOW,
) -> dict[str, pd.DataFrame]:
    """
    Apply RS regime classification to every symbol frame.

    Adds / overwrites:
        rsregime      — stock-level RS quadrant   (from rszscore)
        sectrsregime  — sector-level RS quadrant   (from sectrszscore)
    """
    enriched: dict[str, pd.DataFrame] = {}
    rs_counts: dict[str, int] = {}
    sect_counts: dict[str, int] = {}

    for ticker, df in symbol_frames.items():
        out = classify_rs_regime(
            df, rs_col="rszscore", regime_col="rsregime", ma_window=ma_window,
        )
        out = classify_rs_regime(
            out, rs_col="sectrszscore", regime_col="sectrsregime", ma_window=ma_window,
        )
        enriched[ticker] = out

        if not out.empty:
            last_rs = str(out["rsregime"].iloc[-1])
            last_sect = str(out["sectrsregime"].iloc[-1])
            rs_counts[last_rs] = rs_counts.get(last_rs, 0) + 1
            sect_counts[last_sect] = sect_counts.get(last_sect, 0) + 1

    logger.info(
        "enrich_rs_regimes: symbols=%d last-row rsregime=%s",
        len(enriched), rs_counts,
    )
    logger.info("enrich_rs_regimes: last-row sectrsregime=%s", sect_counts)
    return enriched

################################################
"""
refactor/strategy/scoring_v2.py

Composite scoring for the V2 pipeline.

Revision notes
--------------
- **UNIT FIX**: closevsema30pct and closevssma50pct are in PERCENTAGE-POINT
  units (e.g., 5.0 = "5% above the moving average"). Scaling ranges corrected.
- **Risk differentiation**: adaptive proxies for missing columns.
- **Participation weight redistribution**: dead columns have weight
  redistributed to live components.
- **Regime differentiation**: since scoreregime is constant across all stocks,
  its weight is reduced and redistributed to differentiating components.
- **RS momentum**: rs_accel scaling widened and weighted higher.
- **Score floor warning**: diagnostic emits warning when composite std < 0.08.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from refactor.common.config_refactor import SCORINGWEIGHTS_V2, SCORINGPARAMS_V2

logger = logging.getLogger(__name__)

# Below this std, fixed-threshold scaling is considered "flat" and
# the adaptive helpers fall back to cross-sectional percentile rank.
_ADAPTIVE_MIN_STD = 0.02

# Minimum acceptable composite std — below this we log a warning
_MIN_COMPOSITE_STD = 0.06


# ── Utility helpers ──────────────────────────────────────────────────────

def _s(x, lo, hi):
    """Scale *x* linearly from *lo* → 0 to *hi* → 1, clamped [0, 1]."""
    return pd.Series(np.clip((x - lo) / (hi - lo), 0.0, 1.0), index=x.index)


def _inv(x, lo, hi):
    """Inverse of :func:`_s`: *lo* → 1 (best) to *hi* → 0 (worst)."""
    return 1.0 - _s(x, lo, hi)


def _col_or_default(df: pd.DataFrame, name: str, default: float = 0.0):
    """Return ``(series, has_variance: bool)`` for column *name*."""
    if name in df.columns:
        s = df[name].fillna(default)
        return s, float(s.std()) > 1e-10
    return pd.Series(default, index=df.index, dtype=float), False


def _adaptive_inv(x: pd.Series, lo: float, hi: float, *, label: str = ""):
    """
    Inverse-scale with percentile-rank fallback.

    Primary path: ``1 − clip((x−lo)/(hi−lo), 0, 1)`` (lower x → higher score).
    Fallback: if the fixed mapping has std < ``_ADAPTIVE_MIN_STD``, use
    ``1 − rank(pct=True)`` so the component still differentiates stocks.
    """
    fixed = _inv(x, lo, hi)
    if float(fixed.std()) < _ADAPTIVE_MIN_STD and len(x) > 10:
        ranked = 1.0 - x.rank(pct=True).fillna(0.5)
        if label:
            logger.debug(
                "_adaptive_inv(%s): fixed std=%.4f < %.4f → rank fallback",
                label, float(fixed.std()), _ADAPTIVE_MIN_STD,
            )
        return ranked
    return fixed


def _adaptive_fwd(x: pd.Series, lo: float, hi: float, *, label: str = ""):
    """
    Forward-scale with percentile-rank fallback.

    Primary path: ``clip((x−lo)/(hi−lo), 0, 1)`` (higher x → higher score).
    Fallback: if the fixed mapping has std < ``_ADAPTIVE_MIN_STD``, use
    ``rank(pct=True)`` for differentiation.
    """
    fixed = _s(x, lo, hi)
    if float(fixed.std()) < _ADAPTIVE_MIN_STD and len(x) > 10:
        ranked = x.rank(pct=True).fillna(0.5)
        if label:
            logger.debug(
                "_adaptive_fwd(%s): fixed std=%.4f < %.4f → rank fallback",
                label, float(fixed.std()), _ADAPTIVE_MIN_STD,
            )
        return ranked
    return fixed


# ── Main scoring function ────────────────────────────────────────────────

def compute_composite_v2(
    df: pd.DataFrame,
    weights=None,
    params=None,
    *,
    market_breadth_score: float | None = None,
    market_vol_regime_score: float | None = None,
) -> pd.DataFrame:
    """
    Compute the V2 composite score for every row (stock) in *df*.

    Parameters
    ----------
    df : DataFrame
        Per-stock indicators and RS data.
    weights, params : dict, optional
        Override default scoring weights / params.
    market_breadth_score : float, optional
        Market-wide breadth score (0–1).  The breadth pipeline produces
        a single scalar per date — this is where the runner should pass
        it so that scoreregime actually differentiates from 0.5.
        If None, falls back to the per-row ``breadthscore`` column
        (which is almost always the ensure_columns default of 0.5).
    market_vol_regime_score : float, optional
        Market-wide volatility-regime score (0–1, higher = more volatile).
        If None, falls back to the per-row ``volregimescore`` column.
    """
    p = params if params is not None else SCORINGPARAMS_V2
    w = weights if weights is not None else SCORINGWEIGHTS_V2
    out = df.copy()

    # ══════════════════════════════════════════════════════════════════════
    #  TREND COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    #
    # All sub-components should produce meaningful spread across the
    # universe.  closevsema30pct is in PERCENTAGE-POINT units:
    #   5.0 = stock is 5% above EMA30
    #  -3.0 = stock is 3% below EMA30
    #
    # rszscore is a standard z-score (mean ~0, std ~1).

    stock_rs = _s(out["rszscore"].fillna(0), -1.5, 2.5)
    sector_rs = _s(
        out.get("sectrszscore", pd.Series(0, index=out.index)).fillna(0),
        -1.5, 2.5,
    )
    rs_accel = _s(
        out.get("rsaccel20", pd.Series(0, index=out.index)).fillna(0),
        -0.10, 0.15,
    )

    # ── FIXED: closevsema30pct in PERCENTAGE-POINT units ──────────────
    # Old (broken): _s(x, -0.03, 0.10) → everything above 0.1% clips to 1.0
    # New (correct): _s(x, -3.0, 8.0) → smooth differentiation from
    #   "3% below EMA" (score=0) to "8% above EMA" (score=1).
    #   A stock exactly at EMA30 (x=0) → score = 3.0/11.0 ≈ 0.27
    #   A stock 3% above EMA (x=3) → score = 6.0/11.0 ≈ 0.55
    #   A stock 6% above EMA (x=6) → score = 9.0/11.0 ≈ 0.82
    trend_confirm = _adaptive_fwd(
        out.get("closevsema30pct", pd.Series(0, index=out.index)).fillna(0),
        -3.0, 8.0,
        label="trend_confirm",
    )

    out["scoretrend"] = (
        p["trend"]["w_stock_rs"] * stock_rs
        + p["trend"]["w_sector_rs"] * sector_rs
        + p["trend"]["w_rs_accel"] * rs_accel
        + p["trend"]["w_trend_confirm"] * trend_confirm
    ).clip(0, 1)

    # Trend diagnostics
    _st = out["scoretrend"]
    logger.info(
        "scoretrend: min=%.4f p10=%.4f med=%.4f p90=%.4f max=%.4f std=%.4f | "
        "sub-components: stock_rs std=%.4f sector_rs std=%.4f "
        "rs_accel std=%.4f trend_confirm std=%.4f",
        _st.min(), float(_st.quantile(0.10)), _st.median(),
        float(_st.quantile(0.90)), _st.max(), float(_st.std()),
        float(stock_rs.std()), float(sector_rs.std()),
        float(rs_accel.std()), float(trend_confirm.std()),
    )
    if float(trend_confirm.std()) < _ADAPTIVE_MIN_STD:
        logger.warning(
            "trend_confirm has near-zero std (%.4f) — closevsema30pct may "
            "not be populating. Check upstream data.",
            float(trend_confirm.std()),
        )

    # ══════════════════════════════════════════════════════════════════════
    #  PARTICIPATION COMPONENT (adaptive, with weight redistribution)
    # ══════════════════════════════════════════════════════════════════════
    #
    # When obvslope10d and/or adlineslope10d have zero variance (all-zero
    # because the data source doesn't populate them), their weights are
    # dead — every stock gets the same constant contribution (~0.294).
    # This collapses scoreparticipation differentiation to just rvol and
    # dvol, producing the 0.297-cluster seen in diagnostics.
    #
    # Fix: detect dead components, redistribute their weight proportionally
    # to the live components (rvol, dvol), and use _adaptive_fwd for the
    # live components so rank-based fallback is available if needed.

    rvol_raw = out.get(
        "relativevolume", pd.Series(1, index=out.index),
    ).fillna(1)

    obv_raw, obv_col_ok = _col_or_default(out, "obvslope10d", 0.0)
    adl_raw, adl_col_ok = _col_or_default(out, "adlineslope10d", 0.0)

    dvol_raw_input = out.get(
        "dollarvolume20d", pd.Series(0, index=out.index),
    ).fillna(0)
    dvol_raw = pd.Series(np.log1p(dvol_raw_input), index=out.index)

    # Check if OBV and ADL have meaningful variance
    obv_has_variance = obv_col_ok and float(obv_raw.std()) > 1e-8
    adl_has_variance = adl_col_ok and float(adl_raw.std()) > 1e-8

    # Base weights from params
    w_rvol = p["participation"]["w_rvol"]
    w_obv = p["participation"]["w_obv"]
    w_adl = p["participation"]["w_adline"]
    w_dvol = p["participation"]["w_dollar_volume"]

    # Redistribute dead weights to live components
    dead_weight = 0.0
    _part_sources = []

    if obv_has_variance:
        _part_sources.append("obvslope10d")
    else:
        dead_weight += w_obv
        w_obv = 0.0

    if adl_has_variance:
        _part_sources.append("adlineslope10d")
    else:
        dead_weight += w_adl
        w_adl = 0.0

    _part_sources.extend(["relativevolume", "dollarvolume20d"])

    if dead_weight > 0:
        live_total = w_rvol + w_dvol
        if live_total > 0:
            w_rvol += dead_weight * (w_rvol / live_total)
            w_dvol += dead_weight * (w_dvol / live_total)
        else:
            # Edge case: all components dead — split evenly
            w_rvol = 0.5
            w_dvol = 0.5
        logger.info(
            "scoreparticipation: redistributed %.3f dead weight "
            "(obv_ok=%s adl_ok=%s) → w_rvol=%.3f w_obv=%.3f "
            "w_adl=%.3f w_dvol=%.3f",
            dead_weight, obv_has_variance, adl_has_variance,
            w_rvol, w_obv, w_adl, w_dvol,
        )

    # Scale live components (adaptive for rvol/dvol, fixed for obv/adl)
    rvol = _adaptive_fwd(rvol_raw, 0.8, 2.2, label="rvol")
    dvol = _adaptive_fwd(dvol_raw, 10, 18, label="dvol")
    obv = (
        _s(obv_raw, -0.05, 0.12)
        if obv_has_variance
        else pd.Series(0.0, index=out.index)
    )
    adl = (
        _s(adl_raw, -0.05, 0.12)
        if adl_has_variance
        else pd.Series(0.0, index=out.index)
    )

    out["scoreparticipation"] = (
        w_rvol * rvol
        + w_obv * obv
        + w_adl * adl
        + w_dvol * dvol
    ).clip(0, 1)

    # Participation diagnostics
    _sp = out["scoreparticipation"]
    logger.info(
        "scoreparticipation: sources=%s  min=%.4f med=%.4f max=%.4f "
        "std=%.4f uniq=%d  weights=[rvol=%.3f obv=%.3f adl=%.3f dvol=%.3f]",
        _part_sources,
        _sp.min(), _sp.median(), _sp.max(),
        float(_sp.std()), int(_sp.nunique()),
        w_rvol, w_obv, w_adl, w_dvol,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  RISK COMPONENT (adaptive, with proxy fallbacks)
    # ══════════════════════════════════════════════════════════════════════
    _risk_sources = []

    # — Sub-component 1: Volatility —
    atrp, atrp_ok = _col_or_default(out, "atr14pct", 0.0)
    if atrp_ok:
        vol_pen = _adaptive_inv(
            atrp, 0.01, p["penalties"]["atrp_high"], label="atrp",
        )
        _risk_sources.append("atr14pct")
    else:
        # Proxy: higher relative volume ≈ higher short-term risk
        _rvol_raw = out.get(
            "relativevolume", pd.Series(1.0, index=out.index),
        ).fillna(1.0)
        vol_pen = _adaptive_inv(_rvol_raw, 0.5, 2.5, label="rvol_proxy")
        _risk_sources.append("relativevolume(proxy)")

    # — Sub-component 2: Liquidity —
    illiq, illiq_ok = _col_or_default(out, "amihud20", 0.0)
    if illiq_ok:
        liq_pen = _adaptive_inv(
            illiq, 0.0, p["penalties"]["illiquidity_bad"], label="illiq",
        )
        _risk_sources.append("amihud20")
    else:
        # Proxy: higher dollar volume → more liquid → safer
        _dv = out.get(
            "dollarvolume20d", pd.Series(0, index=out.index),
        ).fillna(0)
        _log_dv = pd.Series(np.log1p(_dv), index=out.index)
        liq_pen = _adaptive_fwd(_log_dv, 12.0, 17.0, label="dvol_proxy")
        _risk_sources.append("dollarvolume20d(proxy)")

    # — Sub-component 3: Gap / tail risk —
    gap, gap_ok = _col_or_default(out, "gaprate20", 0.0)
    if gap_ok:
        gap_pen = _adaptive_inv(gap, 0.05, 0.30, label="gap")
        _risk_sources.append("gaprate20")
    else:
        # Proxy: RSI distance from neutral 50 (extreme readings ≈ tail risk)
        _rsi = out.get("rsi14", pd.Series(50, index=out.index)).fillna(50)
        _rsi_dist = (_rsi - 50).abs()
        gap_pen = _adaptive_inv(_rsi_dist, 5.0, 35.0, label="rsi_dist_proxy")
        _risk_sources.append("rsi_dist(proxy)")

    # — Sub-component 4: Extension from moving average —
    #   FIXED: closevssma50pct is in PERCENTAGE-POINT units, same as
    #   closevsema30pct.  e.g., 8.0 = "8% above SMA50".
    #
    #   Old (broken): ext_warn=0.08, ext_bad=0.20 (decimal, caught nothing)
    #   New (correct): warn at 8 pp, dangerous at 20 pp.
    ext_raw = (
        out.get("closevssma50pct", pd.Series(0, index=out.index))
        .fillna(0).abs()
    )
    # Use percentage-point scaling: 4 pp = starts getting risky, 20 pp = very extended
    ext_warn_pct = p["penalties"].get("extension_warn_pct", 4.0)
    ext_bad_pct = p["penalties"].get("extension_bad_pct", 20.0)
    ext_pen = _adaptive_inv(
        ext_raw, ext_warn_pct, ext_bad_pct, label="extension",
    )
    _risk_sources.append("closevssma50pct")

    out["scorerisk"] = (
        p["risk"]["w_vol_penalty"] * vol_pen
        + p["risk"]["w_liquidity_penalty"] * liq_pen
        + p["risk"]["w_gap_penalty"] * gap_pen
        + p["risk"]["w_extension_penalty"] * ext_pen
    ).clip(0, 1)

    # Risk diagnostics
    _sr = out["scorerisk"]
    logger.info(
        "scorerisk: sources=%s  min=%.4f med=%.4f max=%.4f std=%.4f uniq=%d",
        _risk_sources, _sr.min(), _sr.median(), _sr.max(),
        float(_sr.std()), int(_sr.nunique()),
    )
    if float(_sr.std()) < _ADAPTIVE_MIN_STD:
        logger.warning(
            "scorerisk still has near-zero variance (std=%.4f) after "
            "adaptive fallbacks — check risk inputs: %s",
            float(_sr.std()), _risk_sources,
        )

    # ══════════════════════════════════════════════════════════════════════
    #  REGIME COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    #
    # IMPORTANT: This component is a MARKET-LEVEL scalar (identical for
    # all stocks on a given day). It does NOT differentiate stocks cross-
    # sectionally. Its purpose is to raise/lower the entire composite on
    # days with favorable/unfavorable market conditions.
    #
    # Because it adds no cross-sectional spread, its weight should be
    # modest (10-15%). If it's weighted too heavily, it compresses the
    # differentiating components' contribution.

    if market_breadth_score is not None:
        breadth = pd.Series(
            float(market_breadth_score), index=out.index, dtype=float,
        )
        logger.info(
            "scoreregime: using market_breadth_score=%.4f (from kwarg)",
            market_breadth_score,
        )
    else:
        breadth = out.get(
            "breadthscore", pd.Series(0.5, index=out.index)
        ).fillna(0.5)
        if float(breadth.std()) < 1e-8:
            logger.warning(
                "scoreregime: breadthscore is constant (%.4f) — "
                "pass market_breadth_score kwarg to fix this",
                float(breadth.iloc[0]),
            )

    if market_vol_regime_score is not None:
        volreg = pd.Series(
            float(market_vol_regime_score), index=out.index, dtype=float,
        )
        logger.info(
            "scoreregime: using market_vol_regime_score=%.4f (from kwarg)",
            market_vol_regime_score,
        )
    else:
        volreg = out.get(
            "volregimescore", pd.Series(0.0, index=out.index)
        ).fillna(0.0)
        if float(volreg.std()) < 1e-8:
            logger.warning(
                "scoreregime: volregimescore is constant (%.4f) — "
                "pass market_vol_regime_score kwarg to fix this",
                float(volreg.iloc[0]),
            )

    # Dampened vol-favorability mapping
    #   vol_regime_score semantics: 0.0 = calm, ~0.5 = elevated, 1.0 = chaotic
    #   For long-biased strategies, lower vol is favorable — but we dampen
    #   the mapping so "calm" is favorable without being maximal:
    #
    #     0.10 (calm)     → 0.64  (favorable)
    #     0.35 (normal)   → 0.49  (neutral)
    #     0.60 (elevated) → 0.34  (mildly unfavorable)
    #     1.0  (chaotic)  → 0.10  (very unfavorable)
    vol_favorable = (0.70 - 0.60 * volreg).clip(0.10, 0.70)
    out["volfavorability"] = vol_favorable

    out["scoreregime"] = (
        p["regime"]["w_breadth"] * breadth
        + p["regime"]["w_vol_regime"] * vol_favorable
    ).clip(0, 1)

    logger.info(
        "scoreregime: breadth=%.4f volreg=%.4f vol_favorable=%.4f "
        "→ scoreregime=%.4f  (constant across all %d stocks)",
        float(breadth.iloc[0]),
        float(volreg.iloc[0]),
        float(vol_favorable.iloc[0]),
        float(out["scoreregime"].iloc[0]),
        len(out),
    )

    # Stamp actual market-level values onto per-row columns
    if market_breadth_score is not None:
        out["breadthscore"] = float(market_breadth_score)
    if market_vol_regime_score is not None:
        out["volregimescore"] = float(market_vol_regime_score)

    # ══════════════════════════════════════════════════════════════════════
    #  ROTATION COMPONENT
    # ══════════════════════════════════════════════════════════════════════
    sect_regime = (
        out.get("sectrsregime", pd.Series("unknown", index=out.index))
        .fillna("unknown")
        .astype(str)
        .str.lower()
        .str.strip()
    )
    out["scorerotation"] = pd.Series(
        np.select(
            [
                sect_regime == "leading",
                sect_regime == "improving",
                sect_regime == "weakening",
                sect_regime == "lagging",
            ],
            [1.0, 0.70, 0.40, 0.15],
            default=0.30,
        ),
        index=out.index,
    )

    # ══════════════════════════════════════════════════════════════════════
    #  COMPOSITE SCORE
    # ══════════════════════════════════════════════════════════════════════
    rotation_weight = w.get("rotation", 0.0)
    if rotation_weight == 0.0:
        logger.warning(
            "compute_composite_v2: rotation weight is 0.0 — "
            "scorerotation will not affect composite. "
            "Add 'rotation' to SCORINGWEIGHTS_V2."
        )

    # Compute effective weights for diagnostics
    total_weight = (
        w["trend"] + w["participation"] + w["risk"]
        + w["regime"] + rotation_weight
    )
    differentiating_weight = (
        w["trend"] + w["participation"] + w["risk"] + rotation_weight
    )
    regime_weight = w["regime"]

    logger.info(
        "Composite weights: trend=%.3f participation=%.3f risk=%.3f "
        "regime=%.3f rotation=%.3f | total=%.3f | "
        "differentiating=%.1f%% constant(regime)=%.1f%%",
        w["trend"], w["participation"], w["risk"],
        regime_weight, rotation_weight,
        total_weight,
        100 * differentiating_weight / max(total_weight, 1e-9),
        100 * regime_weight / max(total_weight, 1e-9),
    )

    composite = (
        w["trend"] * out["scoretrend"]
        + w["participation"] * out["scoreparticipation"]
        + w["risk"] * out["scorerisk"]
        + w["regime"] * out["scoreregime"]
        + rotation_weight * out["scorerotation"]
    )

    # ══════════════════════════════════════════════════════════════════════
    #  PENALTIES
    # ══════════════════════════════════════════════════════════════════════
    rsi = out.get("rsi14", pd.Series(50, index=out.index)).fillna(50)
    adx = out.get("adx14", pd.Series(20, index=out.index)).fillna(20)
    rsi_low = p["penalties"]["rsi_soft_low"]
    rsi_high = p["penalties"]["rsi_soft_high"]

    rsi_penalty = pd.Series(
        np.where(
            rsi < rsi_low,
            (rsi_low - rsi) / 30.0,
            np.where(rsi > rsi_high, (rsi - rsi_high) / 30.0, 0.0),
        ),
        index=out.index,
    ).clip(0, 0.15)
    adx_penalty = pd.Series(
        np.where(
            adx < p["penalties"]["adx_soft_min"],
            (p["penalties"]["adx_soft_min"] - adx) / 30.0,
            0.0,
        ),
        index=out.index,
    ).clip(0, 0.10)

    out["scorepenalty"] = (rsi_penalty + adx_penalty).clip(0, 0.20)
    out["scorecomposite_v2"] = (composite - out["scorepenalty"]).clip(0, 1)

    # ══════════════════════════════════════════════════════════════════════
    #  DIAGNOSTIC SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    _sc = out["scorecomposite_v2"]
    composite_std = float(_sc.std())
    composite_iqr = float(_sc.quantile(0.90) - _sc.quantile(0.10))

    logger.info(
        "scorecomposite_v2: min=%.4f p10=%.4f p25=%.4f med=%.4f "
        "p75=%.4f p90=%.4f max=%.4f | std=%.4f iqr_90_10=%.4f",
        _sc.min(),
        float(_sc.quantile(0.10)),
        float(_sc.quantile(0.25)),
        _sc.median(),
        float(_sc.quantile(0.75)),
        float(_sc.quantile(0.90)),
        _sc.max(),
        composite_std,
        composite_iqr,
    )

    if composite_std < _MIN_COMPOSITE_STD:
        logger.warning(
            "SCORE COMPRESSION: composite std=%.4f < %.4f — scores are "
            "too clustered for effective differentiation. The top-ranked "
            "stock (%.4f) is only %.4f above median (%.4f). This means "
            "entry/exit thresholds must be razor-thin or nothing qualifies. "
            "Check: (1) trend_confirm getting variance from closevsema30pct, "
            "(2) scoreparticipation not collapsing, (3) weights distribution.",
            composite_std, _MIN_COMPOSITE_STD,
            _sc.max(), _sc.max() - _sc.median(), _sc.median(),
        )
    elif composite_std < 0.10:
        logger.info(
            "Score spread is moderate (std=%.4f). Adequate for ranking "
            "but consider widening if entry signals are too few/many.",
            composite_std,
        )
    else:
        logger.info(
            "Score spread is healthy (std=%.4f). Good differentiation.",
            composite_std,
        )

    for col in (
        "scoretrend", "scoreparticipation", "scorerisk",
        "scoreregime", "scorerotation", "scorepenalty",
        "scorecomposite_v2",
    ):
        if col in out.columns:
            s = out[col]
            logger.debug(
                "  %s: mean=%.4f std=%.4f min=%.4f p10=%.4f med=%.4f "
                "p90=%.4f max=%.4f",
                col, s.mean(), s.std(), s.min(),
                float(s.quantile(0.10)), s.median(),
                float(s.quantile(0.90)), s.max(),
            )

    # Log the contribution of each component to composite spread
    if logger.isEnabledFor(logging.DEBUG):
        for col, weight in [
            ("scoretrend", w["trend"]),
            ("scoreparticipation", w["participation"]),
            ("scorerisk", w["risk"]),
            ("scoreregime", regime_weight),
            ("scorerotation", rotation_weight),
        ]:
            component_contribution = weight * float(out[col].std())
            logger.debug(
                "  %s: weight=%.3f × std=%.4f = contribution_to_spread=%.4f",
                col, weight, float(out[col].std()), component_contribution,
            )

    return out

###########################################
"""refactor/strategy/signals_v2.py"""
from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from refactor.common.config_refactor import CONVERGENCEPARAMS_V2, SIGNALPARAMS_V2
from .adapters_v2 import ensure_columns

logger = logging.getLogger(__name__)


_SETUP_COLS = [
    "sig_setup_continuation",
    "sig_setup_pullback",
    "sig_setup_relative",
    "sig_setup_any",
]


def _log_bool_counts(out: pd.DataFrame, cols: list[str], prefix: str) -> None:
    vals = {c: int(out[c].sum()) if c in out.columns else 0 for c in cols}
    logger.info("%s counts=%s", prefix, vals)


def _log_preview(out: pd.DataFrame, cols: list[str], label: str, n: int = 40) -> None:
    if out.empty or not logger.isEnabledFor(logging.DEBUG):
        return
    cols = [c for c in cols if c in out.columns]
    if cols:
        logger.debug("%s:\n%s", label, out[cols].head(n).to_string(index=False))


# ═══════════════════════════════════════════════════════════════
#  Vol regime → exit tightening factor
# ═══════════════════════════════════════════════════════════════
#  In elevated/volatile regimes, exits should be TIGHTER (fire sooner)
#  because losses compound faster and recovery is less certain.
#  Factor > 1.0 means thresholds are loosened (easier to trigger exit).
_VOL_EXIT_TIGHTENING: dict[str, float] = {
    "calm":     1.00,   # normal thresholds
    "moderate": 1.10,   # slightly easier exits
    "elevated": 1.25,   # meaningfully easier exits
    "volatile": 1.40,   # much easier exits
    "chaotic":  1.60,   # aggressive exits
}


def apply_signals_v2(df: pd.DataFrame, params=None) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        for c in [
            "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            *_SETUP_COLS,
            "sigeffectiveentrymin_v2", "sigconfirmed_v2",
            "sigpositionpct_v2", "sigexit_v2", "score_rank",
            "exit_momentum", "exit_trend", "exit_rs",
            "exit_no_trend", "exit_score_floor", "exit_reason",
        ]:
            out[c] = []
        return out

    p = params if params is not None else SIGNALPARAMS_V2
    logger.info("apply_signals_v2 start: rows=%d", len(out))
    logger.info(
        "Signal params: base_entry=%.4f "
        "rs_fail_penalty=%.4f breadth_fail_penalty=%.4f "
        "min_rank_pct=%.4f pos_base=%.4f pos_range=%.4f pos_max=%.4f "
        "min_hold_days=%d cooldown=%d",
        float(p.get("base_entry_threshold", 0.0)),
        float(p.get("rs_fail_penalty", 0.10)),
        float(p.get("breadth_fail_penalty", 0.05)),
        float(p.get("min_rank_pct", 0.85)),
        float(p.get("position_base_pct", 0.04)),
        float(p.get("position_range_pct", 0.08)),
        float(p.get("position_max_pct", 0.12)),
        int(p.get("min_hold_days", 5)),
        int(p.get("cooldown_days", 3)),
    )

    # ── exit condition thresholds (TUNED for early deterioration) ───
    #
    # PHILOSOPHY: Exit when a position shows MODERATE deterioration on
    # multiple dimensions, not when it has already catastrophically failed.
    # The goal is to cut losses at -3% to -5%, not hold to -10%.
    #
    # closevsema30pct is in PERCENTAGE-POINT units.
    #   e.g., -3.0 means "close is 3% below EMA30"
    #
    # These defaults are calibrated for a universe of liquid ETFs.
    # For single stocks, you may want slightly looser thresholds.

    exit_rsi_thresh = float(p.get("exit_rsi_thresh", 43.0))           # ← was 35
    exit_ema_pct_thresh = float(p.get("exit_ema_pct_thresh", -2.5))   # ← was -5.0
    exit_rs_z_thresh = float(p.get("exit_rs_z_thresh", -0.7))        # ← was -1.5
    exit_adx_thresh = float(p.get("exit_adx_thresh", 18.0))          # ← was 12
    exit_composite_floor = float(p.get("exit_composite_floor", 0.38)) # ← was 0.15
    exit_min_conditions = int(p.get("exit_min_conditions", 2))        # keep at 2
    exit_vol_adaptive = bool(p.get("exit_vol_adaptive", True))        # ← NEW

    # ── NEW: severe single-condition immediate exits ────────────────
    # These bypass the convergence requirement for catastrophic events.
    exit_rsi_severe = float(p.get("exit_rsi_severe", 28.0))
    exit_ema_severe = float(p.get("exit_ema_severe", -7.0))
    exit_rs_severe = float(p.get("exit_rs_severe", -2.0))

    logger.info(
        "Exit params (per-position, TUNED): rsi_thresh=%.1f ema_pct_thresh=%.2f "
        "rs_z_thresh=%.2f adx_thresh=%.1f composite_floor=%.4f "
        "min_conditions=%d vol_adaptive=%s",
        exit_rsi_thresh, exit_ema_pct_thresh,
        exit_rs_z_thresh, exit_adx_thresh, exit_composite_floor,
        exit_min_conditions, exit_vol_adaptive,
    )
    logger.info(
        "Exit severe (bypass convergence): rsi=%.1f ema=%.2f rs=%.2f",
        exit_rsi_severe, exit_ema_severe, exit_rs_severe,
    )

    # ── verify exit-critical columns are present ────────────────────
    _EXIT_INDICATOR_COLS = ["rsi14", "adx14", "macdhist", "closevsema30pct", "rszscore"]
    _missing_exit_cols = [c for c in _EXIT_INDICATOR_COLS if c not in out.columns]
    if _missing_exit_cols:
        logger.warning(
            "EXIT INDICATORS MISSING: %s — these will use neutral fallbacks "
            "(no exit will fire on these dimensions). Check upstream feature "
            "pipeline.",
            _missing_exit_cols,
        )
    else:
        logger.info("Exit indicator columns all present: %s", _EXIT_INDICATOR_COLS)

    # ── regime columns ──────────────────────────────────────────────
    volreg = out.get("volregime", pd.Series("calm", index=out.index))
    breadthreg = out.get("breadthregime", pd.Series("unknown", index=out.index))
    rsreg = out.get("rsregime", pd.Series("unknown", index=out.index))
    sectreg = out.get("sectrsregime", pd.Series("unknown", index=out.index))

    # ── boolean flags (diagnostic; RS & breadth are soft penalties) ─
    out["sig_vol_ok"] = ~volreg.isin(p["hard_block_vol_regimes"])
    out["sig_breadth_ok"] = ~breadthreg.isin(p["hard_block_breadth_regimes"])
    out["sig_rs_ok"] = rsreg.isin(p["allowed_rs_regimes"])
    out["sig_sector_ok"] = ~sectreg.isin(p["blocked_sector_regimes"])

    # ── effective entry threshold ────────────────────────────────────
    vol_adj = volreg.map(p["regime_entry_adjustment"]).fillna(0)
    breadth_adj = breadthreg.map(p["breadth_entry_adjustment"]).fillna(0)

    rs_penalty = np.where(
        out["sig_rs_ok"], 0.0, p.get("rs_fail_penalty", 0.10)
    )
    breadth_penalty = np.where(
        out["sig_breadth_ok"], 0.0, p.get("breadth_fail_penalty", 0.05)
    )

    out["sigeffectiveentrymin_v2"] = (
        p["base_entry_threshold"]
        + vol_adj
        + breadth_adj
        + rs_penalty
        + breadth_penalty
    )

    # ── setup shapes: vestigial (always True for compat) ────────────
    for c in _SETUP_COLS:
        out[c] = True

    composite = out["scorecomposite_v2"]
    out["score_rank"] = composite.rank(pct=True)

    # ── entry signal ────────────────────────────────────────────────
    hard_blocks_ok = out["sig_vol_ok"] & out["sig_sector_ok"]

    min_rank = p.get("min_rank_pct", 0.85)
    score_passes_threshold = composite >= out["sigeffectiveentrymin_v2"]
    rank_passes = out["score_rank"] >= min_rank

    out["sigconfirmed_v2"] = (
        hard_blocks_ok & score_passes_threshold & rank_passes
    ).astype(int)

    # ── position sizing ─────────────────────────────────────────────
    pos_base = p.get("position_base_pct", 0.04)
    pos_range = p.get("position_range_pct", 0.08)
    pos_max = p.get("position_max_pct", 0.12)

    size_mult = volreg.map(p["size_multipliers"]).fillna(1.0)
    entry_thresh = p["base_entry_threshold"]
    raw_size = pos_base + pos_range * (
        (composite - entry_thresh) / max(1 - entry_thresh, 1e-9)
    )
    out["sigpositionpct_v2"] = np.where(
        out["sigconfirmed_v2"].eq(1),
        np.clip(raw_size, 0.0, pos_max) * size_mult,
        0.0,
    )

    # ══════════════════════════════════════════════════════════════════
    #  EXIT SIGNAL (per-position deterioration)
    # ══════════════════════════════════════════════════════════════════
    #
    # DESIGN PHILOSOPHY (REVISED):
    # ─────────────────────────────
    # The old exit logic was calibrated for catastrophic events (RSI<35,
    # 5% below EMA, etc.) which meant positions were held through -10%
    # losses before any exit fired. A momentum strategy MUST cut losers
    # early — accepting many small losses to capture fewer large wins.
    #
    # NEW APPROACH:
    # 1. MODERATE thresholds: detect early deterioration, not catastrophe
    # 2. CONVERGENCE: still require 2+ conditions (prevents noise exits)
    # 3. SEVERE bypass: single catastrophic condition = immediate exit
    # 4. VOL-ADAPTIVE: tighten exits in volatile regimes
    #
    # Expected behavior:
    # - Normal bull market day: 5-12% of universe flagged (3-8 names)
    # - Mild correction day:   15-25% flagged (10-16 names)
    # - Sharp selloff day:     30-50% flagged (20-32 names)
    #
    # The engine only applies sigexit to HELD positions, so even with
    # higher flag rates, actual exits remain controlled.

    rsi = out.get("rsi14", pd.Series(50.0, index=out.index)).astype(float)
    adx = out.get("adx14", pd.Series(25.0, index=out.index)).astype(float)
    macd_hist = out.get("macdhist", pd.Series(0.0, index=out.index)).astype(float)
    close_vs_ema30 = out.get("closevsema30pct", pd.Series(0.0, index=out.index)).astype(float)
    rs_z = out.get("rszscore", pd.Series(0.0, index=out.index)).astype(float)

    # ── Vol-adaptive threshold adjustment ───────────────────────────
    # In elevated/volatile regimes, we LOOSEN exit thresholds (i.e.,
    # make them trigger more easily) by multiplying thresholds that are
    # "less than" checks.  A tightening factor of 1.25 means RSI thresh
    # goes from 43 → 43*1.25 = 53.75 (easier to trigger).
    if exit_vol_adaptive:
        vol_tightening = volreg.map(_VOL_EXIT_TIGHTENING).fillna(1.0)
    else:
        vol_tightening = pd.Series(1.0, index=out.index)

    # Apply tightening: for "less than" conditions, multiply threshold
    # by tightening factor (higher factor = higher threshold = easier exit)
    eff_rsi_thresh = exit_rsi_thresh * vol_tightening
    eff_ema_thresh = exit_ema_pct_thresh / vol_tightening  # more negative = stricter, so divide
    eff_rs_thresh = exit_rs_z_thresh / vol_tightening      # same logic
    eff_adx_thresh = exit_adx_thresh * vol_tightening
    eff_composite_floor = exit_composite_floor * vol_tightening

    # ── Individual exit conditions ──────────────────────────────────
    #
    # 1. Momentum deterioration: RSI below threshold
    #    (CHANGED: removed the AND with MACD — RSI alone is sufficient
    #     when combined with convergence requirement)
    exit_momentum = rsi < eff_rsi_thresh

    # 2. Trend break: price meaningfully below 30-day EMA
    #    (CHANGED: threshold from -5.0 to -2.5 — catches early breakdowns)
    exit_trend = close_vs_ema30 < eff_ema_thresh

    # 3. Relative strength collapse: underperforming benchmark
    #    (CHANGED: threshold from -1.5 to -0.7 — catches early RS decay)
    exit_rs = rs_z < eff_rs_thresh

    # 4. Trend evaporation: no directional movement remaining
    #    (CHANGED: threshold from 12 to 18 — catches fading trends)
    exit_no_trend = adx < eff_adx_thresh

    # 5. Composite floor: overall score has degraded significantly
    #    (CHANGED: threshold from 0.15 to 0.38 — catches score decay
    #     before it becomes catastrophic)
    exit_score_floor = composite < eff_composite_floor

    # ── NEW: MACD direction as 6th condition ────────────────────────
    # MACD histogram negative AND declining (momentum actively worsening)
    macd_prev = out.get("macdhist", pd.Series(0.0, index=out.index)).astype(float)
    exit_macd_declining = (macd_hist < 0) & (close_vs_ema30 < 0)

    # Store component booleans for diagnostics
    out["exit_momentum"] = exit_momentum
    out["exit_trend"] = exit_trend
    out["exit_rs"] = exit_rs
    out["exit_no_trend"] = exit_no_trend
    out["exit_score_floor"] = exit_score_floor

    # Count how many conditions fire per ticker
    exit_condition_count = (
        exit_momentum.astype(int)
        + exit_trend.astype(int)
        + exit_rs.astype(int)
        + exit_no_trend.astype(int)
        + exit_score_floor.astype(int)
        + exit_macd_declining.astype(int)
    )

    # ── SEVERE BYPASS: single catastrophic condition → immediate exit ──
    # These represent truly broken positions that shouldn't wait for
    # convergence.
    severe_exit = (
        (rsi < exit_rsi_severe)
        | (close_vs_ema30 < exit_ema_severe)
        | (rs_z < exit_rs_severe)
    )

    # ── CONVERGENCE: require N moderate conditions OR 1 severe ─────
    out["sigexit_v2"] = (
        (exit_condition_count >= exit_min_conditions) | severe_exit
    ).astype(int)

    # Build human-readable exit reason string for logging/debug
    reason_parts = []
    for cond, label in [
        (exit_momentum, "momentum"),
        (exit_trend, "trend"),
        (exit_rs, "rs"),
        (exit_no_trend, "no_trend"),
        (exit_score_floor, "score_floor"),
        (exit_macd_declining, "macd_declining"),
        (severe_exit, "SEVERE"),
    ]:
        reason_parts.append((cond, label))

    reason_series = pd.Series("", index=out.index)
    for cond, label in reason_parts:
        reason_series = reason_series.where(
            ~cond, reason_series + label + ";"
        )
    out["exit_reason"] = reason_series.str.rstrip(";")

    # ── diagnostic logging ──────────────────────────────────────────
    logger.info(
        "Composite stats: max=%.4f p90=%.4f p75=%.4f median=%.4f min=%.4f",
        float(composite.max()),
        float(composite.quantile(0.9)),
        float(composite.quantile(0.75)),
        float(composite.median()),
        float(composite.min()),
    )
    logger.info(
        "Effective entry threshold: min=%.4f median=%.4f max=%.4f",
        float(out["sigeffectiveentrymin_v2"].min()),
        float(out["sigeffectiveentrymin_v2"].median()),
        float(out["sigeffectiveentrymin_v2"].max()),
    )
    logger.info(
        "Score rank stats: max=%.4f p90=%.4f median=%.4f | "
        "min_rank_pct=%.4f",
        float(out["score_rank"].max()),
        float(out["score_rank"].quantile(0.9)),
        float(out["score_rank"].median()),
        min_rank,
    )
    logger.info(
        "Gate pass counts (of %d): vol_ok=%d sector_ok=%d "
        "hard_blocks_ok=%d score_ok=%d rank_ok=%d confirmed=%d",
        len(out),
        int(out["sig_vol_ok"].sum()),
        int(out["sig_sector_ok"].sum()),
        int(hard_blocks_ok.sum()),
        int(score_passes_threshold.sum()),
        int(rank_passes.sum()),
        int(out["sigconfirmed_v2"].sum()),
    )
    logger.info(
        "Soft penalty impact: rs_penalized=%d (of %d) breadth_penalized=%d (of %d)",
        int((~out["sig_rs_ok"]).sum()),
        len(out),
        int((~out["sig_breadth_ok"]).sum()),
        len(out),
    )

    # ── exit diagnostic logging ─────────────────────────────────────
    exit_count = int(out["sigexit_v2"].sum())
    exit_pct = 100 * out["sigexit_v2"].mean() if len(out) > 0 else 0.0
    severe_count = int(severe_exit.sum())

    n_momentum = int(exit_momentum.sum())
    n_trend = int(exit_trend.sum())
    n_rs = int(exit_rs.sum())
    n_no_trend = int(exit_no_trend.sum())
    n_score_floor = int(exit_score_floor.sum())
    n_macd = int(exit_macd_declining.sum())

    logger.info(
        "Exit stats (convergence=%d+ of 6, or severe): "
        "momentum=%d trend=%d rs=%d no_trend=%d score_floor=%d macd=%d "
        "severe=%d → sigexit_v2=%d (%.1f%% of %d)",
        exit_min_conditions,
        n_momentum, n_trend, n_rs, n_no_trend, n_score_floor, n_macd,
        severe_count,
        exit_count, exit_pct, len(out),
    )

    # Distribution of condition counts for understanding selectivity
    cond_count_dist = exit_condition_count.value_counts().sort_index().to_dict()
    logger.info(
        "Exit condition count distribution: %s (need >=%d to exit)",
        cond_count_dist, exit_min_conditions,
    )

    # Vol-adaptive tightening diagnostic
    if exit_vol_adaptive:
        vol_tight_dist = vol_tightening.value_counts().to_dict()
        logger.info("Vol exit tightening distribution: %s", vol_tight_dist)

    logger.info(
        "Exit input stats: rsi14 min=%.1f p25=%.1f med=%.1f | "
        "adx14 min=%.1f p25=%.1f med=%.1f | macdhist min=%.4f med=%.4f | "
        "closevsema30pct min=%.2f p25=%.2f med=%.2f | "
        "rszscore min=%.2f p25=%.2f med=%.2f",
        float(rsi.min()), float(rsi.quantile(0.25)), float(rsi.median()),
        float(adx.min()), float(adx.quantile(0.25)), float(adx.median()),
        float(macd_hist.min()), float(macd_hist.median()),
        float(close_vs_ema30.min()), float(close_vs_ema30.quantile(0.25)),
        float(close_vs_ema30.median()),
        float(rs_z.min()), float(rs_z.quantile(0.25)), float(rs_z.median()),
    )

    if exit_pct > 40:
        logger.warning(
            "EXIT CHURN RISK: %.1f%% of universe flagged for exit. "
            "Per-position thresholds may be too loose for this market "
            "regime. Breakdown: momentum=%d trend=%d rs=%d no_trend=%d "
            "score_floor=%d macd=%d severe=%d | min_conditions=%d",
            exit_pct,
            n_momentum, n_trend, n_rs, n_no_trend, n_score_floor,
            n_macd, severe_count,
            exit_min_conditions,
        )
    elif exit_pct < 3:
        logger.warning(
            "EXIT PASSIVITY RISK: only %.1f%% of universe flagged for "
            "exit. Thresholds may still be too strict. Consider loosening "
            "exit_rsi_thresh, exit_ema_pct_thresh, or exit_composite_floor.",
            exit_pct,
        )

    total = len(out)
    if total > 0:
        logger.info(
            "Filter pass rates: vol_ok=%.1f%% sector_ok=%.1f%% "
            "score_ok=%.1f%% rank_ok=%.1f%% confirmed=%.1f%% exit=%.1f%%",
            100 * out["sig_vol_ok"].mean(),
            100 * out["sig_sector_ok"].mean(),
            100 * score_passes_threshold.mean(),
            100 * rank_passes.mean(),
            100 * out["sigconfirmed_v2"].mean(),
            exit_pct,
        )

    _log_bool_counts(
        out,
        [
            "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "sigconfirmed_v2", "sigexit_v2",
        ],
        "Signal bool",
    )

    _log_preview(
        out,
        [
            "ticker", "sig_vol_ok", "sig_breadth_ok", "sig_rs_ok", "sig_sector_ok",
            "scoretrend", "scoreparticipation", "scorecomposite_v2",
            "score_rank", "sigeffectiveentrymin_v2", "sigconfirmed_v2",
            "sigpositionpct_v2", "sigexit_v2", "exit_reason",
            "volregime", "breadthregime",
            "rsregime", "sectrsregime", "rsi14", "adx14", "closevsema30pct",
        ],
        "Signal preview",
    )

    if logger.isEnabledFor(logging.DEBUG):
        # ── rejection reasons for entries ───────────────────────────
        failed = out[~out["sigconfirmed_v2"].eq(1)].copy()
        if not failed.empty:
            reasons = []
            for _, row in failed.iterrows():
                r = []
                if not row.get("sig_vol_ok", False):
                    r.append("vol_block")
                if not row.get("sig_sector_ok", False):
                    r.append("sector_block")
                if row.get("scorecomposite_v2", 0.0) < row.get(
                    "sigeffectiveentrymin_v2", 0.0
                ):
                    r.append(
                        f"below_entry({row.get('scorecomposite_v2', 0):.3f}"
                        f"<{row.get('sigeffectiveentrymin_v2', 0):.3f})"
                    )
                if row.get("score_rank", 0.0) < min_rank:
                    r.append(f"below_rank({row.get('score_rank', 0):.3f}<{min_rank})")
                if not row.get("sig_rs_ok", False):
                    r.append("rs_penalized")
                if not row.get("sig_breadth_ok", False):
                    r.append("breadth_penalized")
                reasons.append(";".join(r))
            failed = failed.assign(rejection_reasons=reasons)
            logger.debug(
                "Signal rejects preview:\n%s",
                failed[
                    [
                        c
                        for c in [
                            "ticker", "scorecomposite_v2", "score_rank",
                            "sigeffectiveentrymin_v2", "sig_vol_ok", "sig_sector_ok",
                            "sig_rs_ok", "sig_breadth_ok",
                            "rejection_reasons",
                        ]
                        if c in failed.columns
                    ]
                ]
                .head(80)
                .to_string(index=False),
            )

        # ── exit flagged tickers detail ─────────────────────────────
        exiting = out[out["sigexit_v2"].eq(1)].copy()
        if not exiting.empty:
            exit_cols = [
                c for c in [
                    "ticker", "scorecomposite_v2", "rsi14", "adx14",
                    "macdhist", "closevsema30pct", "rszscore",
                    "exit_reason",
                ]
                if c in exiting.columns
            ]
            logger.debug(
                "Exit flagged tickers:\n%s",
                exiting[exit_cols].head(80).to_string(index=False),
            )

    return out


def apply_convergence_v2(df: pd.DataFrame, params=None) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["convergence_label_v2"] = []
        out["convergence_tier_v2"] = []
        out["scoreadjusted_v2"] = []
        return out

    p = params if params is not None else CONVERGENCEPARAMS_V2
    logger.info("apply_convergence_v2 start: rows=%d", len(out))
    logger.info("Convergence params tiers=%s adjustments=%s", p.get("tiers"), p.get("adjustments"))

    rotationrec = out.get("rotationrec", pd.Series("HOLD", index=out.index))
    rotation_long = rotationrec.isin(["BUY", "STRONGBUY", "HOLD"])
    score_long = out.get("sigconfirmed_v2", pd.Series(0, index=out.index)).eq(1)

    labels = np.select(
        [
            rotation_long & score_long,
            rotation_long & ~score_long,
            ~rotation_long & score_long,
            rotationrec.eq("CONFLICT"),
        ],
        ["aligned_long", "rotation_long_only", "score_long_only", "mixed"],
        default="avoid",
    )

    out["convergence_label_v2"] = labels
    out["convergence_tier_v2"] = pd.Series(labels, index=out.index).map(p["tiers"]).fillna(0)

    adj = out.get("volregime", pd.Series("calm", index=out.index)).map(p["adjustments"]).fillna(0)
    boost = np.where(
        out["convergence_label_v2"] == "aligned_long",
        adj,
        np.where(out["convergence_label_v2"] == "mixed", -adj, 0.0),
    )
    out["scoreadjusted_v2"] = (out["scorecomposite_v2"] + boost).clip(0, 1)

    logger.info(
        "Convergence summary: aligned_long=%d rotation_long_only=%d "
        "score_long_only=%d mixed=%d avoid=%d",
        int((out["convergence_label_v2"] == "aligned_long").sum()),
        int((out["convergence_label_v2"] == "rotation_long_only").sum()),
        int((out["convergence_label_v2"] == "score_long_only").sum()),
        int((out["convergence_label_v2"] == "mixed").sum()),
        int((out["convergence_label_v2"] == "avoid").sum()),
    )
    logger.info("Convergence tiers: %s", out["convergence_tier_v2"].value_counts(dropna=False).to_dict())
    logger.info(
        "scoreadjusted_v2 stats: min=%.4f median=%.4f max=%.4f",
        float(out["scoreadjusted_v2"].min()),
        float(out["scoreadjusted_v2"].median()),
        float(out["scoreadjusted_v2"].max()),
    )

    if logger.isEnabledFor(logging.DEBUG):
        cols = [c for c in [
            "ticker", "rotationrec", "sigconfirmed_v2", "convergence_label_v2",
            "convergence_tier_v2", "scorecomposite_v2", "scoreadjusted_v2", "volregime",
        ] if c in out.columns]
        logger.debug("Convergence preview:\n%s", out[cols].head(50).to_string(index=False))
        logger.debug("Highest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=False)[cols].head(50).to_string(index=False))
        logger.debug("Lowest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=True)[cols].head(50).to_string(index=False))

    return out.sort_values(["convergence_tier_v2", "scoreadjusted_v2"], ascending=[False, False])


#####################################################