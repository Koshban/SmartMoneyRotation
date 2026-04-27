""" Strategy Files """
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


############################
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

######################################
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

##########################################
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

#################################
"""
refactor/strategy/rotation_v2.py

Rotation engine with RRG-style quadrant classification.

US market  (sector-ETF mode)
----------------------------
Computes relative strength of the 11 GICS sector ETFs versus SPY and
classifies each sector into a rotation quadrant.  Every stock in the
universe inherits its parent sector's regime via TICKER_SECTOR_MAP.

HK / IN markets  (per-stock mode)
----------------------------------
No sector ETFs are available, so the engine computes per-stock relative
strength versus the market benchmark (2800.HK for HK, NIFTYBEES.NS for
IN) and classifies each stock directly into a rotation quadrant.

Quadrants (classic clockwise RRG rotation)
------------------------------------------
    leading    - RS above its trend AND accelerating
    improving  - RS below its trend BUT accelerating
    weakening  - RS above its trend BUT decelerating
    lagging    - RS below its trend AND decelerating

Output
------
``compute_sector_rotation`` returns a dict with:

    sector_summary  - DataFrame with rotation metrics and regime.
    sector_regimes  - dict[str, str]  sector -> regime
    ticker_regimes  - dict[str, str]  ticker -> regime

All RS computation parameters are configurable via params dict.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── US sector map (imported if available) ─────────────────────────────────────
try:
    from common.sector_map import (
        SECTOR_ETFS as _US_SECTOR_ETFS,
        TICKER_SECTOR_MAP as _US_TICKER_SECTOR_MAP,
    )
except ImportError:
    _US_SECTOR_ETFS: dict[str, str] = {}
    _US_TICKER_SECTOR_MAP: dict[str, str] = {}
    logger.debug(
        "common.sector_map not available; US sector-ETF rotation disabled"
    )

MARKET_SECTOR_ETFS: dict[str, dict[str, str]] = {
    "US": dict(_US_SECTOR_ETFS) if _US_SECTOR_ETFS else {},
    "HK": {},
    "IN": {},
}

MARKET_TICKER_SECTOR_MAP: dict[str, dict[str, str]] = {
    "US": dict(_US_TICKER_SECTOR_MAP) if _US_TICKER_SECTOR_MAP else {},
    "HK": {},
    "IN": {},
}

# ── Module-level defaults (used when no params dict is supplied) ──────────────
RS_SMA_PERIOD = 50
RS_MOMENTUM_PERIOD = 20
RS_SMOOTH_SPAN = 10
MIN_HISTORY = 60


# ═══════════════════════════════════════════════════════════════════════════════
# Market auto-detection
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_market(symbol_frames: dict[str, pd.DataFrame]) -> str:
    hk = 0
    india = 0
    other = 0
    for ticker in symbol_frames:
        t = ticker.upper()
        if t.endswith(".HK"):
            hk += 1
        elif t.endswith(".NS") or t.endswith(".BO"):
            india += 1
        else:
            other += 1
    total = hk + india + other
    if total == 0:
        return "US"
    if hk > total * 0.5:
        return "HK"
    if india > total * 0.5:
        return "IN"
    return "US"


# ═══════════════════════════════════════════════════════════════════════════════
# Public API  (FIX 9: accepts params dict)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_sector_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    market: str | None = None,
    params: dict | None = None,
) -> dict:
    """
    Compute rotation quadrants.

    Parameters
    ----------
    symbol_frames : dict
        Ticker -> OHLCV DataFrame.
    bench_df : DataFrame
        Benchmark OHLCV data.
    market : str, optional
        Market code ("US", "HK", "IN").  Auto-detected if None.
    params : dict, optional
        Configuration overrides.  Recognised keys:
            rs_sma_period, rs_momentum_period, smooth_span, min_history
    """
    # ── unpack config ─────────────────────────────────────────────────────
    p = params or {}
    rs_sma_period = p.get("rs_sma_period", RS_SMA_PERIOD)
    rs_momentum_period = p.get("rs_momentum_period", RS_MOMENTUM_PERIOD)
    smooth_span = p.get("smooth_span", RS_SMOOTH_SPAN)
    min_history = p.get("min_history", MIN_HISTORY)

    logger.info(
        "compute_sector_rotation params: rs_sma=%d rs_mom=%d smooth=%d min_hist=%d",
        rs_sma_period, rs_momentum_period, smooth_span, min_history,
    )

    if bench_df is None or bench_df.empty or "close" not in bench_df.columns:
        logger.warning("compute_sector_rotation: benchmark missing or empty")
        return _empty_result()

    bench_close = pd.to_numeric(bench_df["close"], errors="coerce")
    if int(bench_close.notna().sum()) < min_history:
        logger.warning(
            "compute_sector_rotation: insufficient benchmark history (%d rows)",
            int(bench_close.notna().sum()),
        )
        return _empty_result()

    # ── resolve market ────────────────────────────────────────────────────
    if market is None:
        market = _detect_market(symbol_frames)
        logger.debug(
            "Sector rotation: auto-detected market=%s from %d tickers",
            market, len(symbol_frames),
        )

    market = market.upper()
    sector_etfs = MARKET_SECTOR_ETFS.get(market, {})

    # ── sector-ETF mode (US) ──────────────────────────────────────────────
    if sector_etfs:
        available = sum(
            1 for etf in sector_etfs.values()
            if etf in symbol_frames
            and symbol_frames[etf] is not None
            and not symbol_frames[etf].empty
        )
        if available > 0:
            logger.debug(
                "Sector rotation: sector-ETF mode  market=%s  "
                "etfs_available=%d/%d",
                market, available, len(sector_etfs),
            )
            result = _compute_sector_etf_rotation(
                symbol_frames=symbol_frames,
                bench_close=bench_close,
                sector_etfs=sector_etfs,
                ticker_sector_map=MARKET_TICKER_SECTOR_MAP.get(market, {}),
                rs_sma_period=rs_sma_period,
                rs_momentum_period=rs_momentum_period,
                smooth_span=smooth_span,
                min_history=min_history,
            )
            if result["ticker_regimes"]:
                return result
            logger.warning(
                "Sector rotation: sector-ETF mode produced empty results; "
                "falling back to per-stock mode"
            )
        else:
            logger.debug(
                "Sector rotation: market=%s but 0/%d sector ETFs "
                "present in symbol_frames; falling back to per-stock mode",
                market, len(sector_etfs),
            )

    # ── per-stock mode (HK, IN, or fallback) ─────────────────────────────
    logger.debug(
        "Sector rotation: per-stock mode  market=%s  "
        "(%d tickers vs benchmark)",
        market, len(symbol_frames),
    )
    return _compute_per_stock_rotation(
        symbol_frames=symbol_frames,
        bench_close=bench_close,
        rs_sma_period=rs_sma_period,
        rs_momentum_period=rs_momentum_period,
        smooth_span=smooth_span,
        min_history=min_history,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_rs_for_one(
    target_close: pd.Series,
    bench_close: pd.Series,
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict | None:
    combined = pd.DataFrame(
        {"target": target_close, "bench": bench_close},
    ).dropna()

    if len(combined) < min_history:
        return None

    rs_raw = combined["target"] / combined["bench"]
    rs = rs_raw.ewm(
        span=smooth_span,
        min_periods=max(3, smooth_span // 2),
    ).mean()

    rs_sma = rs.rolling(
        rs_sma_period,
        min_periods=int(rs_sma_period * 0.7),
    ).mean()
    rs_level = rs / rs_sma - 1.0

    rs_mom = rs.pct_change(rs_momentum_period)

    rs_roc_5 = rs.pct_change(5)
    excess_20 = (
        combined["target"].pct_change(20)
        - combined["bench"].pct_change(20)
    )

    last_level = rs_level.iloc[-1]
    last_mom = rs_mom.iloc[-1]

    if pd.isna(last_level) or pd.isna(last_mom):
        return None

    return {
        "rs_level": float(last_level),
        "rs_momentum": float(last_mom),
        "rs_roc_5d": (
            float(rs_roc_5.iloc[-1])
            if pd.notna(rs_roc_5.iloc[-1])
            else None
        ),
        "excess_return_20d": (
            float(excess_20.iloc[-1])
            if pd.notna(excess_20.iloc[-1])
            else None
        ),
        "rs_ratio_last": float(rs.iloc[-1]),
    }


def _classify_and_rank(summary: pd.DataFrame) -> pd.DataFrame:
    summary["regime"] = np.select(
        [
            (summary["rs_level"] > 0) & (summary["rs_momentum"] > 0),
            (summary["rs_level"] <= 0) & (summary["rs_momentum"] > 0),
            (summary["rs_level"] > 0) & (summary["rs_momentum"] <= 0),
        ],
        ["leading", "improving", "weakening"],
        default="lagging",
    )

    summary["rs_rank"] = (
        summary["rs_level"]
        .rank(ascending=False, method="min")
        .astype(int)
    )
    summary["momentum_rank"] = (
        summary["rs_momentum"]
        .rank(ascending=False, method="min")
        .astype(int)
    )

    return summary.sort_values("rs_rank").reset_index(drop=True)


def _log_regime_distribution(
    summary: pd.DataFrame,
    label_col: str,
    max_display: int = 20,
) -> None:
    regime_dist = summary["regime"].value_counts().to_dict()
    logger.debug("Rotation regime distribution: %s", regime_dist)

    for regime in ("leading", "improving", "weakening", "lagging"):
        members = summary.loc[
            summary["regime"] == regime, label_col
        ].tolist()
        display = members[:max_display]
        suffix = (
            f" ... (+{len(members) - max_display})"
            if len(members) > max_display
            else ""
        )
        logger.debug(
            "  %-12s: %s%s",
            regime.title(),
            display or ["none"],
            suffix,
        )

    if logger.isEnabledFor(logging.DEBUG):
        display_cols = [
            label_col, "regime", "rs_rank", "momentum_rank",
            "rs_level", "rs_momentum", "excess_return_20d",
        ]
        cols = [c for c in display_cols if c in summary.columns]
        logger.debug(
            "Rotation summary:\n%s",
            summary[cols].to_string(index=False),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Sector-ETF mode (US)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_sector_etf_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_close: pd.Series,
    sector_etfs: dict[str, str],
    ticker_sector_map: dict[str, str],
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict:
    rows: list[dict] = []
    skipped_missing = 0
    skipped_short = 0

    for sector_name, etf_ticker in sector_etfs.items():
        etf_df = symbol_frames.get(etf_ticker)
        if etf_df is None or etf_df.empty or "close" not in etf_df.columns:
            skipped_missing += 1
            continue

        etf_close = pd.to_numeric(etf_df["close"], errors="coerce")
        metrics = _compute_rs_for_one(
            etf_close, bench_close,
            rs_sma_period, rs_momentum_period, smooth_span, min_history,
        )

        if metrics is None:
            skipped_short += 1
            continue

        rows.append({"sector": sector_name, "etf": etf_ticker, **metrics})

    logger.debug(
        "Sector rotation: computed=%d  skipped_missing=%d  skipped_short=%d",
        len(rows), skipped_missing, skipped_short,
    )

    if not rows:
        logger.warning("compute_sector_rotation: no sectors could be computed")
        return _empty_result()

    summary = _classify_and_rank(pd.DataFrame(rows))

    sector_regimes: dict[str, str] = dict(
        zip(summary["sector"], summary["regime"]),
    )
    ticker_regimes: dict[str, str] = {
        ticker: sector_regimes.get(sector, "unknown")
        for ticker, sector in ticker_sector_map.items()
    }

    _log_regime_distribution(summary, "sector")

    return {
        "sector_summary": summary,
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Per-stock mode (HK, IN, or any market without sector ETFs)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_per_stock_rotation(
    symbol_frames: dict[str, pd.DataFrame],
    bench_close: pd.Series,
    rs_sma_period: int,
    rs_momentum_period: int,
    smooth_span: int,
    min_history: int,
) -> dict:
    rows: list[dict] = []
    skipped = 0

    for ticker, df in symbol_frames.items():
        if df is None or df.empty or "close" not in df.columns:
            skipped += 1
            continue

        stock_close = pd.to_numeric(df["close"], errors="coerce")
        metrics = _compute_rs_for_one(
            stock_close, bench_close,
            rs_sma_period, rs_momentum_period, smooth_span, min_history,
        )

        if metrics is None:
            skipped += 1
            continue

        rows.append({"ticker": ticker, "sector": ticker, **metrics})

    logger.debug(
        "Per-stock rotation: computed=%d  skipped=%d  total=%d",
        len(rows), skipped, len(symbol_frames),
    )

    if not rows:
        logger.warning(
            "compute_sector_rotation: no stocks could be computed "
            "(per-stock mode)"
        )
        return _empty_result()

    summary = _classify_and_rank(pd.DataFrame(rows))

    ticker_regimes: dict[str, str] = dict(
        zip(summary["ticker"], summary["regime"]),
    )
    sector_regimes: dict[str, str] = dict(ticker_regimes)

    _log_regime_distribution(summary, "ticker")

    return {
        "sector_summary": summary,
        "sector_regimes": sector_regimes,
        "ticker_regimes": ticker_regimes,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _empty_result() -> dict:
    return {
        "sector_summary": pd.DataFrame(),
        "sector_regimes": {},
        "ticker_regimes": {},
    }

############################################
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

##################################