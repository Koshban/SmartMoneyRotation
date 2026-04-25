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