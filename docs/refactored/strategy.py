##########
"""refactor/strategy/adapters_v2.py"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CRITICAL_COLUMNS = {
    "rszscore",
    "breadthscore",
    "breadthregime",
    "rsregime",
}

DEFAULTS: dict[str, Any] = {
    "rszscore": np.nan,
    "sectrszscore": np.nan,
    "rsaccel20": 0.0,
    "closevsema30pct": 0.0,
    "closevssma50pct": 0.0,
    "relativevolume": 1.0,
    "obvslope10d": 0.0,
    "adlineslope10d": 0.0,
    "dollarvolume20d": 0.0,
    "atr14pct": np.nan,
    "amihud20": np.nan,
    "gaprate20": np.nan,
    "breadthscore": np.nan,
    "breadthregime": "unknown",
    "rsi14": 50.0,
    "adx14": 20.0,
    "rsregime": "unknown",
    "sectrsregime": "unknown",
    "rotationrec": "HOLD",
    "ticker": "UNKNOWN",
    "sector": "Unknown",
    "theme": "Unknown",
    "close": np.nan,
    "volregime": "unknown",
    "volregimescore": np.nan,
}

BENCHMARK_REGIME_COLUMNS = [
    "volregime",
    "volregimescore",
    "atrp_bench",
    "realizedvol_bench",
    "gaprate_bench",
    "dispersion_bench",
]

BREADTH_COLUMNS = ["breadthscore", "breadthregime"]


def _safe_series(df: pd.DataFrame, col: str, default: Any) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(default, index=df.index)


def _safe_numeric_series(df: pd.DataFrame, col: str, default: float) -> pd.Series:
    return pd.to_numeric(_safe_series(df, col, default), errors="coerce")


def _is_placeholder_unknown(series: pd.Series) -> bool:
    if series.empty:
        return True
    vals = series.dropna().astype(str).str.lower().unique().tolist()
    if not vals:
        return True
    return set(vals).issubset({"unknown", "none", "nan", ""})


def _numeric_summary(series: pd.Series, *, fill: float | None = None) -> dict[str, Any]:
    s = pd.to_numeric(series, errors="coerce")
    if fill is not None:
        s = s.fillna(fill)
    valid = s.dropna()
    if valid.empty:
        return {
            "count": int(len(s)),
            "nonnull": 0,
            "nan": int(s.isna().sum()),
            "mean": np.nan,
            "min": np.nan,
            "max": np.nan,
            "std": np.nan,
        }
    return {
        "count": int(len(s)),
        "nonnull": int(valid.shape[0]),
        "nan": int(s.isna().sum()),
        "mean": float(valid.mean()),
        "min": float(valid.min()),
        "max": float(valid.max()),
        "std": float(valid.std(ddof=0)) if len(valid) > 1 else 0.0,
    }


def _value_counts_str(series: pd.Series, topn: int = 10) -> str:
    if series.empty:
        return "empty"
    vc = (
        series.astype("object")
        .where(~series.isna(), other="<<NA>>")
        .astype(str)
        .value_counts(dropna=False)
        .head(topn)
    )
    return ", ".join(f"{idx}={int(val)}" for idx, val in vc.items()) if not vc.empty else "empty"


def _log_missing_defaults(before_cols: set[str], after_cols: set[str]) -> None:
    missing = [c for c in DEFAULTS if c not in before_cols]
    if not missing:
        logger.info("ensure_columns: no columns injected")
        return

    critical_missing = [c for c in missing if c in CRITICAL_COLUMNS]
    soft_missing = [c for c in missing if c not in CRITICAL_COLUMNS]

    logger.info(
        "ensure_columns: injected %d missing columns (%d critical, %d soft)",
        len(missing),
        len(critical_missing),
        len(soft_missing),
    )
    if critical_missing:
        logger.warning(
            "ensure_columns: critical columns were missing and created with non-authoritative defaults/nulls: %s",
            critical_missing,
        )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("ensure_columns missing columns full list: %s", missing)
        if soft_missing:
            logger.debug("ensure_columns soft-missing columns: %s", soft_missing)


def _log_null_diagnostics(df: pd.DataFrame, label: str) -> None:
    tracked = [c for c in DEFAULTS if c in df.columns]
    if not tracked:
        logger.info("%s: no tracked columns present for null diagnostics", label)
        return

    nan_counts = {c: int(df[c].isna().sum()) for c in tracked if df[c].isna().any()}
    if nan_counts:
        logger.warning("%s: NaN counts on tracked columns: %s", label, nan_counts)
    else:
        logger.info("%s: no NaNs on tracked columns", label)

    critical_nan = {
        c: int(df[c].isna().sum())
        for c in CRITICAL_COLUMNS
        if c in df.columns and df[c].isna().any()
    }
    if critical_nan:
        logger.warning("%s: critical columns still contain NaNs: %s", label, critical_nan)


def _log_scoreability(df: pd.DataFrame, label: str) -> None:
    if df.empty:
        logger.info("%s: dataframe empty; scoreability skipped", label)
        return

    critical_presence = {}
    for col in sorted(CRITICAL_COLUMNS):
        if col not in df.columns:
            critical_presence[col] = "missing"
        elif df[col].dtype == object:
            if col.endswith("regime"):
                critical_presence[col] = f"values={_value_counts_str(df[col], topn=6)}"
            else:
                critical_presence[col] = f"nonnull={int(df[col].notna().sum())}/{len(df)}"
        else:
            s = pd.to_numeric(df[col], errors="coerce")
            critical_presence[col] = f"nonnull={int(s.notna().sum())}/{len(df)}"

    logger.info("%s: critical field health: %s", label, critical_presence)

    if "rszscore" in df.columns:
        rs = pd.to_numeric(df["rszscore"], errors="coerce")
        valid = rs.dropna()
        if valid.empty:
            logger.warning("%s: rszscore has no finite values", label)
        else:
            vmin = float(valid.min())
            vmax = float(valid.max())
            vstd = float(valid.std(ddof=0)) if len(valid) > 1 else 0.0
            logger.info(
                "%s: rszscore distribution min=%.4f max=%.4f std=%.6f nonnull=%d/%d",
                label,
                vmin,
                vmax,
                vstd,
                int(valid.shape[0]),
                len(df),
            )
            if np.isclose(vmin, vmax, atol=1e-12):
                logger.warning(
                    "%s: rszscore cross-section/time-series is degenerate (min == max). "
                    "This usually means upstream RS normalization is broken or being neutral-filled.",
                    label,
                )

    if "breadthscore" in df.columns:
        bs = _numeric_summary(df["breadthscore"])
        logger.info(
            "%s: breadthscore summary nonnull=%d/%d mean=%s min=%s max=%s std=%s",
            label,
            bs["nonnull"],
            bs["count"],
            f"{bs['mean']:.4f}" if pd.notna(bs["mean"]) else "nan",
            f"{bs['min']:.4f}" if pd.notna(bs["min"]) else "nan",
            f"{bs['max']:.4f}" if pd.notna(bs["max"]) else "nan",
            f"{bs['std']:.6f}" if pd.notna(bs["std"]) else "nan",
        )

    if "breadthregime" in df.columns:
        logger.info("%s: breadthregime distribution %s", label, _value_counts_str(df["breadthregime"], topn=8))

    if "rsregime" in df.columns:
        logger.info("%s: rsregime distribution %s", label, _value_counts_str(df["rsregime"], topn=8))

    if "sectrsregime" in df.columns:
        logger.info("%s: sectrsregime distribution %s", label, _value_counts_str(df["sectrsregime"], topn=8))


def _log_value_snapshot(out: pd.DataFrame, label: str) -> None:
    if out.empty:
        logger.info("%s: dataframe is empty", label)
        return

    rsz = _numeric_summary(_safe_numeric_series(out, "rszscore", np.nan))
    breadth = _numeric_summary(_safe_numeric_series(out, "breadthscore", np.nan))
    rsi = _numeric_summary(_safe_numeric_series(out, "rsi14", 50.0), fill=50.0)
    adx = _numeric_summary(_safe_numeric_series(out, "adx14", 20.0), fill=20.0)

    logger.info(
        (
            "%s: rows=%d cols=%d "
            "rszscore_nonnull=%d mean=%s min=%s max=%s "
            "breadth_nonnull=%d mean=%s "
            "rsi_mean=%.2f adx_mean=%.2f"
        ),
        label,
        len(out),
        len(out.columns),
        rsz["nonnull"],
        f"{rsz['mean']:.4f}" if pd.notna(rsz["mean"]) else "nan",
        f"{rsz['min']:.4f}" if pd.notna(rsz["min"]) else "nan",
        f"{rsz['max']:.4f}" if pd.notna(rsz["max"]) else "nan",
        breadth["nonnull"],
        f"{breadth['mean']:.4f}" if pd.notna(breadth["mean"]) else "nan",
        float(rsi["mean"]) if pd.notna(rsi["mean"]) else 50.0,
        float(adx["mean"]) if pd.notna(adx["mean"]) else 20.0,
    )

    _log_null_diagnostics(out, label)
    _log_scoreability(out, label)

    cols = [
        c
        for c in [
            "ticker",
            "close",
            "rszscore",
            "sectrszscore",
            "rsaccel20",
            "closevsema30pct",
            "closevssma50pct",
            "relativevolume",
            "atr14pct",
            "breadthscore",
            "breadthregime",
            "rsi14",
            "adx14",
            "rsregime",
            "sectrsregime",
            "rotationrec",
            "volregime",
            "volregimescore",
            "sector",
            "theme",
        ]
        if c in out.columns
    ]

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "%s preview:\n%s",
            label,
            out[cols].head(20).to_string(index=False) if cols else out.head(20).to_string(index=False),
        )


def _add_health_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    required_numeric = ["rszscore", "breadthscore"]
    required_categorical = ["breadthregime", "rsregime"]

    num_ok = pd.Series(True, index=out.index)
    for col in required_numeric:
        if col in out.columns:
            num_ok &= pd.to_numeric(out[col], errors="coerce").notna()
        else:
            num_ok &= False

    cat_ok = pd.Series(True, index=out.index)
    for col in required_categorical:
        if col in out.columns:
            if col.endswith("regime"):
                cat_ok &= out[col].notna() & (out[col].astype(str).str.lower() != "unknown")
            else:
                cat_ok &= out[col].notna()
        else:
            cat_ok &= False

    out["adapter_scoreable_v2"] = (num_ok & cat_ok).astype(int)

    missing_counts = pd.Series(0, index=out.index, dtype=int)
    for col in required_numeric:
        if col not in out.columns:
            missing_counts += 1
        else:
            missing_counts += pd.to_numeric(out[col], errors="coerce").isna().astype(int)

    for col in required_categorical:
        if col not in out.columns:
            missing_counts += 1
        else:
            missing_counts += (out[col].isna() | (out[col].astype(str).str.lower() == "unknown")).astype(int)

    out["adapter_missing_critical_count"] = missing_counts.astype(int)

    if "rszscore" in out.columns:
        rs = pd.to_numeric(out["rszscore"], errors="coerce")
        valid = rs.dropna()
        degenerate = 1 if (not valid.empty and np.isclose(float(valid.min()), float(valid.max()), atol=1e-12)) else 0
        out["adapter_rszscore_degenerate"] = degenerate
    else:
        out["adapter_rszscore_degenerate"] = 1

    return out


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    before_cols = set(out.columns)

    logger.info(
        "ensure_columns: start rows=%d cols=%d incoming_cols_sample=%s",
        len(out),
        len(out.columns),
        sorted(list(out.columns))[:20],
    )

    for col, val in DEFAULTS.items():
        if col not in out.columns:
            out[col] = val

    _log_missing_defaults(before_cols, set(out.columns))

    for col in ["breadthregime", "rsregime", "sectrsregime", "rotationrec", "ticker", "sector", "theme", "volregime"]:
        if col in out.columns:
            out[col] = out[col].fillna(DEFAULTS.get(col, "unknown"))

    out = _add_health_flags(out)

    n_scoreable = int(out["adapter_scoreable_v2"].sum()) if "adapter_scoreable_v2" in out.columns else 0
    logger.info(
        "ensure_columns: scoreable rows=%d/%d missing_critical_avg=%.3f rszscore_degenerate=%s",
        n_scoreable,
        len(out),
        float(pd.to_numeric(out.get("adapter_missing_critical_count", pd.Series(0, index=out.index)), errors="coerce").mean())
        if len(out) > 0
        else 0.0,
        bool(int(out["adapter_rszscore_degenerate"].iloc[0])) if "adapter_rszscore_degenerate" in out.columns and not out.empty else None,
    )

    _log_value_snapshot(out, "ensure_columns output")
    return out


def attach_benchmark_regime(stock_df: pd.DataFrame, regime_df: pd.DataFrame | None) -> pd.DataFrame:
    out = stock_df.copy()

    logger.info(
        "attach_benchmark_regime: start stock_rows=%d stock_cols=%d regime_rows=%d regime_cols=%s",
        len(out),
        len(out.columns),
        len(regime_df) if regime_df is not None else 0,
        list(regime_df.columns) if regime_df is not None else [],
    )

    if regime_df is None or regime_df.empty:
        logger.warning(
            "attach_benchmark_regime: regime_df is missing/empty; benchmark regime context not attached"
        )
        out = ensure_columns(out)
        if "volregime" not in out.columns:
            out["volregime"] = "unknown"
        if "volregimescore" not in out.columns:
            out["volregimescore"] = np.nan
        return out

    missing = [c for c in BENCHMARK_REGIME_COLUMNS if c not in regime_df.columns]
    if missing:
        logger.warning("attach_benchmark_regime: missing regime cols=%s", missing)

    use_cols = [c for c in BENCHMARK_REGIME_COLUMNS if c in regime_df.columns]
    if not use_cols:
        logger.warning(
            "attach_benchmark_regime: no usable benchmark regime columns found; returning ensure_columns(stock_df)"
        )
        out = ensure_columns(out)
        if "volregime" not in out.columns:
            out["volregime"] = "unknown"
        if "volregimescore" not in out.columns:
            out["volregimescore"] = np.nan
        return out

    aligned = regime_df[use_cols].reindex(out.index).ffill()

    aligned_nonnull = {c: int(aligned[c].notna().sum()) for c in use_cols}
    logger.info("attach_benchmark_regime: aligned non-null counts=%s", aligned_nonnull)

    for col in use_cols:
        out[col] = aligned[col]

    if "volregime" in out.columns:
        if _is_placeholder_unknown(out["volregime"]):
            logger.warning(
                "attach_benchmark_regime: resulting volregime remains entirely unknown after merge"
            )
        logger.info(
            "attach_benchmark_regime: volregime distribution %s",
            _value_counts_str(out["volregime"], topn=8),
        )

    if "volregimescore" in out.columns:
        vs = _numeric_summary(out["volregimescore"])
        logger.info(
            "attach_benchmark_regime: volregimescore nonnull=%d/%d mean=%s min=%s max=%s",
            vs["nonnull"],
            vs["count"],
            f"{vs['mean']:.4f}" if pd.notna(vs["mean"]) else "nan",
            f"{vs['min']:.4f}" if pd.notna(vs["min"]) else "nan",
            f"{vs['max']:.4f}" if pd.notna(vs["max"]) else "nan",
        )

    out = ensure_columns(out)

    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [c for c in BENCHMARK_REGIME_COLUMNS if c in out.columns]
        if preview_cols:
            logger.debug(
                "attach_benchmark_regime preview:\n%s",
                out[preview_cols].head(20).to_string(),
            )

    return out


def attach_breadth_context(stock_df: pd.DataFrame, breadth_df: pd.DataFrame | None) -> pd.DataFrame:
    out = stock_df.copy()

    logger.info(
        "attach_breadth_context: start stock_rows=%d stock_cols=%d breadth_rows=%d breadth_cols=%s",
        len(out),
        len(out.columns),
        len(breadth_df) if breadth_df is not None else 0,
        list(breadth_df.columns) if breadth_df is not None else [],
    )

    if breadth_df is None or breadth_df.empty:
        logger.warning(
            "attach_breadth_context: no breadth data available; retaining stock data with non-authoritative breadth defaults only"
        )
        return ensure_columns(out)

    use_cols = [c for c in BREADTH_COLUMNS if c in breadth_df.columns]
    logger.info("attach_breadth_context: usable breadth columns=%s", use_cols)

    if not use_cols:
        logger.warning(
            "attach_breadth_context: breadth_df has no usable columns; applying ensure_columns only"
        )
        return ensure_columns(out)

    aligned = breadth_df[use_cols].reindex(out.index).ffill()

    aligned_nonnull = {c: int(aligned[c].notna().sum()) for c in use_cols}
    logger.info("attach_breadth_context: aligned non-null counts=%s", aligned_nonnull)

    for col in use_cols:
        out[col] = aligned[col]

    out = ensure_columns(out)

    if "breadthscore" in out.columns:
        bs = _numeric_summary(out["breadthscore"])
        logger.info(
            "attach_breadth_context: resulting breadthscore nonnull=%d/%d mean=%s min=%s max=%s std=%s",
            bs["nonnull"],
            bs["count"],
            f"{bs['mean']:.4f}" if pd.notna(bs["mean"]) else "nan",
            f"{bs['min']:.4f}" if pd.notna(bs["min"]) else "nan",
            f"{bs['max']:.4f}" if pd.notna(bs["max"]) else "nan",
            f"{bs['std']:.6f}" if pd.notna(bs["std"]) else "nan",
        )

    if "breadthregime" in out.columns:
        logger.info(
            "attach_breadth_context: resulting breadthregime distribution %s",
            _value_counts_str(out["breadthregime"], topn=8),
        )
        if _is_placeholder_unknown(out["breadthregime"]):
            logger.warning(
                "attach_breadth_context: breadthregime remains entirely unknown after merge"
            )

    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [c for c in ["breadthscore", "breadthregime", "ticker", "sector", "theme"] if c in out.columns]
        logger.debug(
            "attach_breadth_context preview:\n%s",
            out[preview_cols].head(20).to_string(),
        )

    return out

###########################################################################

""" refactor/strategy/portfolio_v2.py """
from __future__ import annotations

import pandas as pd

VOL_MULT = {"calm": 1.00, "volatile": 0.75, "chaotic": 0.40}
BREADTH_EXPOSURE = {"strong": 1.00, "neutral": 0.80, "weak": 0.45, "critical": 0.20, "unknown": 0.80}


def _cap_weights(weights: pd.Series, max_w: float) -> pd.Series:
    w = weights.clip(lower=0)
    if w.sum() <= 0:
        return w
    w = w / w.sum()
    for _ in range(10):
        over = w > max_w
        if not over.any():
            break
        excess = (w[over] - max_w).sum()
        w.loc[over] = max_w
        under = ~over
        if under.any() and excess > 0 and w.loc[under].sum() > 0:
            w.loc[under] = w.loc[under] + excess * (w.loc[under] / w.loc[under].sum())
    return w / w.sum()


def build_portfolio_v2(latest: pd.DataFrame, max_positions: int = 8, max_sector_weight: float = 0.35, max_theme_names: int = 2) -> dict:
    df = latest.copy()
    if df.empty:
        return {
            "selected": pd.DataFrame(),
            "meta": {
                "target_exposure": 0.0,
                "reason": "no candidates",
                "candidate_count": 0,
                "selected_count": 0,
                "breadth_regime": "unknown",
                "vol_regime": "unknown",
            },
        }

    if "ticker" not in df.columns:
        df["ticker"] = df.index.astype(str)

    candidates = df[df.get("sigconfirmed_v2", pd.Series(0, index=df.index)).eq(1)].copy()
    if candidates.empty and "action_v2" in df.columns:
        candidates = df[df["action_v2"].isin(["STRONG_BUY", "BUY"])].copy()

    if candidates.empty:
        return {
            "selected": pd.DataFrame(),
            "meta": {
                "target_exposure": 0.0,
                "reason": "no confirmed names",
                "candidate_count": 0,
                "selected_count": 0,
                "breadth_regime": "unknown",
                "vol_regime": "unknown",
            },
        }

    breadth = str(candidates["breadthregime"].mode().iloc[0]) if "breadthregime" in candidates.columns and not candidates["breadthregime"].dropna().empty else "unknown"
    vol = str(candidates["volregime"].mode().iloc[0]) if "volregime" in candidates.columns and not candidates["volregime"].dropna().empty else "calm"
    target_exposure = BREADTH_EXPOSURE.get(breadth, 0.8) * VOL_MULT.get(vol, 1.0)

    sort_cols = [c for c in ["convergence_tier_v2", "scoreadjusted_v2", "scorecomposite_v2"] if c in candidates.columns]
    candidates = candidates.sort_values(sort_cols, ascending=[False] * len(sort_cols)) if sort_cols else candidates

    picks = []
    theme_count = {}
    for _, row in candidates.iterrows():
        if len(picks) >= max_positions:
            break
        theme = row.get("theme", "Unknown")
        if theme_count.get(theme, 0) >= max_theme_names:
            continue
        picks.append(row)
        theme_count[theme] = theme_count.get(theme, 0) + 1

    selected = pd.DataFrame(picks)
    if selected.empty:
        return {
            "selected": pd.DataFrame(),
            "meta": {
                "target_exposure": 0.0,
                "reason": "all candidates clipped by diversification",
                "candidate_count": int(len(candidates)),
                "selected_count": 0,
                "breadth_regime": breadth,
                "vol_regime": vol,
            },
        }

    score_col = "scoreadjusted_v2" if "scoreadjusted_v2" in selected.columns else "scorecomposite_v2"
    pos_col = "sigpositionpct_v2" if "sigpositionpct_v2" in selected.columns else None

    raw = pd.to_numeric(selected[score_col], errors="coerce").fillna(0).clip(lower=0)
    if pos_col is not None:
        raw = raw * pd.to_numeric(selected[pos_col], errors="coerce").fillna(0.001).clip(lower=0.001)

    weights = _cap_weights(raw, min(0.20, max_sector_weight)) * target_exposure
    selected = selected.assign(target_weight=weights.values)

    if "sector" in selected.columns:
        for sector, idx in selected.groupby("sector").groups.items():
            sector_sum = selected.loc[idx, "target_weight"].sum()
            if sector_sum > max_sector_weight and sector_sum > 0:
                selected.loc[idx, "target_weight"] *= max_sector_weight / sector_sum

    if selected["target_weight"].sum() > 0:
        selected["target_weight"] *= target_exposure / selected["target_weight"].sum()

    meta = {
        "target_exposure": float(target_exposure),
        "breadth_regime": breadth,
        "vol_regime": vol,
        "candidate_count": int(len(candidates)),
        "selected_count": int(len(selected)),
    }
    return {"selected": selected.sort_values("target_weight", ascending=False), "meta": meta}

############################################################
""" refactor/strategy/regime_v2.py """
from __future__ import annotations

import numpy as np
import pandas as pd

from refactor.common.config_refactor import VOLREGIMEPARAMS


def _clip01(x):
    return np.clip(x, 0.0, 1.0)


def classify_volatility_regime(bench: pd.DataFrame, dispersion: pd.Series | None = None) -> pd.DataFrame:
    if bench is None or bench.empty:
        raise ValueError("Benchmark dataframe cannot be empty")
    if "close" not in bench.columns:
        raise ValueError("Benchmark dataframe must contain a close column")

    p = VOLREGIMEPARAMS
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

    label = np.select([score >= 0.75, score >= 0.35], ["chaotic", "volatile"], default="calm")
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

###############################################################
"""refactor/strategy/scoring_v2.py """

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from refactor.common.config_refactor import SCORINGPARAMS_V2, SCORINGWEIGHTS_V2
from .adapters_v2 import ensure_columns

logger = logging.getLogger(__name__)


def _s(x, lo, hi):
    denom = max(hi - lo, 1e-9)
    return pd.Series(np.clip((x - lo) / denom, 0.0, 1.0), index=x.index)


def _inv(x, lo, hi):
    return 1.0 - _s(x, lo, hi)


def _log_score_distribution(out: pd.DataFrame) -> None:
    if out.empty:
        logger.info("scoring_v2 distribution skipped because dataframe is empty")
        return
    logger.info(
        "Composite distribution: min=%.4f p25=%.4f median=%.4f p75=%.4f max=%.4f mean=%.4f",
        float(out["scorecomposite_v2"].min()),
        float(out["scorecomposite_v2"].quantile(0.25)),
        float(out["scorecomposite_v2"].median()),
        float(out["scorecomposite_v2"].quantile(0.75)),
        float(out["scorecomposite_v2"].max()),
        float(out["scorecomposite_v2"].mean()),
    )
    logger.info(
        "Bucket counts: >=0.80=%d >=0.70=%d >=0.62=%d >=0.50=%d <0.50=%d",
        int((out["scorecomposite_v2"] >= 0.80).sum()),
        int((out["scorecomposite_v2"] >= 0.70).sum()),
        int((out["scorecomposite_v2"] >= 0.62).sum()),
        int((out["scorecomposite_v2"] >= 0.50).sum()),
        int((out["scorecomposite_v2"] < 0.50).sum()),
    )


def _log_component_summary(out: pd.DataFrame) -> None:
    if out.empty:
        return
    logger.info(
        "Component means: trend=%.4f participation=%.4f risk=%.4f regime=%.4f penalty=%.4f",
        float(out["scoretrend"].mean()),
        float(out["scoreparticipation"].mean()),
        float(out["scorerisk"].mean()),
        float(out["scoreregime"].mean()),
        float(out["scorepenalty"].mean()),
    )
    logger.info(
        "Penalty counts: penalty>0=%d rsi_penalty>0=%d adx_penalty>0=%d",
        int((out["scorepenalty"] > 0).sum()),
        int((out["rsi_penalty_v2"] > 0).sum()) if "rsi_penalty_v2" in out.columns else 0,
        int((out["adx_penalty_v2"] > 0).sum()) if "adx_penalty_v2" in out.columns else 0,
    )


def _log_debug_views(out: pd.DataFrame) -> None:
    if out.empty or not logger.isEnabledFor(logging.DEBUG):
        return
    top_cols = [c for c in [
        "ticker", "scoretrend", "scoreparticipation", "scorerisk", "scoreregime", "scorepenalty", "scorecomposite_v2",
        "stock_rs_v2", "sector_rs_v2", "rs_accel_v2", "trend_confirm_v2", "rvol_v2", "obv_v2", "adl_v2", "dvol_v2",
        "vol_pen_v2", "liq_pen_v2", "gap_pen_v2", "ext_pen_v2", "breadthscore", "volregimescore", "rsi14", "adx14",
        "rsi_penalty_v2", "adx_penalty_v2"
    ] if c in out.columns]
    logger.debug("Top composite rows:\n%s", out.sort_values("scorecomposite_v2", ascending=False)[top_cols].head(30).to_string(index=False))
    logger.debug("Bottom composite rows:\n%s", out.sort_values("scorecomposite_v2", ascending=True)[top_cols].head(30).to_string(index=False))
    near_cut = out[(out["scorecomposite_v2"] >= 0.45) & (out["scorecomposite_v2"] <= 0.70)]
    if not near_cut.empty:
        logger.debug("Near-threshold composite rows:\n%s", near_cut.sort_values("scorecomposite_v2", ascending=False)[top_cols].head(40).to_string(index=False))


def compute_composite_v2(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["scoretrend"] = []
        out["scoreparticipation"] = []
        out["scorerisk"] = []
        out["scoreregime"] = []
        out["scorepenalty"] = []
        out["scorecomposite_v2"] = []
        return out

    p = SCORINGPARAMS_V2
    w = SCORINGWEIGHTS_V2
    logger.info("compute_composite_v2 start: rows=%d", len(out))
    logger.info("Scoring weights=%s", dict(w))
    logger.info("Scoring params trend=%s participation=%s risk=%s regime=%s penalties=%s", p.get("trend"), p.get("participation"), p.get("risk"), p.get("regime"), p.get("penalties"))

    stock_rs = _s(pd.to_numeric(out["rszscore"], errors="coerce").fillna(0), -1.0, 2.0)
    sector_rs = _s(pd.to_numeric(out.get("sectrszscore", pd.Series(0, index=out.index)), errors="coerce").fillna(0), -1.0, 2.0)
    rs_accel = _s(pd.to_numeric(out.get("rsaccel20", pd.Series(0, index=out.index)), errors="coerce").fillna(0), -0.10, 0.15)
    trend_confirm = _s(pd.to_numeric(out.get("closevsema30pct", pd.Series(0, index=out.index)), errors="coerce").fillna(0), -0.03, 0.10)

    out["stock_rs_v2"] = stock_rs
    out["sector_rs_v2"] = sector_rs
    out["rs_accel_v2"] = rs_accel
    out["trend_confirm_v2"] = trend_confirm

    out["scoretrend"] = (
        p["trend"]["w_stock_rs"] * stock_rs
        + p["trend"]["w_sector_rs"] * sector_rs
        + p["trend"]["w_rs_accel"] * rs_accel
        + p["trend"]["w_trend_confirm"] * trend_confirm
    ).clip(0, 1)

    rvol = _s(pd.to_numeric(out.get("relativevolume", pd.Series(1, index=out.index)), errors="coerce").fillna(1), 0.8, 2.2)
    obv = _s(pd.to_numeric(out.get("obvslope10d", pd.Series(0, index=out.index)), errors="coerce").fillna(0), -0.05, 0.12)
    adl = _s(pd.to_numeric(out.get("adlineslope10d", pd.Series(0, index=out.index)), errors="coerce").fillna(0), -0.05, 0.12)
    dvol = _s(np.log1p(pd.to_numeric(out.get("dollarvolume20d", pd.Series(0, index=out.index)), errors="coerce").fillna(0)), 10, 18)

    out["rvol_v2"] = rvol
    out["obv_v2"] = obv
    out["adl_v2"] = adl
    out["dvol_v2"] = dvol

    out["scoreparticipation"] = (
        p["participation"]["w_rvol"] * rvol
        + p["participation"]["w_obv"] * obv
        + p["participation"]["w_adline"] * adl
        + p["participation"]["w_dollar_volume"] * dvol
    ).clip(0, 1)

    atrp = pd.to_numeric(out.get("atr14pct", pd.Series(0, index=out.index)), errors="coerce").fillna(0)
    illiq = pd.to_numeric(out.get("amihud20", pd.Series(0, index=out.index)), errors="coerce").fillna(0)
    gap = pd.to_numeric(out.get("gaprate20", pd.Series(0, index=out.index)), errors="coerce").fillna(0)
    extension = pd.to_numeric(out.get("closevssma50pct", pd.Series(0, index=out.index)), errors="coerce").fillna(0).abs()

    vol_pen = _inv(atrp, 0.02, p["penalties"]["atrp_high"])
    liq_pen = _inv(illiq, 0.0, p["penalties"]["illiquidity_bad"])
    gap_pen = _inv(gap, 0.05, 0.30)
    ext_pen = 1.0 - pd.Series(
        np.select(
            [
                extension >= p["penalties"]["extension_bad"],
                extension >= p["penalties"]["extension_warn"],
            ],
            [1.0, 0.5],
            default=0.0,
        ),
        index=out.index,
    )

    out["vol_pen_v2"] = vol_pen
    out["liq_pen_v2"] = liq_pen
    out["gap_pen_v2"] = gap_pen
    out["ext_pen_v2"] = ext_pen

    out["scorerisk"] = (
        p["risk"]["w_vol_penalty"] * vol_pen
        + p["risk"]["w_liquidity_penalty"] * liq_pen
        + p["risk"]["w_gap_penalty"] * gap_pen
        + p["risk"]["w_extension_penalty"] * ext_pen
    ).clip(0, 1)

    breadth = pd.to_numeric(out.get("breadthscore", pd.Series(0.5, index=out.index)), errors="coerce").fillna(0.5)
    volreg = pd.to_numeric(out.get("volregimescore", pd.Series(0.0, index=out.index)), errors="coerce").fillna(0.0)

    out["scoreregime"] = (
        p["regime"]["w_breadth"] * breadth
        + p["regime"]["w_vol_regime"] * (1.0 - volreg)
    ).clip(0, 1)

    composite = (
        w["trend"] * out["scoretrend"]
        + w["participation"] * out["scoreparticipation"]
        + w["risk"] * out["scorerisk"]
        + w["regime"] * out["scoreregime"]
    )

    out["score_raw_v2"] = composite.clip(0, 1)

    rsi = pd.to_numeric(out.get("rsi14", pd.Series(50, index=out.index)), errors="coerce").fillna(50)
    adx = pd.to_numeric(out.get("adx14", pd.Series(20, index=out.index)), errors="coerce").fillna(20)
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
        np.where(adx < p["penalties"]["adx_soft_min"], (p["penalties"]["adx_soft_min"] - adx) / 30.0, 0.0),
        index=out.index,
    ).clip(0, 0.10)

    out["rsi_penalty_v2"] = rsi_penalty
    out["adx_penalty_v2"] = adx_penalty
    out["scorepenalty"] = (rsi_penalty + adx_penalty).clip(0, 0.20)
    out["scorecomposite_v2"] = (composite - out["scorepenalty"]).clip(0, 1)

    _log_component_summary(out)
    _log_score_distribution(out)
    _log_debug_views(out)
    return out

##################################
""" refactor/strategy/signals_v2.py """
from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from refactor.common.config_refactor import CONVERGENCEPARAMS_V2, SIGNALPARAMS_V2
from .adapters_v2 import ensure_columns

logger = logging.getLogger(__name__)


def _log_bool_counts(out: pd.DataFrame, cols: list[str], prefix: str) -> None:
    vals = {c: int(out[c].sum()) if c in out.columns else 0 for c in cols}
    logger.info("%s counts=%s", prefix, vals)


def _log_preview(out: pd.DataFrame, cols: list[str], label: str, n: int = 40) -> None:
    if out.empty or not logger.isEnabledFor(logging.DEBUG):
        return
    cols = [c for c in cols if c in out.columns]
    if cols:
        logger.debug("%s:\n%s", label, out[cols].head(n).to_string(index=False))


def apply_signals_v2(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["sig_vol_ok"] = []
        out["sig_breadth_ok"] = []
        out["sig_rs_ok"] = []
        out["sig_sector_ok"] = []
        out["sigeffectiveentrymin_v2"] = []
        out["sig_setup_continuation"] = []
        out["sig_setup_pullback"] = []
        out["sig_setup_any"] = []
        out["sigconfirmed_v2"] = []
        out["sigpositionpct_v2"] = []
        out["sigexit_v2"] = []
        return out

    p = SIGNALPARAMS_V2
    logger.info("apply_signals_v2 start: rows=%d", len(out))
    logger.info(
        "Signal params: base_entry=%.4f base_exit=%.4f pullback_min_trend=%.4f continuation_min_trend=%.4f",
        float(p.get("base_entry_threshold", 0.0)),
        float(p.get("base_exit_threshold", 0.0)),
        float(p.get("pullback_min_trend", 0.0)),
        float(p.get("continuation_min_trend", 0.0)),
    )

    volreg = out.get("volregime", pd.Series("calm", index=out.index))
    breadthreg = out.get("breadthregime", pd.Series("unknown", index=out.index))
    rsreg = out.get("rsregime", pd.Series("unknown", index=out.index))
    sectreg = out.get("sectrsregime", pd.Series("unknown", index=out.index))

    out["sig_vol_ok"] = ~volreg.isin(p["hard_block_vol_regimes"])
    out["sig_breadth_ok"] = ~breadthreg.isin(p["hard_block_breadth_regimes"])
    out["sig_rs_ok"] = rsreg.isin(p["allowed_rs_regimes"])
    out["sig_sector_ok"] = ~sectreg.isin(p["blocked_sector_regimes"])

    vol_adj = volreg.map(p["regime_entry_adjustment"]).fillna(0)
    breadth_adj = breadthreg.map(p["breadth_entry_adjustment"]).fillna(0)
    out["sigeffectiveentrymin_v2"] = p["base_entry_threshold"] + vol_adj + breadth_adj

    pullback_shape = (
        (out.get("scoretrend", pd.Series(0, index=out.index)) >= p["pullback_min_trend"])
        & (out.get("closevsema30pct", pd.Series(0, index=out.index)).between(-0.05, p["pullback_max_short_extension"]))
        & (out.get("rsi14", pd.Series(50, index=out.index)) <= p["pullback_rsi_max"])
    )
    continuation_shape = (
        (out.get("scoretrend", pd.Series(0, index=out.index)) >= p["continuation_min_trend"])
        & (out.get("scoreparticipation", pd.Series(0, index=out.index)) >= 0.50)
    )

    out["sig_setup_continuation"] = continuation_shape
    out["sig_setup_pullback"] = pullback_shape & volreg.isin(["volatile", "chaotic"])
    out["sig_setup_any"] = out["sig_setup_continuation"] | out["sig_setup_pullback"]

    base_ok = out["sig_vol_ok"] & out["sig_breadth_ok"] & out["sig_rs_ok"] & out["sig_sector_ok"] & out["sig_setup_any"]
    out["sigconfirmed_v2"] = (base_ok & (out["scorecomposite_v2"] >= out["sigeffectiveentrymin_v2"])).astype(int)

    size_mult = volreg.map(p["size_multipliers"]).fillna(1.0)
    raw_size = 0.04 + 0.08 * ((out["scorecomposite_v2"] - p["base_entry_threshold"]) / max(1 - p["base_entry_threshold"], 1e-9))
    out["sigpositionpct_v2"] = np.where(out["sigconfirmed_v2"].eq(1), np.clip(raw_size, 0.0, 0.12) * size_mult, 0.0)

    out["sigexit_v2"] = (
        (out["scorecomposite_v2"] <= p["base_exit_threshold"])
        | (volreg == "chaotic")
        | (sectreg == "lagging")
    ).astype(int)

    logger.info(
        "Signal summary: vol_ok=%d breadth_ok=%d rs_ok=%d sector_ok=%d any_setup=%d confirmed=%d exits=%d",
        int(out["sig_vol_ok"].sum()),
        int(out["sig_breadth_ok"].sum()),
        int(out["sig_rs_ok"].sum()),
        int(out["sig_sector_ok"].sum()),
        int(out["sig_setup_any"].sum()),
        int(out["sigconfirmed_v2"].sum()),
        int(out["sigexit_v2"].sum()),
    )
    logger.info(
        "Signal setup counts: continuation=%d pullback=%d any=%d",
        int(out["sig_setup_continuation"].sum()),
        int(out["sig_setup_pullback"].sum()),
        int(out["sig_setup_any"].sum()),
    )
    logger.info(
        "sigeffectiveentrymin_v2 stats: min=%.4f median=%.4f max=%.4f",
        float(out["sigeffectiveentrymin_v2"].min()),
        float(out["sigeffectiveentrymin_v2"].median()),
        float(out["sigeffectiveentrymin_v2"].max()),
    )
    _log_bool_counts(
        out,
        [
            "sig_vol_ok",
            "sig_breadth_ok",
            "sig_rs_ok",
            "sig_sector_ok",
            "sig_setup_continuation",
            "sig_setup_pullback",
            "sig_setup_any",
            "sigconfirmed_v2",
            "sigexit_v2",
        ],
        "Signal bool",
    )

    _log_preview(
        out,
        [
            "ticker",
            "sig_vol_ok",
            "sig_breadth_ok",
            "sig_rs_ok",
            "sig_sector_ok",
            "sig_setup_continuation",
            "sig_setup_pullback",
            "sig_setup_any",
            "scoretrend",
            "scoreparticipation",
            "scorecomposite_v2",
            "sigeffectiveentrymin_v2",
            "sigconfirmed_v2",
            "sigpositionpct_v2",
            "sigexit_v2",
            "volregime",
            "breadthregime",
            "rsregime",
            "sectrsregime",
            "rsi14",
            "adx14",
            "closevsema30pct",
        ],
        "Signal preview",
    )

    if logger.isEnabledFor(logging.DEBUG):
        failed = out[~out["sigconfirmed_v2"].eq(1)].copy()
        if not failed.empty:
            reasons = []
            for _, row in failed.iterrows():
                r = []
                if not row.get("sig_vol_ok", False):
                    r.append("vol_block")
                if not row.get("sig_breadth_ok", False):
                    r.append("breadth_block")
                if not row.get("sig_rs_ok", False):
                    r.append("rs_block")
                if not row.get("sig_sector_ok", False):
                    r.append("sector_block")
                if not row.get("sig_setup_any", False):
                    r.append("no_setup")
                if row.get("scorecomposite_v2", 0.0) < row.get("sigeffectiveentrymin_v2", 0.0):
                    r.append("below_entry")
                reasons.append(";".join(r))
            failed = failed.assign(rejection_reasons=reasons)
            logger.debug(
                "Signal rejects preview:\n%s",
                failed[
                    [
                        c
                        for c in [
                            "ticker",
                            "scorecomposite_v2",
                            "sigeffectiveentrymin_v2",
                            "sig_vol_ok",
                            "sig_breadth_ok",
                            "sig_rs_ok",
                            "sig_sector_ok",
                            "sig_setup_any",
                            "rejection_reasons",
                        ]
                        if c in failed.columns
                    ]
                ]
                .head(80)
                .to_string(index=False)
            )
    return out


def apply_convergence_v2(df: pd.DataFrame) -> pd.DataFrame:
    out = ensure_columns(df)
    if out.empty:
        out["convergence_label_v2"] = []
        out["convergence_tier_v2"] = []
        out["scoreadjusted_v2"] = []
        return out

    p = CONVERGENCEPARAMS_V2
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
        "Convergence summary: aligned_long=%d rotation_long_only=%d score_long_only=%d mixed=%d avoid=%d",
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
        cols = [c for c in ["ticker", "rotationrec", "sigconfirmed_v2", "convergence_label_v2", "convergence_tier_v2", "scorecomposite_v2", "scoreadjusted_v2", "volregime"] if c in out.columns]
        logger.debug("Convergence preview:\n%s", out[cols].head(50).to_string(index=False))
        logger.debug("Highest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=False)[cols].head(50).to_string(index=False))
        logger.debug("Lowest adjusted scores:\n%s", out.sort_values("scoreadjusted_v2", ascending=True)[cols].head(50).to_string(index=False))

    return out.sort_values(["convergence_tier_v2", "scoreadjusted_v2"], ascending=[False, False])

#############################################
