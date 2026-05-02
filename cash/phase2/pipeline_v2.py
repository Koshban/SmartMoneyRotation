"""phase2/pipeline_v2.py"""
from __future__ import annotations
"""
D1. SCORING
    scoretrend, scoreparticipation, scorerisk, scoreregime
    scorerotation = binary (old: 1.0 if sector=="leading" else 0.0)
    scorecomposite_v2 = weighted sum (first pass)
              │
              ▼
D2. SECTOR ROTATION (rotation_v2.py)
    ├── RS analysis per sector ETF vs SPY
    ├── ETF universe composite scoring (SOXX, AIQ, XLK, etc.)
    ├── Blended regime per sector (RRG 65% + ETF composite 35%)
    └── Returns: sector_regimes, ticker_regimes, sector_summary, etf_ranking
              │
              ▼
D3. ENRICHMENT (enrich_v2.py)          ◄── THIS IS THE NEW STEP
    ├── Maps each ticker → its sector regime
    ├── scorerotation = graded (leading=1.0, improving=0.65, weakening=0.30, lagging=0.0)
    ├── etf_boost = ±0.10 from sector ETF composite vs universe median
    ├── scorerotation = base + etf_boost, clamped [0, 1]
    └── scorecomposite_v2 = recomputed with updated scorerotation
              │
              ▼
E. ACTION ASSIGNMENT
    Uses enriched scorecomposite_v2 → STRONG_BUY / BUY / HOLD / SELL
              │
              ▼
F. PORTFOLIO CONSTRUCTION
              │
              ▼
G. RETURN DICT
    includes: sector_summary, etf_ranking (for runner display)
"""


import logging
import math

import numpy as np
import pandas as pd

from cash.compute.indicators import compute_all_indicators

from common.sector_map import get_sector_or_class, get_theme
from cash.phase2.strategy.enrich_v2 import enrich_with_rotation
from cash.phase2.strategy.adapters_v2 import ensure_columns
from cash.phase2.strategy.breadth_v2 import compute_breadth
from cash.phase2.strategy.portfolio_v2 import (
    build_portfolio_v2,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_MAX_SECTOR_WEIGHT,
    DEFAULT_MAX_THEME_NAMES,
    DEFAULT_MAX_SINGLE_WEIGHT,
    DEFAULT_MIN_WEIGHT,
)
from cash.phase2.strategy.regime_v2 import classify_volatility_regime
from cash.phase2.strategy.rotation_v2 import compute_sector_rotation
from cash.phase2.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
from cash.phase2.strategy.scoring_v2 import compute_composite_v2
from cash.phase2.strategy.signals_v2 import apply_convergence_v2, apply_signals_v2
from common.config_refactor import (
    VOLREGIMEPARAMS,
    SCORINGWEIGHTS_V2,
    SCORINGPARAMS_V2,
    SIGNALPARAMS_V2,
    CONVERGENCEPARAMS_V2,
    ACTIONPARAMS_V2,
    BREADTHPARAMS,
    ROTATIONPARAMS,
)

logger = logging.getLogger(__name__)

CRITICAL_SCORE_COLUMNS_V2 = (
    "rszscore",
    "breadthscore",
    "breadthregime",
    "volregime",
    "atr14pct",
    "realizedvol20d",
    "gaprate20",
)

OPTIONAL_SCORE_COLUMNS_V2 = (
    "dispersion20",
    "dispersion",
)

# ── Vol regime: string label → numeric risk score (0=calm … 1=chaotic) ──
_VOL_REGIME_RISK_MAP: dict[str, float] = {
    "calm":     0.10,
    "moderate": 0.35,
    "elevated": 0.60,
    "volatile": 0.80,
    "chaotic":  1.00,
}

# ── Vol favorability label thresholds ────────────────────────────────────
_VOL_FAV_LABELS = [
    (0.70, "very favorable"),
    (0.55, "favorable"),
    (0.40, "neutral"),
    (0.25, "unfavorable"),
    (0.00, "very unfavorable"),
]


def _vol_fav_label(value: float, quartile: int) -> str:
    """
    Produce a human-readable vol favorability label combining absolute
    quality and relative quartile position within the universe.

    quartile: 1=lowest vol (best), 4=highest vol (worst)
    """
    abs_label = "very unfavorable"
    for threshold, label in _VOL_FAV_LABELS:
        if value >= threshold:
            abs_label = label
            break
    return f"{abs_label} ({value:.2f}) [Q{quartile}]"


def _is_missing_value(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _safe_float_or_none(val) -> float | None:
    """Convert to float; return None on failure or NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _classify_breadth_regime(breadth_df: pd.DataFrame | None) -> dict:
    """Extract breadth context from the last row of the breadth DataFrame."""
    if breadth_df is None or breadth_df.empty:
        return {
            "breadth_regime": "unknown",
            "breadthscore": None,
            "dispersion20": None,
            "dispersion": None,
            "above_sma50_pct": None,
            "above_sma200_pct": None,
            "advancing_pct": None,
            "net_highs_pct": None,
        }
    row = breadth_df.iloc[-1]
    regime = row.get("breadthregime", row.get("breadth_regime", "unknown"))
    score = row.get("breadthscore", row.get("breadth_score", None))
    disp20 = row.get("dispersion20", None)
    disp = row.get("dispersion_daily", row.get("dispersion", None))
    above_sma50 = row.get("above_sma50_pct", row.get("pct_above_sma50",
                  row.get("above_sma50", None)))
    above_sma200 = row.get("above_sma200_pct", row.get("pct_above_sma200",
                   row.get("above_sma200", None)))
    advancing = row.get("advancing_pct", row.get("pct_advancing",
                row.get("advancing", None)))
    net_highs = row.get("net_highs_pct", row.get("net_new_highs_pct",
                row.get("net_highs", None)))
    return {
        "breadth_regime": regime,
        "breadthscore": score,
        "dispersion20": disp20,
        "dispersion": disp,
        "above_sma50_pct": _safe_float_or_none(above_sma50),
        "above_sma200_pct": _safe_float_or_none(above_sma200),
        "advancing_pct": _safe_float_or_none(advancing),
        "net_highs_pct": _safe_float_or_none(net_highs),
    }


def _canonicalize_indicator_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename_map = {
        "rsi_14": "rsi14",
        "adx_14": "adx14",
        "atr_14_pct": "atr14pct",
        "relative_volume": "relativevolume",
        "dollar_volume": "dollarvolume20d",
        "close_vs_ema_30_pct": "closevsema30pct",
        "close_vs_sma_50_pct": "closevssma50pct",
        "ema_30": "ema30",
        "sma_30": "sma30",
        "sma_50": "sma50",
        "macd_line": "macdline",
        "macd_signal": "macdsignal",
        "macd_hist": "macdhist",
        "atr_14": "atr14",
        "volume_avg_20": "volumeavg20",
        "dollar_volume_avg_20": "dollarvolumeavg20",
        "realized_vol_20d": "realizedvol20d",
        "realized_vol_20d_chg5": "realizedvol20dchg5",
        "gap_rate_20": "gaprate20",
        "dispersion_20": "dispersion20",
        "dispersion_20d": "dispersion20",
    }
    existing = {src: dst for src, dst in rename_map.items() if src in out.columns and dst not in out.columns}
    if existing:
        out = out.rename(columns=existing)
    return out

def _compute_thematic_rotation(
    etf_ranking: pd.DataFrame,
    thematic_etf_map: dict[str, list[str]],
) -> dict:
    """
    Compute per-theme rotation quadrants by averaging ETF-level scores
    for each theme's constituent ETFs.

    Returns dict with keys: thematic_summary (DataFrame), thematic_regimes (dict).
    """
    if etf_ranking.empty or not thematic_etf_map:
        return {"thematic_summary": pd.DataFrame(), "thematic_regimes": {}}

    comp_col = "etf_composite"
    mom_col = "sub_momentum"
    ticker_col = "ticker"

    if comp_col not in etf_ranking.columns or ticker_col not in etf_ranking.columns:
        logger.warning(
            "_compute_thematic_rotation: etf_ranking missing required columns "
            "(%s, %s) — skipping",
            comp_col, ticker_col,
        )
        return {"thematic_summary": pd.DataFrame(), "thematic_regimes": {}}

    has_mom = mom_col in etf_ranking.columns

    rows = []
    for theme, tickers in thematic_etf_map.items():
        mask = etf_ranking[ticker_col].isin(tickers)
        subset = etf_ranking.loc[mask]
        if subset.empty:
            continue

        avg_composite = float(subset[comp_col].mean())
        avg_momentum = float(subset[mom_col].mean()) if has_mom else 0.0
        avg_trend = float(subset["sub_trend"].mean()) if "sub_trend" in subset.columns else 0.0
        avg_ret20 = float(subset["return20d"].mean()) if "return20d" in subset.columns else 0.0
        n_etfs = len(subset)
        etf_tickers = subset[ticker_col].tolist()

        rows.append({
            "theme": theme,
            "etf_composite": avg_composite,
            "momentum": avg_momentum,
            "trend": avg_trend,
            "return20d": avg_ret20,
            "n_etfs": n_etfs,
            "etfs": ", ".join(etf_tickers),
        })

    if not rows:
        return {"thematic_summary": pd.DataFrame(), "thematic_regimes": {}}

    summary = pd.DataFrame(rows).sort_values("etf_composite", ascending=False).reset_index(drop=True)

    # Classify quadrants: level vs median, momentum vs 0
    median_composite = summary["etf_composite"].median()

    def _classify_theme(row):
        high_level = row["etf_composite"] >= median_composite
        positive_mom = row["momentum"] >= 0
        if high_level and positive_mom:
            return "leading"
        elif not high_level and positive_mom:
            return "improving"
        elif high_level and not positive_mom:
            return "weakening"
        else:
            return "lagging"

    summary["regime"] = summary.apply(_classify_theme, axis=1)

    thematic_regimes = dict(zip(summary["theme"], summary["regime"]))

    logger.info(
        "Thematic rotation computed: themes=%d  regimes=%s",
        len(summary),
        summary["regime"].value_counts().to_dict(),
    )
    logger.info(
        "Thematic summary:\n%s",
        summary[["theme", "regime", "etf_composite", "momentum", "return20d", "n_etfs", "etfs"]].to_string(index=False),
    )

    return {
        "thematic_summary": summary,
        "thematic_regimes": thematic_regimes,
    }


def _fill_missing_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = (
        pd.to_numeric(out["close"], errors="coerce")
        if "close" in out.columns
        else None
    )
    if close is None or int(close.notna().sum()) < 20:
        return out

    if "realizedvol20d" not in out.columns or out["realizedvol20d"].isna().all():
        log_ret = np.log(close / close.shift(1))
        out["realizedvol20d"] = (
            log_ret.rolling(20, min_periods=15).std() * np.sqrt(252)
        )

    if "gaprate20" not in out.columns and "open" in out.columns:
        open_ = pd.to_numeric(out["open"], errors="coerce")
        gap_pct = (open_ / close.shift(1) - 1.0).abs()
        out["gaprate20"] = (
            (gap_pct > 0.005).astype(float).rolling(20, min_periods=15).mean()
        )

    if "atr14pct" not in out.columns and all(
        c in out.columns for c in ("high", "low")
    ):
        high = pd.to_numeric(out["high"], errors="coerce")
        low = pd.to_numeric(out["low"], errors="coerce")
        prev_close = close.shift(1)
        tr = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        atr14 = tr.ewm(span=14, min_periods=10).mean()
        out["atr14pct"] = atr14 / close
        if "atr14" not in out.columns:
            out["atr14"] = atr14

    return out


_dispersion_warned: set[tuple[str, ...]] = set()


def annotate_scoreability(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["scoreable_v2"] = pd.Series(dtype=bool)
        out["missing_critical_count_v2"] = pd.Series(dtype=int)
        out["missing_critical_fields_v2"] = pd.Series(dtype=object)
        out["scoreability_reason_v2"] = pd.Series(dtype=object)
        return out

    available_optional = [c for c in OPTIONAL_SCORE_COLUMNS_V2 if c in out.columns]
    required_cols = [c for c in CRITICAL_SCORE_COLUMNS_V2 if c in out.columns]
    unavailable_required = [c for c in CRITICAL_SCORE_COLUMNS_V2 if c not in out.columns]

    if available_optional:
        all_optional_nan = all(out[c].isna().all() for c in available_optional)
        if all_optional_nan:
            key = tuple(available_optional)
            if key not in _dispersion_warned:
                _dispersion_warned.add(key)
                logger.warning(
                    "annotate_scoreability: all optional dispersion columns "
                    "are NaN (%s). Scoring will use neutral default for "
                    "dispersion. This is expected if breadth context has not "
                    "been stamped yet.",
                    available_optional,
                )

    missing_counts = []
    missing_fields = []
    reasons = []
    scoreable_flags = []

    for _, row in out.iterrows():
        row_missing = [c for c in required_cols if _is_missing_value(row.get(c))]
        row_missing.extend(unavailable_required)
        row_missing = list(dict.fromkeys(row_missing))
        is_scoreable = len(row_missing) == 0

        missing_counts.append(len(row_missing))
        missing_fields.append(", ".join(row_missing) if row_missing else "")
        reasons.append("ok" if is_scoreable else f"missing critical inputs: {', '.join(row_missing)}")
        scoreable_flags.append(is_scoreable)

    out["scoreable_v2"] = pd.Series(scoreable_flags, index=out.index, dtype=bool)
    out["missing_critical_count_v2"] = pd.Series(missing_counts, index=out.index, dtype=int)
    out["missing_critical_fields_v2"] = pd.Series(missing_fields, index=out.index, dtype=object)
    out["scoreability_reason_v2"] = pd.Series(reasons, index=out.index, dtype=object)
    return out


def _prepare_frame(df: pd.DataFrame) -> pd.DataFrame:
    enriched = compute_all_indicators(df.copy())
    enriched = _canonicalize_indicator_columns(enriched)
    enriched = _fill_missing_indicators(enriched)
    enriched = ensure_columns(enriched)
    enriched = annotate_scoreability(enriched)
    return enriched


def _build_leadership_snapshot(leadership_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for ticker, df in leadership_frames.items():
        if df is None or df.empty:
            continue
        prepared = _prepare_frame(df)
        row = prepared.iloc[-1].to_dict()
        row["ticker"] = ticker
        if not row.get("sector") or row["sector"] == "Unknown":
            row["sector"] = get_sector_or_class(ticker)
        row["sector"] = row.get("sector") or "Unknown"
        if not row.get("theme") or row["theme"] == "Unknown":
            row["theme"] = get_theme(ticker) or "Unknown"
        rows.append(row)
    snap = pd.DataFrame(rows)
    if snap.empty:
        logger.info("Leadership snapshot is empty")
        return snap
    if "rszscore" in snap.columns:
        snap = snap.sort_values("rszscore", ascending=False)
    logger.info("Leadership snapshot built: names=%d cols=%d", len(snap), len(snap.columns))
    if logger.isEnabledFor(logging.DEBUG):
        preview_cols = [c for c in ["ticker", "rszscore", "leadership_strength", "sector", "theme", "rsi14", "adx14", "scoreable_v2", "missing_critical_fields_v2"] if c in snap.columns]
        logger.debug("Leadership snapshot preview:\n%s", snap[preview_cols].head(10).to_string(index=False) if preview_cols else snap.head(10).to_string(index=False))
    return snap


def _normalize_leadership(snapshot: pd.DataFrame) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot
    out = snapshot.copy()
    if "rszscore" in out.columns:
        rs = pd.to_numeric(out["rszscore"], errors="coerce")
        valid = rs.dropna()
        if valid.empty:
            out["leadership_strength"] = 0.0
            logger.warning(
                "Leadership normalization: rszscore has no finite values; "
                "leadership_strength set to 0.0"
            )
        elif np.isclose(float(valid.min()), float(valid.max()), atol=1e-9):
            out["leadership_strength"] = 0.5
            logger.warning(
                "Leadership normalization: rszscore is degenerate "
                "(min ≈ max = %.4f); leadership_strength set to 0.5",
                float(valid.min()),
            )
        else:
            mn = float(valid.min())
            mx = float(valid.max())
            denom = mx - mn
            out["leadership_strength"] = ((rs - mn) / denom).fillna(0.0).clip(0, 1)
            logger.info(
                "Leadership normalization applied: rszscore min=%.4f max=%.4f spread=%.4f",
                mn, mx, denom,
            )
    else:
        out["leadership_strength"] = 0.0
        logger.warning("Leadership snapshot missing rszscore; leadership_strength set to 0.0")
    return out


def _instrument_type(ticker: str) -> str:
    if "." in ticker and (ticker.endswith(".HK") or ticker.endswith(".NS") or ticker.endswith(".BO")):
        return "stock"
    etf_like = {
        "SPY","QQQ","IWM","DIA","MDY","XLK","XLF","XLE","XLV","XLI","XLC","XLY","XLP","XLU","XLRE","XLB","SOXX","SMH",
        "XBI","IBB","IGV","SKYY","HACK","CIBR","BOTZ","AIQ","QTUM","FINX","TAN","ICLN","LIT","DRIV","URA","NLR","URNM",
        "IBIT","BLOK","MTUM","ITA","ARKK","ARKG","KWEB","EEM","EFA","VWO","FXI","EWJ","EWZ","INDA","EWG","EWT","EWY",
        "TLT","IEF","HYG","LQD","TIP","AGG","GLD","SLV","USO","UNG","DBA","DBC"
    }
    return "etf" if ticker in etf_like else "stock"


def _lookup_group_strength(row: pd.Series, leadership_snapshot: pd.DataFrame) -> float:
    if leadership_snapshot.empty:
        return 0.0
    ticker = row.get("ticker")
    sector = row.get("sector", "Unknown")
    theme = row.get("theme", "Unknown")

    direct = leadership_snapshot[leadership_snapshot["ticker"].eq(ticker)]
    if not direct.empty:
        val = direct["leadership_strength"].max()
        return float(val) if pd.notna(val) else 0.0

    theme_col = leadership_snapshot["theme"] if "theme" in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    theme_match = leadership_snapshot[theme_col.eq(theme)]
    if not theme_match.empty:
        val = theme_match["leadership_strength"].max()
        return float(val) if pd.notna(val) else 0.0

    sector_col = leadership_snapshot["sector"] if "sector" in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    sector_match = leadership_snapshot[sector_col.eq(sector)]
    if not sector_match.empty:
        val = sector_match["leadership_strength"].max()
        return float(val) if pd.notna(val) else 0.0

    broad = leadership_snapshot[leadership_snapshot["ticker"].isin(["SPY", "QQQ", "IWM"])]
    if not broad.empty:
        val = broad["leadership_strength"].mean()
        return float(val) if pd.notna(val) else 0.0

    return 0.0


def _add_score_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["score_percentile_v2"] = pd.Series(dtype=float)
        return out
    base_score = out.get("scoreadjusted_v2", out.get("scorecomposite_v2", pd.Series(0.0, index=out.index)))
    out["score_percentile_v2"] = base_score.rank(pct=True, method="average").fillna(0.0)
    return out


def _generate_actions(df: pd.DataFrame, params=None) -> pd.DataFrame:
    ap = params if params is not None else ACTIONPARAMS_V2

    out = _add_score_percentiles(df.copy())
    if out.empty:
        out["action_v2"] = pd.Series(dtype=object)
        out["conviction_v2"] = pd.Series(dtype=object)
        out["action_reason_v2"] = pd.Series(dtype=object)
        out["action_sort_key_v2"] = pd.Series(dtype=float)
        return out

    sb = ap.get("strong_buy", {})
    bu = ap.get("buy", {})
    ho = ap.get("hold", {})
    se = ap.get("sell", {})
    oe = ap.get("overextended", {})
    cv = ap.get("conviction", {"high_pct": 0.85, "high_score": 0.75,
                                "medium_pct": 0.55, "medium_score": 0.60})

    score = out.get(
        "scoreadjusted_v2",
        out.get("scorecomposite_v2", pd.Series(0.0, index=out.index)),
    ).fillna(0.0)
    pct = out.get("score_percentile_v2", pd.Series(0.0, index=out.index)).fillna(0.0)
    entry = out.get(
        "sigeffectiveentrymin_v2", pd.Series(0.50, index=out.index),
    ).fillna(0.50)

    # ── STRONG_BUY qualification gates ────────────────────────────────
    sig_confirmed = out.get(
        "sigconfirmed_v2", pd.Series(False, index=out.index),
    ).fillna(False).astype(bool)

    sect_regime = (
        out.get("sectrsregime", pd.Series("unknown", index=out.index))
        .fillna("unknown").astype(str).str.lower().str.strip()
    )

    # ── RSI overextension gate (from config) ─────────────────────────
    rsi_series = pd.to_numeric(
        out.get("rsi14", pd.Series(50.0, index=out.index)),
        errors="coerce",
    ).fillna(50.0)

    sb_max_rsi = sb.get("max_rsi", 75.0)          # from strong_buy config
    bu_max_rsi = oe.get("max_rsi", 80.0)          # from overextended config
    # ─────────────────────────────────────────────────────────────────

    sb_require_confirmed = sb.get("require_confirmed", True)
    sb_allowed_regimes = set(
        sb.get("allowed_regimes", ["leading", "improving"]),
    )
    max_strong_buy = ap.get("max_strong_buy", 15)

    sell_floor = se.get("floor_score", 0.35)
    sell_pct_floor = se.get("floor_percentile", 0.10)
    sb_min_pct = sb.get("min_percentile", 0.90)
    sb_min_score = sb.get("min_score", 0.75)
    sb_above = sb.get("score_above_entry", 0.06)
    bu_min_pct = bu.get("min_percentile", 0.50)
    bu_min_score = bu.get("min_score", 0.52)
    bu_above = bu.get("score_above_entry", 0.01)
    ho_min_pct = ho.get("min_percentile", 0.25)
    ho_min_score = ho.get("min_score", 0.42)
    ho_below = ho.get("score_below_entry", 0.06)

    logger.info(
        "Action thresholds: sell_floor=%.3f sell_pct=%.3f "
        "sb_pct=%.2f sb_score=%.3f sb_confirmed=%s sb_regimes=%s sb_max_rsi=%.1f "
        "max_sb=%d bu_pct=%.2f bu_score=%.3f bu_max_rsi=%.1f "
        "ho_pct=%.2f ho_score=%.3f",
        sell_floor, sell_pct_floor,
        sb_min_pct, sb_min_score,
        sb_require_confirmed, sb_allowed_regimes, sb_max_rsi, max_strong_buy,
        bu_min_pct, bu_min_score, bu_max_rsi,
        ho_min_pct, ho_min_score,
    )

    actions = []
    reasons = []
    convictions = []
    sort_keys = []
    action_rank = {"STRONG_BUY": 4, "BUY": 3, "HOLD": 2, "SELL": 1}

    for i in out.index:
        s = float(score.loc[i])
        pv = float(pct.loc[i])
        e = float(entry.loc[i])
        confirmed_i = bool(sig_confirmed.loc[i])
        regime_i = str(sect_regime.loc[i])
        rsi_i = float(rsi_series.loc[i])

        if s < sell_floor or pv <= sell_pct_floor:
            action = "SELL"
            reason = "Below sell floor"
        elif (
            pv >= sb_min_pct
            and s >= max(sb_min_score, e + sb_above)
            and (not sb_require_confirmed or confirmed_i)
            and (not sb_allowed_regimes or regime_i in sb_allowed_regimes)
            and rsi_i <= sb_max_rsi
        ):
            action = "STRONG_BUY"
            reason = (
                f"Top-tier: score={s:.3f} pct={pv:.0%} "
                f"confirmed={confirmed_i} regime={regime_i} rsi={rsi_i:.1f}"
            )
        elif pv >= bu_min_pct and s >= max(bu_min_score, e + bu_above):
            if rsi_i > bu_max_rsi:
                action = "HOLD"
                reason = (
                    f"BUY-qualified but RSI={rsi_i:.1f} > {bu_max_rsi:.0f} "
                    f"— too extended for new entry"
                )
            else:
                action = "BUY"
                reason = "Above buy threshold"
        elif pv >= ho_min_pct and s >= max(ho_min_score, e - ho_below):
            action = "HOLD"
            reason = "In hold band"
        else:
            action = "SELL"
            reason = "Below hold band"

        conviction = (
            "high" if (pv >= cv.get("high_pct", 0.85) or s >= cv.get("high_score", 0.75))
            else "medium" if (pv >= cv.get("medium_pct", 0.55) or s >= cv.get("medium_score", 0.60))
            else "low"
        )

        actions.append(action)
        reasons.append(reason)
        convictions.append(conviction)
        sort_keys.append(action_rank[action] * 10 + pv + s / 10.0)

    # ── STRONG_BUY hard cap ───────────────────────────────────────────
    sb_count = sum(1 for a in actions if a == "STRONG_BUY")
    if sb_count > max_strong_buy:
        sb_items = [
            (idx, sort_keys[idx])
            for idx, a in enumerate(actions)
            if a == "STRONG_BUY"
        ]
        sb_items.sort(key=lambda x: x[1], reverse=True)
        demoted = 0
        for idx, _ in sb_items[max_strong_buy:]:
            actions[idx] = "BUY"
            reasons[idx] = "Demoted from STRONG_BUY: cap exceeded"
            sort_keys[idx] -= 10
            demoted += 1
        logger.info(
            "STRONG_BUY cap applied: %d → %d (demoted %d to BUY)",
            sb_count, max_strong_buy, demoted,
        )
    else:
        logger.info(
            "STRONG_BUY count=%d (within cap=%d)", sb_count, max_strong_buy,
        )

    # Log gate impact for diagnostics
    _total = len(actions)
    _sb_before_cap = sb_count
    _sb_blocked_confirmed = 0
    _sb_blocked_regime = 0
    _sb_blocked_rsi = 0
    for i_idx, i_val in enumerate(out.index):
        s = float(score.loc[i_val])
        pv = float(pct.loc[i_val])
        e = float(entry.loc[i_val])
        if pv >= sb_min_pct and s >= max(sb_min_score, e + sb_above):
            rsi_i = float(rsi_series.loc[i_val])
            if sb_require_confirmed and not bool(sig_confirmed.loc[i_val]):
                _sb_blocked_confirmed += 1
            elif sb_allowed_regimes and str(sect_regime.loc[i_val]) not in sb_allowed_regimes:
                _sb_blocked_regime += 1
            elif rsi_i > sb_max_rsi:
                _sb_blocked_rsi += 1
    logger.info(
        "STRONG_BUY gate impact: score+pct qualified=%d "
        "blocked_by_confirmation=%d blocked_by_regime=%d blocked_by_rsi=%d "
        "passed_all_gates=%d after_cap=%d",
        _sb_before_cap + _sb_blocked_confirmed + _sb_blocked_regime + _sb_blocked_rsi,
        _sb_blocked_confirmed,
        _sb_blocked_regime,
        _sb_blocked_rsi,
        _sb_before_cap,
        sum(1 for a in actions if a == "STRONG_BUY"),
    )

    # Log BUY→HOLD RSI demotions
    _bu_blocked_rsi = sum(
        1 for a, r in zip(actions, reasons) if a == "HOLD" and "too extended" in r
    )
    if _bu_blocked_rsi > 0:
        logger.info(
            "BUY→HOLD RSI demotion: %d names blocked (RSI > %.0f)",
            _bu_blocked_rsi, bu_max_rsi,
        )

    out["action_v2"] = actions
    out["conviction_v2"] = convictions
    out["action_reason_v2"] = reasons
    out["action_sort_key_v2"] = sort_keys

    counts = pd.Series(actions).value_counts().to_dict()
    logger.info("Action counts: %s", counts)

    sort_col = (
        "scoreadjusted_v2"
        if "scoreadjusted_v2" in out.columns
        else "scorecomposite_v2"
    )
    return out.sort_values(
        ["action_sort_key_v2", sort_col], ascending=[False, False]
    ).reset_index(drop=True)


def _build_review_table(action_table: pd.DataFrame, action_params: dict | None = None) -> pd.DataFrame:
    if action_table.empty:
        return pd.DataFrame()

    ap = action_params if action_params is not None else ACTIONPARAMS_V2
    oe = ap["overextended"]

    review = action_table.rename(columns={
        "action_v2": "recommendation",
        "scoreadjusted_v2": "composite_score",
        "score_percentile_v2": "score_percentile",
        "rsi14": "rsi_14",
        "adx14": "adx_14",
        "relativevolume": "relative_volume",
        "closevsema30pct": "price_vs_ema30_pct",
        "closevssma50pct": "price_vs_sma50_pct",
        "rsaccel20": "rs_accel_20",
        "gaprate20": "gap_rate_20",
        "atr14pct": "atr_14_pct",
    }).copy()

    review["overextended_flag"] = (
        (review.get("price_vs_ema30_pct", pd.Series(0.0, index=review.index)).fillna(0) >= oe["max_ema_pct"]) |
        (review.get("rsi_14", pd.Series(50.0, index=review.index)).fillna(50) >= oe["max_rsi"])
    ).map({True: "YES", False: "NO"})

    def why(row):
        parts = [
            f"score {row.get('composite_score', 0):.3f}",
            f"pct {row.get('score_percentile', 0):.0%}",
            f"RSI14 {row.get('rsi_14', 0):.1f}",
            f"ADX14 {row.get('adx_14', 0):.1f}",
            f"RVOL {row.get('relative_volume', 0):.2f}x",
            f"EMA30 {row.get('price_vs_ema30_pct', 0):.1%}",
            f"lead {row.get('leadership_strength', 0):.2f}",
            f"{row.get('sector', 'Unknown')} / {row.get('theme', 'Unknown')}",
        ]
        return ", ".join(parts)

    review["why_this_name"] = review.apply(why, axis=1)
    keep = [
        "ticker", "recommendation", "composite_score", "score_percentile", "rsi_14", "adx_14", "relative_volume",
        "price_vs_ema30_pct", "price_vs_sma50_pct", "rs_accel_20", "atr_14_pct", "gap_rate_20", "leadership_strength",
        "overextended_flag", "sector", "theme", "breadthregime", "volregime", "rsregime", "sectrsregime",
        "instrument_type", "conviction_v2", "action_reason_v2", "why_this_name"
    ]
    cols = [c for c in keep if c in review.columns]
    review = review[cols].copy()
    logger.info("Review table built: rows=%d", len(review))
    if logger.isEnabledFor(logging.DEBUG) and not review.empty:
        logger.debug("Review table preview:\n%s", review.head(20).to_string(index=False))
    return review


def _build_selling_exhaustion_table(tradable_frames: dict[str, pd.DataFrame], breadth_regime: str, vol_regime: str, leadership_snapshot: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker, df in tradable_frames.items():
        if df is None or len(df) < 6:
            continue
        tail = _prepare_frame(df).tail(6).copy()
        rsi_col = tail["rsi14"]
        vol_col = tail["relativevolume"]
        close_col = tail["close"]
        high_col = tail["high"] if "high" in tail.columns else close_col
        adx_col = tail["adx14"]
        ext_col = tail["closevsema30pct"]
        gap_col = tail["gaprate20"]
        atr_col = tail["atr14pct"]

        last_rsi = float(rsi_col.iloc[-1]) if pd.notna(rsi_col.iloc[-1]) else None
        prev_rsi = float(rsi_col.iloc[-2]) if pd.notna(rsi_col.iloc[-2]) else None
        last_vol = float(vol_col.iloc[-1]) if pd.notna(vol_col.iloc[-1]) else None
        prev_vol = float(vol_col.iloc[-2]) if pd.notna(vol_col.iloc[-2]) else None
        last_close = float(close_col.iloc[-1]) if pd.notna(close_col.iloc[-1]) else None
        prev_close = float(close_col.iloc[-2]) if pd.notna(close_col.iloc[-2]) else None
        prev_high = float(high_col.iloc[-2]) if pd.notna(high_col.iloc[-2]) else None
        last_adx = float(adx_col.iloc[-1]) if pd.notna(adx_col.iloc[-1]) else None
        prev_adx = float(adx_col.iloc[-2]) if pd.notna(adx_col.iloc[-2]) else None
        last_ext = float(ext_col.iloc[-1]) if pd.notna(ext_col.iloc[-1]) else None
        last_gap = float(gap_col.iloc[-1]) if pd.notna(gap_col.iloc[-1]) else None
        last_atr = float(atr_col.iloc[-1]) if pd.notna(atr_col.iloc[-1]) else None

        down_streak = int((close_col.diff().dropna() < 0).tail(3).sum()) if close_col.notna().all() else 0
        rsi_down_streak = int((rsi_col.diff().dropna() < 0).tail(3).sum()) if rsi_col.notna().all() else 0
        vol_down_streak = int((vol_col.diff().dropna() < 0).tail(3).sum()) if vol_col.notna().all() else 0
        price_5d_change = float(close_col.iloc[-1] / close_col.iloc[0] - 1.0) if close_col.notna().all() and close_col.iloc[0] != 0 else None

        sector = get_sector_or_class(ticker) or "Unknown"
        theme = get_theme(ticker) or "Unknown"

        leadership = _lookup_group_strength(
            pd.Series({"ticker": ticker, "sector": sector, "theme": theme}),
            leadership_snapshot,
        )

        weak_participation = (last_vol is not None and last_vol < 0.95) or vol_down_streak >= 2
        oversold = last_rsi is not None and last_rsi <= 30
        weak_trend = last_adx is not None and last_adx < 18
        stretched_down = last_ext is not None and last_ext <= -0.04
        fast_drop = price_5d_change is not None and price_5d_change <= -0.05
        rsi_turn_up = last_rsi is not None and prev_rsi is not None and last_rsi > prev_rsi
        bullish_close = last_close is not None and prev_close is not None and last_close > prev_close
        volume_reexpansion = last_vol is not None and prev_vol is not None and last_vol > prev_vol
        close_above_prior_high = last_close is not None and prev_high is not None and last_close > prev_high
        adx_stabilizing = last_adx is not None and prev_adx is not None and last_adx >= prev_adx

        trigger_score = sum([rsi_turn_up, bullish_close, volume_reexpansion, close_above_prior_high, adx_stabilizing])
        exhaustion_score = sum([2 if oversold else 0, 1 if rsi_down_streak >= 2 else 0, 1 if weak_participation else 0, 1 if down_streak >= 2 else 0, 1 if weak_trend else 0, 1 if stretched_down else 0, 1 if fast_drop else 0])

        if exhaustion_score < 4:
            continue

        if trigger_score >= 3:
            setup = "TRIGGERED_REVERSAL"
        elif trigger_score >= 1:
            setup = "EARLY_REVERSAL_SIGNAL"
        else:
            setup = "WATCH_REVERSAL" if oversold and weak_participation else "WEAK_SELLING"

        if trigger_score >= 4 and exhaustion_score >= 6:
            quality = "HIGH_RISK_HIGH_REWARD"
        elif trigger_score >= 4 and exhaustion_score >= 4:
            quality = "HIGH_QUALITY_BOUNCE"
        elif trigger_score >= 2:
            quality = "EARLY"
        else:
            quality = "TOO_EARLY"

        rows.append({
            "ticker": ticker, "instrument_type": _instrument_type(ticker),
            "status": setup, "quality_label": quality,
            "selling_exhaustion_score": exhaustion_score,
            "reversal_trigger_score": trigger_score,
            "rsi_14": last_rsi, "rsi_down_streak_3d": rsi_down_streak,
            "rsi_turn_up_1d": "YES" if rsi_turn_up else "NO",
            "close_down_streak_3d": down_streak,
            "bullish_close_1d": "YES" if bullish_close else "NO",
            "close_above_prior_high": "YES" if close_above_prior_high else "NO",
            "relative_volume": last_vol, "volume_down_streak_3d": vol_down_streak,
            "volume_reexpansion_1d": "YES" if volume_reexpansion else "NO",
            "adx_14": last_adx,
            "adx_stabilizing_1d": "YES" if adx_stabilizing else "NO",
            "price_5d_change": price_5d_change,
            "price_vs_ema30_pct": last_ext, "atr_14_pct": last_atr,
            "gap_rate_20": last_gap, "leadership_strength": leadership,
            "breadthregime": breadth_regime, "volregime": vol_regime,
            "sector": sector,
            "theme": theme,
            "decision_hint": "Use only with confirmation; stronger when RSI turns up, price firms, and volume re-expands",
        })

    out = pd.DataFrame(rows)
    if out.empty:
        logger.info("Selling exhaustion table is empty")
        return out
    logger.info("Selling exhaustion table built: rows=%d", len(out))
    return out.sort_values(["reversal_trigger_score", "selling_exhaustion_score", "rsi_14", "price_5d_change"], ascending=[False, False, True, True]).reset_index(drop=True)


def _build_skipped_table(skipped_rows: list[dict]) -> pd.DataFrame:
    if not skipped_rows:
        return pd.DataFrame(columns=[
            "ticker", "instrument_type", "status_v2", "scoreable_v2",
            "missing_critical_count_v2", "missing_critical_fields_v2", "scoreability_reason_v2",
            "breadthregime", "volregime", "sector", "theme"
        ])
    out = pd.DataFrame(skipped_rows).copy()
    sort_cols = [c for c in ["missing_critical_count_v2", "ticker"] if c in out.columns]
    return out.sort_values(sort_cols, ascending=[False, True]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Main pipeline entry point
# ═══════════════════════════════════════════════════════════════════════════════

def run_pipeline_v2(
    tradable_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    breadth_df: pd.DataFrame | None = None,
    market: str = "US",
    leadership_frames: dict[str, pd.DataFrame] | None = None,
    portfolio_params: dict | None = None,
    config: dict | None = None,
    precomputed: bool = False,
) -> dict:
    if bench_df is None or bench_df.empty:
        raise ValueError("bench_df is required and cannot be empty")

    config = config or {}

    vol_regime_params  = config.get("vol_regime_params")   or config.get("VOLREGIMEPARAMS", None)
    scoring_weights    = config.get("scoring_weights")     or config.get("SCORINGWEIGHTS_V2", None)
    scoring_params     = config.get("scoring_params")      or config.get("SCORINGPARAMS_V2", None)
    signal_params      = config.get("signal_params")       or config.get("SIGNALPARAMS_V2", None)
    convergence_params = config.get("convergence_params")  or config.get("CONVERGENCEPARAMS_V2", None)
    action_params      = config.get("action_params")       or config.get("ACTIONPARAMS_V2", None)
    breadth_params     = config.get("breadth_params")      or config.get("BREADTHPARAMS", None)
    rotation_params    = config.get("rotation_params")     or config.get("ROTATIONPARAMS", None)

    logger.info(
        "run_pipeline_v2 start: market=%s tradable=%d leadership=%d precomputed=%s",
        market, len(tradable_frames), len(leadership_frames or {}), precomputed,
    )

    # ── A. volatility regime from benchmark ───────────────────────────────────
    regime_df = classify_volatility_regime(bench_df, params=vol_regime_params)

    # ── B. merge all symbol frames ────────────────────────────────────────────
    all_symbol_frames: dict[str, pd.DataFrame] = {}
    if leadership_frames:
        all_symbol_frames.update(leadership_frames)
    all_symbol_frames.update(tradable_frames)

    logger.info(
        "Universe merge: combined=%d (tradable=%d leadership=%d overlap=%d)",
        len(all_symbol_frames),
        len(tradable_frames),
        len(leadership_frames or {}),
        len(set(tradable_frames) & set(leadership_frames or {})),
    )

    # ── C. BREADTH ────────────────────────────────────────────────────────────
    breadth_computed_df = compute_breadth(all_symbol_frames, params=breadth_params)

    if (
        breadth_df is not None
        and not breadth_df.empty
        and "breadthscore" in breadth_df.columns
    ):
        breadth_info = _classify_breadth_regime(breadth_df)
        breadth_source = "caller"
    elif not breadth_computed_df.empty:
        breadth_info = _classify_breadth_regime(breadth_computed_df)
        breadth_source = "computed"
    else:
        breadth_info = {
            "breadth_regime": "unknown",
            "breadthscore": None,
            "dispersion20": None,
            "dispersion": None,
            "above_sma50_pct": None,
            "above_sma200_pct": None,
            "advancing_pct": None,
            "net_highs_pct": None,
        }
        breadth_source = "none"

    logger.info(
        "Breadth context (source=%s): regime=%s score=%s dispersion20=%s "
        "above_sma50=%s above_sma200=%s advancing=%s net_highs=%s",
        breadth_source,
        breadth_info.get("breadth_regime", "unknown"),
        breadth_info.get("breadthscore"),
        breadth_info.get("dispersion20"),
        breadth_info.get("above_sma50_pct"),
        breadth_info.get("above_sma200_pct"),
        breadth_info.get("advancing_pct"),
        breadth_info.get("net_highs_pct"),
    )

    # ── D. CROSS-SECTIONAL RS ─────────────────────────────────────────────────
    if not precomputed:
        all_symbol_frames = compute_rs_zscores(all_symbol_frames, bench_df)
        all_symbol_frames = enrich_rs_regimes(all_symbol_frames)
    else:
        logger.info("Skipping RS z-score computation (precomputed=True)")

    tradable_enriched = {
        k: all_symbol_frames[k]
        for k in tradable_frames
        if k in all_symbol_frames
    }
    leadership_enriched = {
        k: all_symbol_frames[k]
        for k in (leadership_frames or {})
        if k in all_symbol_frames
    }

    # ── D2. SECTOR ROTATION ──────────────────────────────────────────────────
    rotation_result = compute_sector_rotation(
        all_symbol_frames,
        bench_df,
        market=market,
        params=rotation_params,
    )
    sector_regimes = rotation_result["sector_regimes"]
    ticker_regimes = rotation_result["ticker_regimes"]
    sector_summary = rotation_result["sector_summary"]
    etf_ranking    = rotation_result.get("etf_ranking", pd.DataFrame())

    # ── FIX: Clean up etf_ranking NaN composites for display ─────────────────
    _etf_has_composite = (
        not etf_ranking.empty
        and "etf_composite" in etf_ranking.columns
        and etf_ranking["etf_composite"].notna().any()
    )
    if not etf_ranking.empty and "etf_composite" in etf_ranking.columns:
        if etf_ranking["etf_composite"].isna().all():
            logger.warning(
                "etf_ranking: all etf_composite values are NaN — "
                "no sector ETF data available for market=%s. "
                "Filling with 0.0 for display; ETF boost disabled.",
                market,
            )
            etf_ranking = etf_ranking.copy()
            etf_ranking["etf_composite"] = 0.0
    # ── END etf_ranking fix ──────────────────────────────────────────────────

    # ── D2b. THEMATIC ROTATION ────────────────────────────────────────────────
    _thematic_etf_map = config.get("thematic_etf_map", {})
    thematic_rotation_result = _compute_thematic_rotation(etf_ranking, _thematic_etf_map)
    thematic_summary = thematic_rotation_result["thematic_summary"]
    thematic_regimes = thematic_rotation_result["thematic_regimes"]

    # ── E. leadership snapshot ────────────────────────────────────────────────
    if precomputed:
        leadership_snapshot = pd.DataFrame()
        logger.info("Skipping leadership snapshot build (precomputed=True)")
    else:
        leadership_snapshot = _normalize_leadership(
            _build_leadership_snapshot(leadership_enriched)
        )

    last_vol = regime_df.iloc[-1]

    leading_sectors = (
        sector_summary.loc[sector_summary["regime"] == "leading", "sector"].tolist()
        if not sector_summary.empty else []
    )
    lagging_sectors = (
        sector_summary.loc[sector_summary["regime"] == "lagging", "sector"].tolist()
        if not sector_summary.empty else []
    )

    # ══════════════════════════════════════════════════════════════════════════
    # ── FIXED: derive vol regime score from label if raw score is 0.0 ──────
    # ══════════════════════════════════════════════════════════════════════════
    _vol_regime_label = str(last_vol.get("volregime", "unknown")).lower().strip()

    logger.info(
        "Regime context: breadth=%s breadthscore=%s vol=%s volscore=%s "
        "leading_sectors=%s lagging_sectors=%s",
        breadth_info.get("breadth_regime", "unknown"),
        breadth_info.get("breadthscore"),
        _vol_regime_label,
        last_vol.get("volregimescore", None),
        leading_sectors or ["none"],
        lagging_sectors or ["none"],
    )

    _cp = convergence_params if convergence_params is not None else CONVERGENCEPARAMS_V2
    _sect_to_rec = _cp.get("rotation_rec_map", {
        "leading":   "STRONGBUY",
        "improving": "BUY",
        "weakening": "SELL",
        "lagging":   "SELL",
    })
    _sect_to_rec_default = _cp.get("rotation_rec_default", "HOLD")

    logger.info(
        "Rotation rec map: %s  default=%s",
        _sect_to_rec, _sect_to_rec_default,
    )

    # ── F. per-symbol preparation loop ────────────────────────────────────────
    latest_rows = []
    skipped_rows = []
    prep_logged = 0

    for ticker, df in tradable_enriched.items():
        if df is None or df.empty:
            logger.debug("Skipping empty tradable frame for %s", ticker)
            continue

        if precomputed:
            row = df.iloc[-1].to_dict()
        else:
            prepared = _prepare_frame(df)
            row = prepared.iloc[-1].to_dict()

        row["ticker"] = ticker
        row["instrument_type"] = _instrument_type(ticker)

        _row_vol = row.get("volregime")
        if _row_vol is None or str(_row_vol).lower() in ("unknown", "nan", ""):
            row["volregime"] = last_vol.get("volregime", "calm")
        _row_volscore = row.get("volregimescore")
        if _row_volscore is None or (isinstance(_row_volscore, float) and math.isnan(_row_volscore)):
            row["volregimescore"] = last_vol.get("volregimescore", 0.0)

        _row_br = row.get("breadthregime")
        if _row_br is None or str(_row_br).lower() in ("unknown", "nan", ""):
            row["breadthregime"] = breadth_info.get("breadth_regime", "unknown")
        _row_bs = row.get("breadthscore")
        if _row_bs is None or (isinstance(_row_bs, float) and math.isnan(_row_bs)):
            row["breadthscore"] = breadth_info.get("breadthscore", 0.5)

        _row_d20 = row.get("dispersion20")
        if _row_d20 is None or (isinstance(_row_d20, float) and math.isnan(_row_d20)):
            row["dispersion20"] = breadth_info.get("dispersion20")
        _row_d = row.get("dispersion")
        if _row_d is None or (isinstance(_row_d, float) and math.isnan(_row_d)):
            row["dispersion"] = breadth_info.get("dispersion")

        if not row.get("sector") or row["sector"] == "Unknown":
            row["sector"] = get_sector_or_class(ticker)
        row["sector"] = row.get("sector") or "Unknown"

        if not row.get("theme") or row["theme"] == "Unknown":
            row["theme"] = get_theme(ticker) or "Unknown"

        _row_sr = row.get("sectrsregime")
        if _row_sr is None or str(_row_sr).lower() in ("unknown", "nan", ""):
            row["sectrsregime"] = ticker_regimes.get(ticker, "unknown")

        _row_rr = row.get("rotationrec")
        if _row_rr is None or str(_row_rr).lower() in ("unknown", "nan", ""):
            row["rotationrec"] = _sect_to_rec.get(
                str(row.get("sectrsregime", "unknown")).lower(),
                _sect_to_rec_default,
            )

        is_scoreable = bool(row.get("scoreable_v2", True))
        if not is_scoreable:
            skipped_rows.append({
                "ticker": ticker,
                "instrument_type": row.get("instrument_type", _instrument_type(ticker)),
                "status_v2": "SKIPPED",
                "scoreable_v2": False,
                "missing_critical_count_v2": int(row.get("missing_critical_count_v2", 0) or 0),
                "missing_critical_fields_v2": row.get("missing_critical_fields_v2", ""),
                "scoreability_reason_v2": row.get("scoreability_reason_v2", "missing critical inputs"),
                "breadthregime": row.get("breadthregime", "unknown"),
                "volregime": row.get("volregime", "unknown"),
                "sectrsregime": row.get("sectrsregime", "unknown"),
                "rotationrec": row.get("rotationrec", _sect_to_rec_default),
                "sector": row.get("sector", "Unknown"),
                "theme": row.get("theme", "Unknown"),
            })
            logger.info(
                "Skipping %s from scoring: %s",
                ticker,
                row.get("scoreability_reason_v2", "missing critical inputs"),
            )
            continue

        latest_rows.append(row)

        if logger.isEnabledFor(logging.DEBUG) and prep_logged < 25:
            logger.debug(
                "Prepared %s last-row snapshot: close=%.4f rsi14=%.2f adx14=%.2f "
                "atr14pct=%.4f rvol=%.2f rszscore=%s rsregime=%s sectrsregime=%s "
                "rotationrec=%s breadth=%s vol=%s dispersion20=%s sector=%s theme=%s",
                ticker,
                float(row.get("close", 0.0) or 0.0),
                float(row.get("rsi14", 50.0) or 50.0),
                float(row.get("adx14", 20.0) or 20.0),
                float(row.get("atr14pct", 0.03) or 0.03),
                float(row.get("relativevolume", 1.0) or 1.0),
                row.get("rszscore", "missing"),
                row.get("rsregime", "unknown"),
                row.get("sectrsregime", "unknown"),
                row.get("rotationrec", _sect_to_rec_default),
                row.get("breadthregime", "unknown"),
                row.get("volregime", "unknown"),
                row.get("dispersion20", "missing"),
                row.get("sector", "Unknown"),
                row.get("theme", "Unknown"),
            )
            prep_logged += 1

    latest = pd.DataFrame(latest_rows) if latest_rows else pd.DataFrame()
    skipped_table = _build_skipped_table(skipped_rows)
    logger.info("Latest tradable snapshot rows=%d skipped=%d", len(latest), len(skipped_table))

    if not latest.empty:
        latest["leadership_strength"] = latest.apply(
            lambda row: _lookup_group_strength(row, leadership_snapshot), axis=1
        )
        latest = ensure_columns(latest)

        # ── FIXED: re-stamp breadth context after ensure_columns ──────────
        # ensure_columns may add dispersion20/dispersion as NaN defaults,
        # overwriting the values we stamped in the per-symbol loop.
        _bi_d20 = breadth_info.get("dispersion20")
        if _bi_d20 is not None and not (isinstance(_bi_d20, float) and math.isnan(_bi_d20)):
            if "dispersion20" in latest.columns:
                latest["dispersion20"] = latest["dispersion20"].fillna(_bi_d20)
            else:
                latest["dispersion20"] = _bi_d20
        _bi_d = breadth_info.get("dispersion")
        if _bi_d is not None and not (isinstance(_bi_d, float) and math.isnan(_bi_d)):
            if "dispersion" in latest.columns:
                latest["dispersion"] = latest["dispersion"].fillna(_bi_d)
            else:
                latest["dispersion"] = _bi_d
        # ── END re-stamp ──────────────────────────────────────────────────

        latest = annotate_scoreability(latest)
        logger.info(
            "Latest snapshot diagnostics: avg_rsi14=%.2f avg_adx14=%.2f avg_rvol=%.2f "
            "avg_lead=%.2f avg_rszscore=%s breadth=%s",
            float(latest["rsi14"].fillna(50.0).mean()),
            float(latest["adx14"].fillna(20.0).mean()),
            float(latest["relativevolume"].fillna(1.0).mean()),
            float(latest["leadership_strength"].fillna(0.0).mean()),
            f"{float(latest['rszscore'].dropna().mean()):.4f}" if latest["rszscore"].notna().any() else "nan",
            breadth_info.get("breadth_regime", "unknown"),
        )

        if "rotationrec" in latest.columns:
            rr_dist = latest["rotationrec"].value_counts().to_dict()
            logger.info("Scoreable set rotationrec distribution: %s", rr_dist)

        if "sectrsregime" in latest.columns:
            sr_dist = latest["sectrsregime"].value_counts().to_dict()
            logger.info("Scoreable set sectrsregime distribution: %s", sr_dist)

        if "theme" in latest.columns:
            theme_dist = latest["theme"].value_counts().to_dict()
            logger.info("Scoreable set theme distribution: %s", theme_dist)

        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in [
                "ticker", "close", "rsi14", "adx14", "atr14pct", "relativevolume",
                "rszscore", "rsregime", "sectrsregime", "rotationrec",
                "leadership_strength", "breadthregime", "breadthscore",
                "volregime", "realizedvol20d", "gaprate20", "dispersion20",
                "scoreable_v2", "missing_critical_fields_v2", "sector", "theme",
            ] if c in latest.columns]
            logger.debug("Latest snapshot preview:\n%s", latest[cols].head(30).to_string(index=False))

    # ══════════════════════════════════════════════════════════════════════════
    # ── Extract market-level scalars for scoreregime ──────────────────────────
    # ══════════════════════════════════════════════════════════════════════════
    _market_breadth_score: float | None = None
    _raw_breadth = breadth_info.get("breadthscore")
    if _raw_breadth is not None:
        try:
            _market_breadth_score = float(_raw_breadth)
            if math.isnan(_market_breadth_score):
                _market_breadth_score = None
        except (TypeError, ValueError):
            _market_breadth_score = None

    _market_vol_score: float | None = None
    _raw_vol = last_vol.get("volregimescore")
    if _raw_vol is not None:
        try:
            _market_vol_score = float(_raw_vol)
            if math.isnan(_market_vol_score):
                _market_vol_score = None
        except (TypeError, ValueError):
            _market_vol_score = None

    # ── FIXED: derive vol score from label when raw score is 0.0 or None ──
    if _market_vol_score is None or _market_vol_score == 0.0:
        _derived = _VOL_REGIME_RISK_MAP.get(_vol_regime_label)
        if _derived is not None:
            logger.info(
                "Vol regime score fallback: raw=%s label=%s → derived=%.4f",
                _market_vol_score, _vol_regime_label, _derived,
            )
            _market_vol_score = _derived
        elif _market_vol_score is None:
            _market_vol_score = 0.35  # neutral default
            logger.info(
                "Vol regime score fallback: raw=None label=%s → neutral=0.35",
                _vol_regime_label,
            )
    # ── END vol score fix ─────────────────────────────────────────────────

    # ── Build vol_info dict for report rendering ──────────────────────────
    _rv20d = last_vol.get("realizedvol20d", last_vol.get("realized_vol_20d"))
    _rv20d_f = _safe_float_or_none(_rv20d)
    vol_info = {
        "vol_regime": _vol_regime_label,
        "vol_regime_score": _market_vol_score,
        "vol_favorability": max(0.10, 0.70 - 0.60 * (_market_vol_score or 0.35)),
        "realized_vol_20d": _rv20d_f,
    }

    logger.info(
        "Market-level scores for composite: breadth_score=%s vol_regime_score=%s "
        "vol_favorability=%.4f",
        f"{_market_breadth_score:.4f}" if _market_breadth_score is not None else "None",
        f"{_market_vol_score:.4f}" if _market_vol_score is not None else "None",
        vol_info["vol_favorability"],
    )
    # ══════════════════════════════════════════════════════════════════════════

    # ── SCORING ───────────────────────────────────────────────────────────────
    scored = (
        compute_composite_v2(
            latest,
            weights=scoring_weights,
            params=scoring_params,
            market_breadth_score=_market_breadth_score,
            market_vol_regime_score=_market_vol_score,
            price_frames=tradable_enriched,
        )
        if not latest.empty
        else pd.DataFrame()
    )
    logger.info("Scored rows=%d", len(scored))

    if not scored.empty:
        _ap = action_params if action_params is not None else ACTIONPARAMS_V2
        _lead_w = _ap.get("leadership_boost_weight", 0.10)
        scored["scorecomposite_v2"] = (
            scored["scorecomposite_v2"]
            + _lead_w * scored.get("leadership_strength", 0.0)
        ).clip(0, 1)

        # ══════════════════════════════════════════════════════════════════════
        # ── Per-ticker volfavorability (rank-based with atr14pct fallback) ───
        # ══════════════════════════════════════════════════════════════════════
        # Build the vol proxy: prefer realizedvol20d, fall back to atr14pct
        # (annualized approximation: atr14pct × sqrt(252) ≈ atr14pct × 15.87)
        _rv = pd.to_numeric(
            scored.get("realizedvol20d", pd.Series(dtype=float, index=scored.index)),
            errors="coerce",
        )
        _rv_nan_before = int(_rv.isna().sum())

        if "atr14pct" in scored.columns:
            _atr_proxy = pd.to_numeric(scored["atr14pct"], errors="coerce") * 15.87
            _rv = _rv.fillna(_atr_proxy)

        _rv_nan_after = int(_rv.isna().sum())
        _rv_valid = _rv.dropna()

        logger.info(
            "Volfavorability vol proxy: realizedvol20d NaN=%d/%d, "
            "after atr14pct fallback NaN=%d/%d, valid=%d std=%.6f",
            _rv_nan_before, len(scored),
            _rv_nan_after, len(scored),
            len(_rv_valid),
            float(_rv_valid.std()) if len(_rv_valid) > 1 else 0.0,
        )

        if len(_rv_valid) >= 5 and _rv_valid.std() > 1e-6:
            # Percentile rank: 0.0 = lowest vol (best), 1.0 = highest vol (worst)
            _rv_pctile = _rv.rank(pct=True, method="average").fillna(0.5)

            # ── WIDER SPREAD: ±0.20 from base ────────────────────────────────
            # Low-vol stocks get up to +0.20, high-vol get up to -0.20
            _vol_rank_adj = 0.20 * (1.0 - 2.0 * _rv_pctile)

            # ── Direction bonus for high-vol stocks: ±0.10 ───────────────────
            # High-vol + strong trend → partial rescue (+0.10 max)
            # High-vol + no trend → no rescue
            _has_direction = False
            if "scoretrend" in scored.columns:
                _st = pd.to_numeric(scored["scoretrend"], errors="coerce")
                if _st.notna().sum() > 0 and _st.std() > 0.03:
                    _dir_signal = ((_st - 0.5) * 2.0).fillna(0.0).clip(-1.0, 1.0)
                    _has_direction = True

            if not _has_direction:
                # Fallback: RSI deviation from 50 + ADX strength
                _rsi = pd.to_numeric(
                    scored.get("rsi14", pd.Series(50.0, index=scored.index)),
                    errors="coerce",
                ).fillna(50.0)
                _adx = pd.to_numeric(
                    scored.get("adx14", pd.Series(20.0, index=scored.index)),
                    errors="coerce",
                ).fillna(20.0)
                _rsi_dev = ((_rsi - 50.0) / 25.0).clip(-1.0, 1.0)
                _adx_weight = ((_adx - 15.0) / 25.0).clip(0.0, 1.0)
                _dir_signal = (_rsi_dev * _adx_weight).clip(-1.0, 1.0)

            # Only above-median vol stocks get direction adjustment
            _rv_excess = (_rv_pctile - 0.5).clip(0.0, 0.5)
            _direction_adj = 0.20 * _rv_excess * _dir_signal

            _base_vf = vol_info["vol_favorability"]
            scored["volfavorability"] = (
                _base_vf + _vol_rank_adj + _direction_adj
            ).clip(0.10, 0.95)

            # ── RELATIVE LABELS (quartile-based) ─────────────────────────────
            # Quartile assignment: Q1=lowest vol (best), Q4=highest vol (worst)
            _quartiles = pd.cut(
                _rv_pctile,
                bins=[-0.01, 0.25, 0.50, 0.75, 1.01],
                labels=[1, 2, 3, 4],
            ).astype(int).fillna(2)

            scored["volfavorability_quartile"] = _quartiles.values
            scored["volfavorability_label"] = [
                _vol_fav_label(float(v), int(q))
                for v, q in zip(scored["volfavorability"], scored["volfavorability_quartile"])
            ]

            logger.info(
                "Per-ticker volfavorability (rank+fallback): base=%.4f "
                "vol_rank_adj range=[%.4f, %.4f] "
                "direction_adj range=[%.4f, %.4f] "
                "final range=[%.4f, %.4f] std=%.4f "
                "quartile distribution: Q1=%d Q2=%d Q3=%d Q4=%d",
                _base_vf,
                float(_vol_rank_adj.min()), float(_vol_rank_adj.max()),
                float(_direction_adj.min()), float(_direction_adj.max()),
                float(scored["volfavorability"].min()),
                float(scored["volfavorability"].max()),
                float(scored["volfavorability"].std()),
                int((_quartiles == 1).sum()),
                int((_quartiles == 2).sum()),
                int((_quartiles == 3).sum()),
                int((_quartiles == 4).sum()),
            )
        else:
            # Degenerate case: not enough data to differentiate
            scored["volfavorability"] = vol_info["vol_favorability"]
            scored["volfavorability_quartile"] = 2
            scored["volfavorability_label"] = _vol_fav_label(
                vol_info["vol_favorability"], 2
            )
            logger.warning(
                "Per-ticker volfavorability: insufficient vol data "
                "(valid=%d std=%.6f) — using uniform base=%.4f",
                len(_rv_valid),
                float(_rv_valid.std()) if len(_rv_valid) > 1 else 0.0,
                vol_info["vol_favorability"],
            )

        logger.info(
            "Per-ticker volfavorability summary: min=%.4f median=%.4f max=%.4f "
            "std=%.4f nunique=%d",
            float(scored["volfavorability"].min()),
            float(scored["volfavorability"].median()),
            float(scored["volfavorability"].max()),
            float(scored["volfavorability"].std()),
            int(scored["volfavorability"].nunique()),
        )

        # ══════════════════════════════════════════════════════════════════════
        # ── Modulate scorerisk by per-ticker volfavorability ─────────────────
        # ══════════════════════════════════════════════════════════════════════
        # Stocks with higher volfavorability → less risk penalty (higher scorerisk)
        # Stocks with lower volfavorability → more risk penalty (lower scorerisk)
        # The deviation from base determines the adjustment magnitude.
        _vf_base = vol_info["vol_favorability"]
        _vf_deviation = scored["volfavorability"] - _vf_base
        # Scale: ±0.20 volfav deviation → ±0.10 scorerisk adjustment
        _scorerisk_adj = _vf_deviation * 0.50
        _scorerisk_before = scored["scorerisk"].copy()
        scored["scorerisk"] = (scored["scorerisk"] + _scorerisk_adj).clip(0.0, 1.0)

        logger.info(
            "scorerisk modulation by volfav: adj range=[%.4f, %.4f] "
            "scorerisk before=[%.4f, %.4f] after=[%.4f, %.4f]",
            float(_scorerisk_adj.min()), float(_scorerisk_adj.max()),
            float(_scorerisk_before.min()), float(_scorerisk_before.max()),
            float(scored["scorerisk"].min()), float(scored["scorerisk"].max()),
        )
        # ══════════════════════════════════════════════════════════════════════

        # ── D3. ENRICH SCORED TABLE WITH ROTATION ────────────────────────────
        enrich_params = {
            "regime_scores": {
                "leading":    1.00,
                "improving":  0.65,
                "weakening":  0.30,
                "lagging":    0.00,
                "unknown":    0.15,
            },
            "apply_etf_boost": _etf_has_composite,
            "recompute_composite": True,
            "composite_weights": {
                "scoretrend":         0.30,
                "scoreparticipation": 0.20,
                "scorerisk":          0.15,
                "scoreregime":        0.15,
                "scorerotation":      0.20,
            },
        }

        scored = enrich_with_rotation(
            scored_df=scored,
            rotation_result=rotation_result,
            params=enrich_params,
        )

        _sp = signal_params if signal_params is not None else SIGNALPARAMS_V2
        _diag_entry = _sp.get("base_entry_threshold", 0.55)
        logger.info(
            "Score diagnostics: min=%.4f median=%.4f max=%.4f >=entry(%.2f)=%d >=0.50=%d",
            float(scored["scorecomposite_v2"].min()),
            float(scored["scorecomposite_v2"].median()),
            float(scored["scorecomposite_v2"].max()),
            _diag_entry,
            int((scored["scorecomposite_v2"] >= _diag_entry).sum()),
            int((scored["scorecomposite_v2"] >= 0.50).sum()),
        )
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "scoretrend", "scoreparticipation", "scorerisk", "scoreregime", "scorerotation", "scorepenalty", "scorecomposite_v2", "rsi14", "adx14", "relativevolume", "sectrsregime"] if c in scored.columns]
            logger.debug("Top scored names:\n%s", scored.sort_values("scorecomposite_v2", ascending=False)[cols].head(30).to_string(index=False))

    # ── DIAGNOSTIC: score component audit for top candidates ─────────────────
    if not scored.empty and logger.isEnabledFor(logging.INFO):
        _diag = scored.nlargest(30, "scorecomposite_v2")[
            [c for c in [
                "ticker", "sector", "sectrsregime",
                "scoretrend", "scoreparticipation", "scorerisk",
                "scoreregime", "scorerotation", "scorepenalty",
                "scorecomposite_v2",
                "breadthscore", "volregimescore",
                "volfavorability", "volfavorability_label",
                "rsi14", "adx14", "relativevolume", "rszscore",
            ] if c in scored.columns]
        ].copy()
        logger.info(
            "DIAGNOSTIC — Top 30 score component breakdown:\n%s",
            _diag.to_string(index=False, float_format="%.4f"),
        )

        # Borderline analysis: names near the 0.68 cutoff
        _border = scored[
            scored["scorecomposite_v2"].between(0.65, 0.72)
        ].sort_values("scorecomposite_v2", ascending=False)
        if not _border.empty:
            logger.info(
                "DIAGNOSTIC — Borderline names (0.65–0.72):\n%s",
                _border[
                    [c for c in [
                        "ticker", "scoretrend", "scoreparticipation",
                        "scorerisk", "scoreregime", "scorerotation",
                        "scorecomposite_v2", "sectrsregime",
                        "volfavorability", "volfavorability_label",
                    ] if c in _border.columns]
                ].to_string(index=False, float_format="%.4f"),
            )
    # ── END DIAGNOSTIC ───────────────────────────────────────────────────────

    signaled = apply_signals_v2(scored, params=signal_params) if not scored.empty else pd.DataFrame()
    logger.info("Signals rows=%d", len(signaled))
    if not signaled.empty and logger.isEnabledFor(logging.DEBUG):
        cols = [c for c in ["ticker", "scorecomposite_v2", "sigeffectiveentrymin_v2", "sigconfirmed_v2", "sigexit_v2", "rsi14", "adx14"] if c in signaled.columns]
        logger.debug("Signal preview:\n%s", signaled.sort_values("scorecomposite_v2", ascending=False)[cols].head(30).to_string(index=False))

    converged = apply_convergence_v2(signaled, params=convergence_params) if not signaled.empty else pd.DataFrame()
    logger.info("Converged rows=%d", len(converged))
    if not converged.empty:
        if "convergence_label_v2" in converged.columns:
            cl_dist = converged["convergence_label_v2"].value_counts().to_dict()
            logger.info("Convergence label distribution: %s", cl_dist)
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "scorecomposite_v2", "scoreadjusted_v2", "sigeffectiveentrymin_v2", "sigconfirmed_v2", "sigexit_v2", "convergence_label_v2", "rotationrec", "rsi14", "adx14", "relativevolume"] if c in converged.columns]
            logger.debug("Converged preview:\n%s", converged.sort_values(cols[1] if len(cols) > 1 else converged.columns[0], ascending=False)[cols].head(30).to_string(index=False))

    action_table = _generate_actions(converged, params=action_params) if not converged.empty else pd.DataFrame()
    logger.info("Action table rows=%d", len(action_table))
    if not action_table.empty:
        logger.info("Action counts=%s", action_table["action_v2"].value_counts().to_dict())
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "action_v2", "conviction_v2", "scorecomposite_v2", "scoreadjusted_v2", "score_percentile_v2", "rsi14", "adx14", "relativevolume", "sectrsregime", "rotationrec", "convergence_label_v2", "action_reason_v2"] if c in action_table.columns]
            logger.debug("Action table preview:\n%s", action_table[cols].head(50).to_string(index=False))

    if precomputed:
        review_table = pd.DataFrame()
        selling_exhaustion_table = pd.DataFrame()
        logger.info("Skipping review_table + selling_exhaustion_table (precomputed=True)")
    else:
        review_table = _build_review_table(action_table, action_params=action_params) if not action_table.empty else pd.DataFrame()
        selling_exhaustion_table = _build_selling_exhaustion_table(
            tradable_enriched,
            breadth_info.get("breadth_regime", "unknown"),
            last_vol.get("volregime", "unknown"),
            leadership_snapshot,
        )

    if precomputed:
        portfolio = {
            "selected": pd.DataFrame(),
            "meta": {
                "selected_count": 0,
                "candidate_count": 0,
                "target_exposure": 0.0,
                "breadth_regime": breadth_info.get("breadth_regime", "unknown"),
                "vol_regime": last_vol.get("volregime", "unknown"),
            },
        }
        logger.info("Skipping portfolio construction (precomputed=True)")
    else:
        params = portfolio_params or {}
        portfolio = (
            build_portfolio_v2(
                action_table,
                max_positions=params.get("max_positions", DEFAULT_MAX_POSITIONS),
                max_sector_weight=params.get("max_sector_weight", DEFAULT_MAX_SECTOR_WEIGHT),
                max_theme_names=params.get("max_theme_names", DEFAULT_MAX_THEME_NAMES),
                max_single_weight=params.get("max_single_weight", DEFAULT_MAX_SINGLE_WEIGHT),
                min_weight=params.get("min_weight", DEFAULT_MIN_WEIGHT),
            )
            if not action_table.empty
            else {
                "selected": pd.DataFrame(),
                "meta": {
                    "selected_count": 0,
                    "candidate_count": 0,
                    "target_exposure": 0.0,
                    "breadth_regime": breadth_info.get("breadth_regime", "unknown"),
                    "vol_regime": last_vol.get("volregime", "unknown"),
                },
            }
        )

    logger.info(
        "Pipeline skip summary: scored=%d skipped=%d",
        len(latest), len(skipped_table),
    )
    logger.info("Portfolio meta=%s", portfolio.get("meta", {}))

    return {
        "market": market,
        "latest": latest,
        "skipped_table": skipped_table,
        "scored": scored,
        "signals": signaled,
        "converged": converged,
        "action_table": action_table,
        "review_table": review_table,
        "selling_exhaustion_table": selling_exhaustion_table,
        "portfolio": portfolio,
        "regime_df": regime_df,
        "breadth_info": breadth_info,
        "breadth_df": breadth_computed_df,
        "vol_info": vol_info,
        "sector_summary": sector_summary,
        "sector_regimes": sector_regimes,
        "etf_ranking": etf_ranking,
        "thematic_summary": thematic_summary,
        "thematic_regimes": thematic_regimes,
        "leadership_snapshot": leadership_snapshot,
    }