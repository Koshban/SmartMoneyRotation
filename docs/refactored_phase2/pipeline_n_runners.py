"""refactor/pipeline_v2.py"""
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

from compute.indicators import compute_all_indicators

from common.sector_map import get_sector_or_class, get_theme
from refactor.strategy.enrich_v2 import enrich_with_rotation
from refactor.strategy.adapters_v2 import ensure_columns
from refactor.strategy.breadth_v2 import compute_breadth
from refactor.strategy.portfolio_v2 import (
    build_portfolio_v2,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_MAX_SECTOR_WEIGHT,
    DEFAULT_MAX_THEME_NAMES,
    DEFAULT_MAX_SINGLE_WEIGHT,
    DEFAULT_MIN_WEIGHT,
)
from refactor.strategy.regime_v2 import classify_volatility_regime
from refactor.strategy.rotation_v2 import compute_sector_rotation
from refactor.strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
from refactor.strategy.scoring_v2 import compute_composite_v2
from refactor.strategy.signals_v2 import apply_convergence_v2, apply_signals_v2
from refactor.common.config_refactor import (
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

# ── Vol regime: string label → numeric risk score (0=calm … 1=chaotic) ──  # ← NEW
_VOL_REGIME_RISK_MAP: dict[str, float] = {
    "calm":     0.10,
    "moderate": 0.35,
    "elevated": 0.60,
    "volatile": 0.80,
    "chaotic":  1.00,
}


def _is_missing_value(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _safe_float_or_none(val) -> float | None:                               # ← NEW helper
    """Convert to float; return None on failure or NaN."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _classify_breadth_regime(breadth_df: pd.DataFrame | None) -> dict:
    """Extract breadth context from the last row of the breadth DataFrame."""  # ← FIXED: enriched
    if breadth_df is None or breadth_df.empty:
        return {
            "breadth_regime": "unknown",
            "breadthscore": None,
            "dispersion20": None,
            "dispersion": None,
            "above_sma50_pct": None,                                         # ← NEW
            "above_sma200_pct": None,                                        # ← NEW
            "advancing_pct": None,                                           # ← NEW
            "net_highs_pct": None,                                           # ← NEW
        }
    row = breadth_df.iloc[-1]
    regime = row.get("breadthregime", row.get("breadth_regime", "unknown"))
    score = row.get("breadthscore", row.get("breadth_score", None))
    disp20 = row.get("dispersion20", None)
    disp = row.get("dispersion_daily", row.get("dispersion", None))
    above_sma50 = row.get("above_sma50_pct", row.get("pct_above_sma50",     # ← NEW
                  row.get("above_sma50", None)))
    above_sma200 = row.get("above_sma200_pct", row.get("pct_above_sma200",  # ← NEW
                   row.get("above_sma200", None)))
    advancing = row.get("advancing_pct", row.get("pct_advancing",            # ← NEW
                row.get("advancing", None)))
    net_highs = row.get("net_highs_pct", row.get("net_new_highs_pct",       # ← NEW
                row.get("net_highs", None)))
    return {
        "breadth_regime": regime,
        "breadthscore": score,
        "dispersion20": disp20,
        "dispersion": disp,
        "above_sma50_pct": _safe_float_or_none(above_sma50),                # ← NEW
        "above_sma200_pct": _safe_float_or_none(above_sma200),              # ← NEW
        "advancing_pct": _safe_float_or_none(advancing),                     # ← NEW
        "net_highs_pct": _safe_float_or_none(net_highs),                     # ← NEW
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


def _fill_missing_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = (
        pd.to_numeric(out["close"], errors="coerce")
        if "close" in out.columns
        else None
    )
    if close is None or int(close.notna().sum()) < 20:
        return out

    if "realizedvol20d" not in out.columns:
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
        "sb_pct=%.2f sb_score=%.3f sb_confirmed=%s sb_regimes=%s "
        "max_sb=%d bu_pct=%.2f bu_score=%.3f ho_pct=%.2f ho_score=%.3f",
        sell_floor, sell_pct_floor,
        sb_min_pct, sb_min_score,
        sb_require_confirmed, sb_allowed_regimes, max_strong_buy,
        bu_min_pct, bu_min_score,
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

        if s < sell_floor or pv <= sell_pct_floor:
            action = "SELL"
            reason = "Below sell floor"
        elif (
            pv >= sb_min_pct
            and s >= max(sb_min_score, e + sb_above)
            and (not sb_require_confirmed or confirmed_i)
            and (not sb_allowed_regimes or regime_i in sb_allowed_regimes)
        ):
            action = "STRONG_BUY"
            reason = (
                f"Top-tier: score={s:.3f} pct={pv:.0%} "
                f"confirmed={confirmed_i} regime={regime_i}"
            )
        elif pv >= bu_min_pct and s >= max(bu_min_score, e + bu_above):
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
            sort_keys[idx] -= 10  # shift from SB tier (40+) to BUY tier (30+)
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
    for i_idx, i_val in enumerate(out.index):
        s = float(score.loc[i_val])
        pv = float(pct.loc[i_val])
        e = float(entry.loc[i_val])
        if pv >= sb_min_pct and s >= max(sb_min_score, e + sb_above):
            if sb_require_confirmed and not bool(sig_confirmed.loc[i_val]):
                _sb_blocked_confirmed += 1
            elif sb_allowed_regimes and str(sect_regime.loc[i_val]) not in sb_allowed_regimes:
                _sb_blocked_regime += 1
    logger.info(
        "STRONG_BUY gate impact: score+pct qualified=%d "
        "blocked_by_confirmation=%d blocked_by_regime=%d "
        "passed_all_gates=%d after_cap=%d",
        _sb_before_cap + _sb_blocked_confirmed + _sb_blocked_regime,
        _sb_blocked_confirmed,
        _sb_blocked_regime,
        _sb_before_cap,
        sum(1 for a in actions if a == "STRONG_BUY"),
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
        "above_sma50=%s above_sma200=%s advancing=%s net_highs=%s",           # ← FIXED: richer log
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
    # ── FIXED: derive vol regime score from label if raw score is 0.0 ────── # ← NEW
    # classify_volatility_regime may produce volregimescore=0.0 as a valid
    # value for "calm", but this looks like "missing" in reports and collapses
    # the vol dimension.  Derive a small positive score from the label so
    # that the scoring formula and display both work meaningfully.
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

        # ── FIXED: re-stamp breadth context after ensure_columns ──────────  # ← NEW
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

    # ── FIXED: derive vol score from label when raw score is 0.0 or None ──  # ← NEW
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

    # ── Build vol_info dict for report rendering ──────────────────────────  # ← NEW
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

    scored = (
        compute_composite_v2(
            latest,
            weights=scoring_weights,
            params=scoring_params,
            market_breadth_score=_market_breadth_score,
            market_vol_regime_score=_market_vol_score,
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

        # ── D3. ENRICH SCORED TABLE WITH ROTATION ────────────────────────────
        # ── D3. ENRICH SCORED TABLE WITH ROTATION ────────────────────────────
        enrich_params = {
            "regime_scores": {
                "leading":    1.00,
                "improving":  0.65,
                "weakening":  0.30,
                "lagging":    0.00,
                "unknown":    0.15,
            },
            "apply_etf_boost": not etf_ranking.empty,   # ← CHANGED: was True
            "recompute_composite": True,
            "composite_weights": {
                "scoretrend":         0.30,
                "scoreparticipation": 0.20,
                "scorerisk":          0.15,
                "scoreregime":        0.15,
                "scorerotation":      0.20,
            },
        }
        # ── TEMPORARY DIAGNOSTIC: score component audit for STRONG_BUY candidates ──
    if not scored.empty and logger.isEnabledFor(logging.INFO):
        _diag = scored.nlargest(30, "scorecomposite_v2")[
            [c for c in [
                "ticker", "sector", "sectrsregime",
                "scoretrend", "scoreparticipation", "scorerisk",
                "scoreregime", "scorerotation", "scorepenalty",
                "scorecomposite_v2",
                "breadthscore", "volregimescore",
                "volfavorability",                                            # ← NEW
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
                    ] if c in _border.columns]
                ].to_string(index=False, float_format="%.4f"),
            )
    # ── END DIAGNOSTIC ──
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
        "vol_info": vol_info,                                                # ← NEW
        "sector_summary": sector_summary,
        "sector_regimes": sector_regimes,
        "etf_ranking": etf_ranking,
        "leadership_snapshot": leadership_snapshot,
    }

######################
"""refactor/runner_v2.py"""
from __future__ import annotations

from utils.run_logger import RunLogger
from utils.display_results import print_run_summary

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


import pandas as pd


from common.config import DATA_DIR, LOGS_DIR
from refactor.common.market_config_v2 import get_market_config_v2
from refactor.pipeline_v2 import run_pipeline_v2
from refactor.report_v2 import build_report_v2, to_text_v2


# ── SIGNAL WRITER: optional import ────────────────────────────
try:
    from signal_writer import write_signals as _write_signals
    _HAS_SIGNAL_WRITER = True
except ImportError:
    _HAS_SIGNAL_WRITER = False

# ── HTML REPORT: optional import ──────────────────────────────
try:
    from utils.html_report import build_html_report, save_html_report
    _HAS_HTML_REPORT = True
except ImportError:
    _HAS_HTML_REPORT = False

# ── EMAIL: optional import ────────────────────────────────────
try:
    from utils.email_report import send_report_email
    _HAS_EMAIL = True
except ImportError:
    _HAS_EMAIL = False


logger = logging.getLogger("refactor.runner_v2")


MARKET_PARQUET = {
    "US": "us_cash.parquet",
    "HK": "hk_cash.parquet",
    "IN": "in_cash.parquet",
}
DATE_CANDIDATE_COLS = ("date", "datetime", "timestamp", "dt")
TICKER_CANDIDATE_COLS = ("ticker", "symbol")
BENCHMARK_FALLBACKS = {
    "US": ["SPY", "QQQ", "IWM"],
    "HK": ["2800.HK"],
    "IN": ["NIFTYBEES.NS"],
}


# ═══════════════════════════════════════════════════════════════
#  THEMATIC ETF DEFINITIONS
# ═══════════════════════════════════════════════════════════════

THEMATIC_ETF_MAP: dict[str, dict[str, list[str]]] = {
    "US": {
        "Semiconductors":       ["SOXX", "SMH"],
        "AI / Robotics":        ["QTUM", "AIQ", "BOTZ", "ROBO"],
        "Cybersecurity":        ["HACK", "CIBR", "BUG"],
        "Clean Energy":         ["ICLN", "TAN", "QCLN"],
        "Cloud / Software":     ["SKYY", "WCLD", "IGV"],
        "Blockchain / Crypto":  ["BLOK", "BKCH", "BITQ"],
        "Genomics / Biotech":   ["ARKG", "XBI"],
        "Space / Defense":      ["UFO", "ITA"],
        "Internet / China Tech": ["KWEB", "FDN"],
        "Fintech":              ["FINX", "ARKF"],
        "Momentum Factor":      ["MTUM"],
    },
    "HK": {
        "China Tech":           ["3067.HK", "2845.HK"],
        "Semiconductors":       ["3135.HK"],
    },
    "IN": {},
}

THEMATIC_ETF_TICKERS: dict[str, set[str]] = {
    mkt: {
        etf
        for etfs in themes.values()
        for etf in etfs
    }
    for mkt, themes in THEMATIC_ETF_MAP.items()
}

SECTOR_ETF_MAP: dict[str, str] = {
    "Technology":              "XLK",
    "Consumer Discretionary":  "XLY",
    "Communication Services":  "XLC",
    "Financials":              "XLF",
    "Healthcare":              "XLV",
    "Industrials":             "XLI",
    "Consumer Staples":        "XLP",
    "Energy":                  "XLE",
    "Utilities":               "XLU",
    "Real Estate":             "XLRE",
    "Materials":               "XLB",
}


# ═══════════════════════════════════════════════════════════════
#  DISPLAY FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════

_BOX_WIDTH = 110

_REGIME_ICON: dict[str, str] = {
    "leading":   "🟢",
    "improving": "🔵",
    "weakening": "🟡",
    "lagging":   "🔴",
    "unknown":   "⚪",
}


def _box_header(emoji: str, title: str) -> list[str]:
    """Produce a ╔═══╗ / ║ ... ║ / ╚═══╝ header block."""
    inner = _BOX_WIDTH - 2
    content = f" {emoji}  {title} "
    padded = content + " " * max(0, inner - len(content))
    return [
        "╔" + "═" * inner + "╗",
        "║" + padded[:inner] + "║",
        "╚" + "═" * inner + "╝",
    ]


def _sub_header(text: str, width: int = 90) -> str:
    """Produce a ── Title (extra) ── style sub-header line."""
    dash_len = max(4, width - len(text) - 6)
    return f"  ── {text}  {'─' * dash_len}"


def _hbar(value: float, max_val: float, width: int = 16) -> str:
    """Render a thin Unicode horizontal bar chart segment."""
    if max_val <= 0:
        return " " * width
    ratio = max(0.0, min(1.0, value / max_val))
    filled = ratio * width
    full = int(filled)
    frac = filled - full
    partials = " ▏▎▍▌▋▊▉"
    bar = "█" * full
    if full < width:
        bar += partials[min(int(frac * 8), 7)]
        bar += " " * (width - full - 1)
    return bar[:width]


def _signed(val: float, width: int = 8, decimals: int = 4, pct: bool = False) -> str:
    """Format a signed numeric value with explicit +/- prefix."""
    if pct:
        formatted = f"{val * 100:+.2f}%"
    else:
        formatted = f"{val:+.{decimals}f}"
    return formatted.rjust(width)


def _detect_stale_columns(
    df: pd.DataFrame,
    columns: list[str],
    threshold: float = 0.95,
) -> list[str]:
    """Return column names where ≥ threshold fraction of non-null values are identical."""
    stale: list[str] = []
    for col in columns:
        if col not in df.columns:
            continue
        series = df[col].dropna()
        if len(series) < 3:
            continue
        if series.nunique() <= 1:
            stale.append(col)
            continue
        top_pct = series.value_counts(normalize=True).iloc[0]
        if top_pct >= threshold:
            stale.append(col)
    return stale


# ═══════════════════════════════════════════════════════════════
#  THEMATIC FRAME EXTRACTION
# ═══════════════════════════════════════════════════════════════

def _extract_thematic_frames(
    universe_frames: dict[str, pd.DataFrame],
    market: str,
) -> tuple[dict[str, pd.DataFrame], dict[str, list[str]]]:
    market_upper = market.upper()
    theme_map = THEMATIC_ETF_MAP.get(market_upper, {})

    thematic_frames: dict[str, pd.DataFrame] = {}
    available_map: dict[str, list[str]] = {}
    missing_tickers: list[str] = []

    for theme, tickers in theme_map.items():
        found_in_theme: list[str] = []
        for ticker in tickers:
            if ticker in universe_frames:
                thematic_frames[ticker] = universe_frames[ticker]
                found_in_theme.append(ticker)
            else:
                missing_tickers.append(ticker)
        if found_in_theme:
            available_map[theme] = found_in_theme

    logger.info(
        "Thematic ETFs: market=%s configured_themes=%d available_themes=%d "
        "configured_etfs=%d found_etfs=%d missing=%d",
        market_upper,
        len(theme_map),
        len(available_map),
        sum(len(v) for v in theme_map.values()),
        len(thematic_frames),
        len(missing_tickers),
    )
    if missing_tickers:
        logger.info(
            "Missing thematic ETFs in parquet: %s", sorted(missing_tickers),
        )
    if logger.isEnabledFor(logging.DEBUG):
        for theme, tickers in available_map.items():
            logger.debug("  Theme '%s': %s", theme, tickers)

    return thematic_frames, available_map


# ═══════════════════════════════════════════════════════════════
#  PORTFOLIO SELECTION GAP DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════

def _log_portfolio_selection_gaps(result: dict[str, Any]) -> None:
    scored_df: pd.DataFrame | None = None
    for key in _SCORED_TABLE_KEYS:
        candidate = result.get(key)
        if isinstance(candidate, pd.DataFrame) and not candidate.empty:
            scored_df = candidate
            break
    if scored_df is None:
        return

    ticker_col = _find_col(scored_df, TICKER_CANDIDATE_COLS)
    signal_col = _find_col(scored_df, _SIGNAL_COL_CANDIDATES)
    if ticker_col is None or signal_col is None:
        return

    sb_mask = scored_df[signal_col].astype(str).str.upper() == "STRONG_BUY"
    all_strong_buy = set(scored_df.loc[sb_mask, ticker_col].astype(str))
    if not all_strong_buy:
        return

    report = result.get("report_v2", {})
    selected_set = _extract_portfolio_set(report)

    for key in ("portfolio_tickers", "selected_tickers", "selected_names"):
        extra = result.get(key)
        if isinstance(extra, (list, set, frozenset)):
            selected_set |= {str(t) for t in extra}

    dropped = sorted(all_strong_buy - selected_set)
    if dropped:
        drop_rows = scored_df[
            scored_df[ticker_col].astype(str).isin(dropped)
        ]
        preview_cols = [
            c for c in [ticker_col, "composite_score", "final_score", "score",
                        "sector", "gics_sector", "avg_dollar_volume", "dollar_volume",
                        "market_cap", signal_col]
            if c in drop_rows.columns
        ]
        logger.warning(
            "Portfolio selection gap: %d STRONG_BUY names were NOT selected "
            "into TOP RECS: %s  "
            "(likely cause: sector/theme concentration cap, liquidity filter, "
            "or max_positions limit in portfolio construction)",
            len(dropped), dropped,
        )
        if preview_cols:
            logger.warning(
                "Dropped STRONG_BUY details:\n%s",
                drop_rows[preview_cols].to_string(index=False),
            )
    else:
        logger.info(
            "Portfolio selection: all %d STRONG_BUY names included in TOP RECS",
            len(all_strong_buy),
        )


def _parse_iso_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    return date.fromisoformat(value)


def setup_logging(verbose: bool = False) -> Path:
    log_dir = Path(LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"runner_v2_{ts}.log"
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return log_file


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run refactor v2 pipeline from desktop CLI")
    p.add_argument("--market", default="US", help="US, HK, or IN")
    p.add_argument("--start-date", type=_parse_iso_date, default=None, help="Inclusive start date YYYY-MM-DD")
    p.add_argument("--end-date", type=_parse_iso_date, default=None, help="Inclusive end date YYYY-MM-DD")
    p.add_argument("--parquet-path", default=None, help="Optional explicit parquet file path")
    p.add_argument("--print-report", action="store_true", help="Print plain-text v2 report to stdout")
    p.add_argument("--no-email", action="store_true", help="Skip emailing the HTML report")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def _resolve_parquet_path(market: str, parquet_path: str | None = None) -> Path:
    if parquet_path:
        return Path(parquet_path)
    m = market.upper()
    if m not in MARKET_PARQUET:
        raise ValueError(f"Unknown market {market!r}")
    return Path(DATA_DIR) / MARKET_PARQUET[m]


def _find_date_col(df: pd.DataFrame) -> str:
    for col in DATE_CANDIDATE_COLS:
        if col in df.columns:
            return col
    if isinstance(df.index, pd.DatetimeIndex):
        return "__index__"
    raise ValueError(f"Could not find a date column. Tried {DATE_CANDIDATE_COLS}")


def _find_ticker_col(df: pd.DataFrame) -> str:
    for col in TICKER_CANDIDATE_COLS:
        if col in df.columns:
            return col
    raise ValueError(f"Could not find a ticker column. Tried {TICKER_CANDIDATE_COLS}")


def _coerce_and_filter_dates(df: pd.DataFrame, start_date: date | None, end_date: date | None) -> pd.DataFrame:
    out = df.copy()
    date_col = _find_date_col(out)
    if date_col == "__index__":
        dates = pd.to_datetime(out.index, errors="coerce")
    else:
        out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
        dates = out[date_col]
    mask = pd.Series(True, index=out.index)
    if start_date is not None:
        mask &= dates >= pd.Timestamp(start_date)
    if end_date is not None:
        mask &= dates <= pd.Timestamp(end_date)
    out = out.loc[mask].copy()
    if date_col != "__index__":
        out = out.sort_values(date_col)
    else:
        out = out.sort_index()
    return out


def _build_frames_from_panel(df: pd.DataFrame, market: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame | None]:
    ticker_col = _find_ticker_col(df)
    date_col = _find_date_col(df)
    work = df.copy()
    if date_col != "__index__":
        work[date_col] = pd.to_datetime(work[date_col], errors="coerce")
    cfg = get_market_config_v2(market)
    benchmark = cfg["benchmark"]
    universe_frames: dict[str, pd.DataFrame] = {}

    for ticker, g in work.groupby(ticker_col):
        g = g.copy()
        g["ticker"] = str(ticker)
        if date_col != "__index__":
            g = g.sort_values(date_col).set_index(date_col)
        else:
            g = g.sort_index()
        g.index = pd.to_datetime(g.index)
        g.index.name = "date"
        universe_frames[str(ticker)] = g

    bench_df = universe_frames.get(benchmark)

    if bench_df is None:
        for alt in BENCHMARK_FALLBACKS.get(market.upper(), []):
            if alt in universe_frames:
                bench_df = universe_frames[alt]
                logger.warning("Benchmark %s missing; using fallback %s", benchmark, alt)
                break
    if bench_df is None or bench_df.empty:
        raise ValueError(f"Benchmark frame not found for market {market}: expected {benchmark}")
    breadth_df = None
    logger.info("Built market frames: total_symbols=%d benchmark=%s benchmark_rows=%d", len(universe_frames), benchmark, len(bench_df))
    if logger.isEnabledFor(logging.DEBUG):
        lengths = sorted(((k, len(v)) for k, v in universe_frames.items()), key=lambda x: x[1], reverse=True)
        logger.debug("Top symbol frame sizes:\n%s", "\n".join(f"{k}: {n}" for k, n in lengths[:25]))
    return universe_frames, bench_df, breadth_df


def _human_file_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def load_market_data_v2(
    market: str,
    start_date: date | None = None,
    end_date: date | None = None,
    parquet_path: str | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame | None]:
    path = _resolve_parquet_path(market, parquet_path)
    resolved_path = path.expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {resolved_path}")
    size_bytes = resolved_path.stat().st_size
    logger.info(
        "Resolved parquet file: market=%s override=%s path=%s size_bytes=%d size=%s",
        market.upper(),
        bool(parquet_path),
        resolved_path,
        size_bytes,
        _human_file_size(size_bytes),
    )
    panel = pd.read_parquet(resolved_path)
    logger.info("Rows before date filter: %s", len(panel))
    panel = _coerce_and_filter_dates(panel, start_date, end_date)
    logger.info("Rows after date filter: %s", len(panel))
    if panel.empty:
        raise ValueError("No rows remain after date filtering")
    ticker_col = _find_ticker_col(panel)
    date_col = _find_date_col(panel)
    logger.info("Filtered panel summary: symbols=%d date_col=%s ticker_col=%s", panel[ticker_col].nunique(), date_col, ticker_col)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Filtered panel head:\n%s", panel.head(10).to_string(index=False))
        logger.debug("Filtered panel tail:\n%s", panel.tail(10).to_string(index=False))
    return _build_frames_from_panel(panel, market)


# ═══════════════════════════════════════════════════════════════
#  SIGNAL WRITER HELPERS
# ═══════════════════════════════════════════════════════════════

_BUY_LABELS = frozenset({
    "BUY", "STRONG_BUY", "BUY_SCORING", "BUY_ROTATION",
})
_SELL_LABELS = frozenset({
    "SELL", "STRONG_SELL", "SELL_SCORING", "SELL_ROTATION", "REDUCE",
})


def _normalise_action(raw: str) -> str:
    raw_upper = str(raw).upper().strip()
    if raw_upper in _BUY_LABELS:
        return "BUY"
    if raw_upper in _SELL_LABELS:
        return "SELL"
    return "HOLD"


_SCORED_TABLE_KEYS = (
    "final_table", "scored_table", "ranking_table",
    "composite_table", "ticker_scores", "scores",
)
_SCORE_COL_CANDIDATES = (
    "composite_score", "final_score", "score", "total_score",
    "composite", "blended_score",
)
_SIGNAL_COL_CANDIDATES = (
    "signal", "action", "recommendation", "final_signal",
)
_SECTOR_COL_CANDIDATES = (
    "sector", "gics_sector", "industry_sector",
)
_QUADRANT_COL_CANDIDATES = (
    "quadrant", "rotation_quadrant", "regime", "sector_quadrant",
)


def _find_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _resolve_run_date(
    bench_df: pd.DataFrame | None,
    result: dict[str, Any],
) -> str:
    if bench_df is not None and not bench_df.empty:
        try:
            last = bench_df.index[-1]
            if hasattr(last, "strftime"):
                return last.strftime("%Y-%m-%d")
        except Exception:
            pass

    report = result.get("report_v2", {})
    header = report.get("header", {})
    for key in ("as_of_date", "run_date", "date"):
        val = header.get(key)
        if val:
            return str(val)[:10]

    return date.today().strftime("%Y-%m-%d")


def _extract_action_sets_from_report(
    report: dict[str, Any],
) -> tuple[set[str], set[str]]:
    buy_set: set[str] = set()
    sell_set: set[str] = set()
    actions = report.get("actions", {})

    if isinstance(actions, dict):
        for key in ("buys", "buy_tickers", "buy_list"):
            items = actions.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        t = item.get("ticker") or item.get("symbol")
                        if t:
                            buy_set.add(str(t))
                    elif isinstance(item, str):
                        buy_set.add(item)
        for key in ("sells", "sell_tickers", "sell_list", "reduces", "reduce_tickers"):
            items = actions.get(key, [])
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        t = item.get("ticker") or item.get("symbol")
                        if t:
                            sell_set.add(str(t))
                    elif isinstance(item, str):
                        sell_set.add(item)

    elif isinstance(actions, list):
        for item in actions:
            if isinstance(item, dict):
                t = item.get("ticker") or item.get("symbol")
                a = str(item.get("action", "")).upper()
                if t:
                    if a in _BUY_LABELS:
                        buy_set.add(str(t))
                    elif a in _SELL_LABELS:
                        sell_set.add(str(t))

    return buy_set, sell_set


def _extract_portfolio_set(report: dict[str, Any]) -> set[str]:
    selected: set[str] = set()
    portfolio = report.get("portfolio", {})

    for key in ("holdings", "selected", "selected_names", "positions"):
        items = portfolio.get(key, [])
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    t = item.get("ticker") or item.get("symbol")
                    if t:
                        selected.add(str(t))
                elif isinstance(item, str):
                    selected.add(item)

    return selected


def _emit_v2_signals(
    market: str,
    result: dict[str, Any],
    bench_df: pd.DataFrame | None = None,
) -> None:
    if not _HAS_SIGNAL_WRITER:
        return

    report = result.get("report_v2", {})
    run_date = _resolve_run_date(bench_df, result)

    scored_df: pd.DataFrame | None = None
    for key in _SCORED_TABLE_KEYS:
        candidate = result.get(key)
        if isinstance(candidate, pd.DataFrame) and not candidate.empty:
            scored_df = candidate
            logger.debug("Signal writer: using scored table from result['%s'] (%d rows)", key, len(candidate))
            break

    report_buy_set, report_sell_set = _extract_action_sets_from_report(report)
    portfolio_set = _extract_portfolio_set(report)

    exhaustion_set: set[str] = set()
    exhaustion_scores: dict[str, float] = {}
    exhaustion_table = result.get("selling_exhaustion_table", pd.DataFrame())
    if isinstance(exhaustion_table, pd.DataFrame) and not exhaustion_table.empty:
        ticker_col_ex = _find_col(exhaustion_table, TICKER_CANDIDATE_COLS)
        if ticker_col_ex:
            for _, row in exhaustion_table.iterrows():
                t = str(row[ticker_col_ex])
                exhaustion_set.add(t)
                for scol in ("selling_exhaustion_score", "reversal_trigger_score"):
                    if scol in exhaustion_table.columns and pd.notna(row.get(scol)):
                        exhaustion_scores[t] = round(float(row[scol]), 4)
                        break

    signals: dict[str, dict] = {}

    if scored_df is not None:
        ticker_col = _find_col(scored_df, TICKER_CANDIDATE_COLS)
        score_col = _find_col(scored_df, _SCORE_COL_CANDIDATES)
        signal_col = _find_col(scored_df, _SIGNAL_COL_CANDIDATES)
        sector_col = _find_col(scored_df, _SECTOR_COL_CANDIDATES)
        quadrant_col = _find_col(scored_df, _QUADRANT_COL_CANDIDATES)

        if ticker_col is None:
            logger.warning("Signal writer: scored table has no ticker column — skipping")
        else:
            buy_rank = 0

            for _, row in scored_df.iterrows():
                ticker = str(row[ticker_col])

                score = 0.0
                if score_col and pd.notna(row.get(score_col)):
                    try:
                        score = round(float(row[score_col]), 4)
                    except (TypeError, ValueError):
                        pass

                if signal_col and pd.notna(row.get(signal_col)):
                    raw_signal = str(row[signal_col])
                    action = _normalise_action(raw_signal)
                elif ticker in report_buy_set:
                    raw_signal = "BUY"
                    action = "BUY"
                elif ticker in report_sell_set:
                    raw_signal = "SELL"
                    action = "SELL"
                elif ticker in portfolio_set:
                    raw_signal = "BUY"
                    action = "BUY"
                else:
                    raw_signal = "HOLD"
                    action = "HOLD"

                rank = None
                if action == "BUY":
                    buy_rank += 1
                    rank = buy_rank

                sector = None
                if sector_col and pd.notna(row.get(sector_col)):
                    sector = str(row[sector_col])

                regime = None
                if quadrant_col and pd.notna(row.get(quadrant_col)):
                    regime = str(row[quadrant_col])

                rs_rank = None
                for rs_col in ("rs_rank", "rs_zscore", "relative_strength_rank", "rs_composite"):
                    if rs_col in scored_df.columns and pd.notna(row.get(rs_col)):
                        try:
                            rs_rank = round(float(row[rs_col]), 4)
                        except (TypeError, ValueError):
                            pass
                        break

                notes_parts: list[str] = []
                if raw_signal != action:
                    notes_parts.append(raw_signal)
                if ticker in exhaustion_set:
                    ex_score = exhaustion_scores.get(ticker)
                    if ex_score is not None:
                        notes_parts.append(f"selling_exhaustion={ex_score}")
                    else:
                        notes_parts.append("selling_exhaustion")
                if ticker in report_buy_set and signal_col:
                    notes_parts.append("report_buy")
                if ticker in report_sell_set and signal_col:
                    notes_parts.append("report_sell")

                thematic_map = result.get("thematic_etf_map", {})
                for theme, tickers in thematic_map.items():
                    if ticker in tickers:
                        notes_parts.append(f"theme={theme}")
                        break

                signals[ticker] = {
                    "action":  action,
                    "score":   score,
                    "rank":    rank,
                    "rs_rank": rs_rank,
                    "sector":  sector,
                    "regime":  regime,
                    "notes":   "; ".join(notes_parts) if notes_parts else "",
                }

    if not signals and (report_buy_set or report_sell_set or portfolio_set):
        logger.info("Signal writer: no scored table — building from report actions/portfolio")
        buy_rank = 0
        all_tickers = sorted(report_buy_set | report_sell_set | portfolio_set)

        for ticker in all_tickers:
            if ticker in report_buy_set or ticker in portfolio_set:
                action = "BUY"
                buy_rank += 1
                rank = buy_rank
            elif ticker in report_sell_set:
                action = "SELL"
                rank = None
            else:
                action = "HOLD"
                rank = None

            notes_parts = []
            if ticker in exhaustion_set:
                ex_score = exhaustion_scores.get(ticker)
                if ex_score is not None:
                    notes_parts.append(f"selling_exhaustion={ex_score}")
                else:
                    notes_parts.append("selling_exhaustion")

            signals[ticker] = {
                "action":  action,
                "score":   0.0,
                "rank":    rank,
                "rs_rank": None,
                "sector":  None,
                "regime":  None,
                "notes":   "; ".join(notes_parts) if notes_parts else "",
            }

    if not signals:
        logger.info("Signal writer: no signals to emit for market %s", market)
        return

    header = report.get("header", {})
    rotation_section = report.get("rotation", {})
    portfolio_section = report.get("portfolio", {})

    meta: dict[str, Any] = {
        "processed_names":  header.get("processed_names", 0),
        "candidate_count":  portfolio_section.get("candidate_count", 0),
        "selected_count":   portfolio_section.get("selected_count", 0),
    }

    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        meta["rotation_quadrants"] = {}
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            meta["rotation_quadrants"][q] = {
                "count":   info.get("count", 0),
                "sectors": info.get("sectors", []),
            }

    thematic_rotation = report.get("thematic_rotation", {})
    if thematic_rotation.get("available"):
        tqc = thematic_rotation.get("quadrant_counts", {})
        meta["thematic_rotation_quadrants"] = {}
        for q in ("leading", "improving", "weakening", "lagging"):
            info = tqc.get(q, {})
            meta["thematic_rotation_quadrants"][q] = {
                "count":   info.get("count", 0),
                "themes":  info.get("themes", []),
            }

    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        meta["portfolio_rotation_exposure"] = {
            e.get("quadrant", "?"): round(e.get("weight_pct", 0), 2)
            for e in rot_exp
        }

    if exhaustion_set:
        meta["selling_exhaustion_count"] = len(exhaustion_set)

    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        meta["skipped_count"] = len(skipped_table)

    path = _write_signals(
        phase="phase1",
        market=market.upper(),
        run_date=run_date,
        signals=signals,
        model_name="V2 Pipeline (refactor)",
        meta=meta,
    )
    logger.info("V2 signals written → %s  (%d tickers)", path, len(signals))


# ═══════════════════════════════════════════════════════════════
#  SECTOR ROTATION DISPLAY (Blended RRG + ETF Composite)
# ═══════════════════════════════════════════════════════════════

def _display_sector_rotation_v2(result: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    sector_summary: pd.DataFrame = result.get("sector_summary", pd.DataFrame())

    if isinstance(sector_summary, pd.DataFrame) and not sector_summary.empty:
        lines.append("")
        lines.extend(_box_header("📊", "SECTOR ROTATION — Blended RRG + ETF Composite"))
        lines.append("")

        regime_groups: dict[str, list[str]] = {}
        for _, row in sector_summary.iterrows():
            r = str(row.get("regime", "unknown"))
            regime_groups.setdefault(r, []).append(str(row.get("sector", "?")))

        for regime_name in ("leading", "improving", "weakening", "lagging"):
            members = regime_groups.get(regime_name, [])
            icon = _REGIME_ICON.get(regime_name, "⚪")
            label = regime_name.upper()
            lines.append(
                f"  {icon} {label:<14}({len(members):>2})  "
                + (", ".join(members) if members else "—")
            )

        lines.append("")

        has_blended    = "blended_score"   in sector_summary.columns
        has_rs_level   = "rs_level"        in sector_summary.columns
        has_rs_mom     = "rs_mom"          in sector_summary.columns
        has_etf_comp   = "etf_composite"   in sector_summary.columns
        has_theme_avg  = "theme_avg_score" in sector_summary.columns
        has_excess     = "excess_20d"      in sector_summary.columns
        has_rrg_quad   = "rrg_quadrant"    in sector_summary.columns

        hdr = f"  {'#':>4}  {'Sector':<25} {'ETF':<6}  {'Regime':<16}"
        if has_blended:
            hdr += f"  {'Blended':>22}"
        if has_rs_level:
            hdr += f"  {'RS Lvl':>9}"
        if has_rs_mom:
            hdr += f"  {'RS Mom':>9}"
        if has_etf_comp:
            hdr += f"  {'ETF Scr':>8}"
        if has_theme_avg:
            hdr += f"  {'Thm Avg':>8}"
        if has_excess:
            hdr += f"  {'Excess20d':>10}"
        if has_rrg_quad:
            hdr += f"  {'RRG Quad':>16}"

        lines.append(hdr)
        lines.append("  " + "━" * (len(hdr)))

        max_blended = 0.80
        if has_blended and not sector_summary["blended_score"].empty:
            max_blended = max(0.80, sector_summary["blended_score"].max() * 1.15)

        for i, (_, row) in enumerate(sector_summary.iterrows(), 1):
            sector_name = str(row.get("sector", "?"))
            etf_ticker  = str(row.get("etf", "?"))
            regime      = str(row.get("regime", "?"))
            icon        = _REGIME_ICON.get(regime, "⚪")

            rl = f"  {i:>4}  {sector_name:<25} {etf_ticker:<6}  {icon} {regime:<12}"

            if has_blended:
                bv = float(row.get("blended_score", 0))
                bar = _hbar(bv, max_blended, width=12)
                rl += f"  {bar} {bv:.4f}"
            if has_rs_level:
                rl += f"  {_signed(float(row.get('rs_level', 0)), width=9)}"
            if has_rs_mom:
                rl += f"  {_signed(float(row.get('rs_mom', 0)), width=9)}"
            if has_etf_comp:
                rl += f"  {float(row.get('etf_composite', 0)):>8.4f}"
            if has_theme_avg:
                rl += f"  {float(row.get('theme_avg_score', 0)):>8.4f}"
            if has_excess:
                rl += f"  {_signed(float(row.get('excess_20d', 0)), width=10, pct=True)}"
            if has_rrg_quad:
                rq = str(row.get("rrg_quadrant", ""))
                rq_icon = _REGIME_ICON.get(rq, "⚪")
                rl += f"  {rq_icon} {rq:<12}"

            lines.append(rl)

        lines.append("")
        return lines

    report = result.get("report_v2", {})
    rotation_section = report.get("rotation", {})
    if not rotation_section.get("available"):
        lines.append("")
        lines.extend(_box_header("📊", "SECTOR ROTATION"))
        lines.append("  (not available)")
        return lines

    lines.append("")
    lines.extend(_box_header("📊", "SECTOR ROTATION — RRG Quadrants (Legacy)"))
    lines.append("")

    qc = rotation_section.get("quadrant_counts", {})
    for regime_name in ("leading", "improving", "weakening", "lagging"):
        info = qc.get(regime_name, {})
        count = info.get("count", 0)
        sectors = info.get("sectors", [])
        icon = _REGIME_ICON.get(regime_name, "⚪")
        label = regime_name.upper()
        lines.append(
            f"  {icon} {label:<14}({count:>2})  "
            + (", ".join(sectors) if sectors else "—")
        )

    sector_detail = rotation_section.get("sector_detail", [])
    if sector_detail:
        lines.append("")
        hdr = (
            f"  {'#':>4}  {'Sector':<25} {'ETF':<6}  {'Regime':<16}"
            f"  {'RS Level':>10}  {'RS Mom':>8}  {'Excess 20d':>10}"
        )
        lines.append(hdr)
        lines.append("  " + "━" * len(hdr))
        for i, sd in enumerate(sector_detail, 1):
            regime = sd.get("regime", "?")
            icon = _REGIME_ICON.get(regime, "⚪")
            lines.append(
                f"  {i:>4}  {sd.get('sector', '?'):<25} "
                f"{sd.get('etf', '?'):<6}  {icon} {regime:<12}"
                f"  {sd.get('rs_level', 0):>10.4f}"
                f"  {sd.get('rs_mom', 0):>8.4f}"
                f"  {_signed(sd.get('excess_20d', 0), width=10, pct=True)}"
            )

    lines.append("")
    return lines


# ═══════════════════════════════════════════════════════════════
#  ETF UNIVERSE RANKING DISPLAY
# ═══════════════════════════════════════════════════════════════

def _display_etf_ranking(result: dict[str, Any], max_rows: int = 40) -> list[str]:
    lines: list[str] = []
    etf_ranking: pd.DataFrame = result.get("etf_ranking", pd.DataFrame())

    if not isinstance(etf_ranking, pd.DataFrame) or etf_ranking.empty:
        lines.append("")
        lines.extend(_box_header("📈", "ETF UNIVERSE RANKING"))
        lines.append("  (not available — rotation_v2 ETF scoring not yet active)")
        lines.append("")
        return lines

    lines.append("")
    lines.extend(_box_header("📈", "ETF UNIVERSE RANKING — by Composite Score"))
    lines.append("")

    comp_col = "etf_composite"
    n_total = len(etf_ranking)
    mean_sc = etf_ranking[comp_col].mean() if comp_col in etf_ranking.columns else 0.0
    top_ticker = str(etf_ranking.iloc[0].get("ticker", "?")) if n_total > 0 else "?"
    top_score = float(etf_ranking.iloc[0].get(comp_col, 0)) if n_total > 0 else 0.0
    bot_ticker = str(etf_ranking.iloc[-1].get("ticker", "?")) if n_total > 0 else "?"
    bot_score = float(etf_ranking.iloc[-1].get(comp_col, 0)) if n_total > 0 else 0.0

    lines.append(
        f"  ETFs scored: {n_total}     "
        f"Mean: {mean_sc:.3f}     "
        f"Top: {top_ticker} ({top_score:.3f})     "
        f"Bottom: {bot_ticker} ({bot_score:.3f})"
    )
    lines.append("")

    _CHECK_COLS: dict[str, str] = {
        "sub_trend":          "Trend",
        "sub_participation":  "Part",
        "rsi14":              "RSI",
        "adx14":              "ADX",
        "relativevolume":     "RVOL",
    }
    stale_internal = _detect_stale_columns(etf_ranking, list(_CHECK_COLS.keys()))
    stale_labels: set[str] = {_CHECK_COLS[c] for c in stale_internal}

    if stale_internal:
        stale_vals_parts: list[str] = []
        for col in stale_internal:
            if col in etf_ranking.columns:
                mode_series = etf_ranking[col].mode()
                mode_val = mode_series.iloc[0] if not mode_series.empty else "?"
                if isinstance(mode_val, float):
                    mode_val = f"{mode_val:.3f}" if mode_val < 10 else f"{mode_val:.1f}"
                stale_vals_parts.append(f"{_CHECK_COLS[col]}={mode_val}")

        lines.append("  ⚠️  DATA QUALITY WARNING")
        lines.append(
            f"  │  Columns marked † show identical values across all ETFs "
            f"({', '.join(stale_vals_parts)})."
        )
        lines.append(
            "  │  Composite score is effectively driven by the remaining "
            "varying columns only."
        )
        lines.append(
            "  │  Check the upstream scoring pipeline for these indicators."
        )
        lines.append("  └" + "─" * 95)
        lines.append("")

    has_theme       = "theme"              in etf_ranking.columns
    has_sector      = "parent_sector"      in etf_ranking.columns
    has_composite   = comp_col             in etf_ranking.columns
    has_trend       = "sub_trend"          in etf_ranking.columns
    has_momentum    = "sub_momentum"       in etf_ranking.columns
    has_part        = "sub_participation"  in etf_ranking.columns
    has_rsi         = "rsi14"              in etf_ranking.columns
    has_adx         = "adx14"              in etf_ranking.columns
    has_rvol        = "relativevolume"     in etf_ranking.columns
    has_ret20       = "return20d"          in etf_ranking.columns
    has_is_sector   = "is_sector_etf"      in etf_ranking.columns
    has_is_broad    = "is_broad"           in etf_ranking.columns
    has_is_regional = "is_regional"        in etf_ranking.columns

    def _clbl(display_name: str, internal_col: str) -> str:
        return display_name + "†" if internal_col in stale_internal else display_name

    hdr = f"  {'#':>4}  {'Ticker':<9}"
    if has_theme:
        hdr += f" {'Theme':<22}"
    if has_sector:
        hdr += f" {'Sector':<20}"
    if has_composite:
        hdr += f"  {'Score':>18}"
    if has_momentum:
        hdr += f"  {_clbl('Mom', 'sub_momentum'):>6}"
    if has_trend:
        hdr += f"  {_clbl('Trend', 'sub_trend'):>7}"
    if has_part:
        hdr += f"  {_clbl('Part', 'sub_participation'):>6}"
    if has_rsi:
        hdr += f"  {_clbl('RSI', 'rsi14'):>6}"
    if has_adx:
        hdr += f"  {_clbl('ADX', 'adx14'):>6}"
    if has_rvol:
        hdr += f"  {_clbl('RVOL', 'relativevolume'):>6}"
    if has_ret20:
        hdr += f"  {'Ret20d':>8}"

    lines.append(hdr)
    lines.append("  " + "━" * len(hdr))

    display_df = etf_ranking.copy()
    if has_is_regional:
        display_df = display_df[~display_df["is_regional"]].copy()
    display_df = display_df.head(max_rows)

    max_score = 0.60
    if has_composite and not display_df.empty:
        max_score = max(0.60, display_df[comp_col].max() * 1.10)

    for i, (_, row) in enumerate(display_df.iterrows(), 1):
        ticker = str(row.get("ticker", "?"))

        marker = " "
        if has_is_sector and row.get("is_sector_etf"):
            marker = "●"
        elif has_is_broad and row.get("is_broad"):
            marker = "○"

        rl = f"  {i:>4}  {ticker:<6}{marker} "

        if has_theme:
            theme = str(row.get("theme", ""))[:21]
            rl += f" {theme:<22}"
        if has_sector:
            sector = str(row.get("parent_sector", ""))[:19]
            rl += f" {sector:<20}"
        if has_composite:
            sv = float(row.get(comp_col, 0))
            bar = _hbar(sv, max_score, width=10)
            rl += f"  {bar} {sv:.3f}"
        if has_momentum:
            rl += f"  {float(row.get('sub_momentum', 0)):>6.3f}"
        if has_trend:
            rl += f"  {float(row.get('sub_trend', 0)):>7.3f}"
        if has_part:
            rl += f"  {float(row.get('sub_participation', 0)):>6.3f}"
        if has_rsi:
            rl += f"  {float(row.get('rsi14', 50)):>6.1f}"
        if has_adx:
            rl += f"  {float(row.get('adx14', 15)):>6.1f}"
        if has_rvol:
            rl += f"  {float(row.get('relativevolume', 1)):>6.2f}"
        if has_ret20:
            ret = float(row.get("return20d", 0))
            rl += f"  {ret:>+8.1%}"

        lines.append(rl)

    lines.append("")
    legend_parts = ["  (● = sector ETF   ○ = broad market)"]
    if stale_labels:
        legend_parts.append(
            f"  († = stale data — identical across all ETFs: "
            f"{', '.join(sorted(stale_labels))})"
        )
    lines.extend(legend_parts)
    lines.append("")

    return lines


# ═══════════════════════════════════════════════════════════════
#  STRATEGY RUNNER
# ═══════════════════════════════════════════════════════════════


def run_strategy_v2(
    market: str,
    universe_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    breadth_df: pd.DataFrame | None = None,
    config: dict | None = None,
) -> dict[str, Any]:
    cfg = get_market_config_v2(market)
    tradable = set(cfg["tradable_universe"])
    leadership = set(cfg["leadership_universe"])
    tradable_frames = {k: v for k, v in universe_frames.items() if k in tradable}
    leadership_frames = {k: v for k, v in universe_frames.items() if k in leadership}
    logger.info(
        "Universe selection: configured_tradable=%d configured_leadership=%d matched_tradable=%d matched_leadership=%d",
        len(tradable), len(leadership), len(tradable_frames), len(leadership_frames),
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Matched tradable tickers (first 100): %s", sorted(tradable_frames.keys())[:100])
        logger.debug("Matched leadership tickers (first 100): %s", sorted(leadership_frames.keys())[:100])
    missing_tradable = sorted(tradable - set(tradable_frames.keys()))
    missing_leadership = sorted(leadership - set(leadership_frames.keys()))
    if missing_tradable:
        logger.info("Missing tradable symbols in parquet: count=%d sample=%s", len(missing_tradable), missing_tradable[:25])
    if missing_leadership:
        logger.info("Missing leadership symbols in parquet: count=%d sample=%s", len(missing_leadership), missing_leadership[:25])

    thematic_frames, available_thematic_map = _extract_thematic_frames(
        universe_frames, market,
    )

    portfolio_params = {
        "max_positions": cfg.get("max_positions", 8),
        "max_sector_weight": cfg.get("max_sector_weight", 0.35),
        "max_theme_names": cfg.get("max_theme_names", 2),
        "max_single_weight": cfg.get("max_single_weight", 0.20),
        "min_weight": cfg.get("min_weight", 0.04),
    }

    effective_config = dict(config or {})
    effective_config["thematic_etf_frames"] = thematic_frames
    effective_config["thematic_etf_map"] = available_thematic_map

    result = run_pipeline_v2(
        tradable_frames=tradable_frames,
        leadership_frames=leadership_frames,
        bench_df=bench_df,
        breadth_df=breadth_df,
        market=cfg["market"],
        portfolio_params=portfolio_params,
        config=effective_config,
    )
    result["market_config_v2"] = cfg
    result["tradable_universe"] = sorted(tradable)
    result["leadership_universe"] = sorted(leadership)

    result["thematic_etf_frames"] = thematic_frames
    result["thematic_etf_map"] = available_thematic_map

    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        sample_cols = [c for c in ["ticker", "missing_critical_fields_v2"] if c in skipped_table.columns]
        logger.info(
            "Skipped symbols due to scoreability gate: count=%d sample=%s",
            len(skipped_table),
            skipped_table[sample_cols].head(20).to_dict(orient="records") if sample_cols else skipped_table.head(20).to_dict(orient="records"),
        )

    exhaustion_table = result.get("selling_exhaustion_table", pd.DataFrame())
    if isinstance(exhaustion_table, pd.DataFrame) and not exhaustion_table.empty:
        logger.info(
            "Selling exhaustion candidates: count=%d",
            len(exhaustion_table),
        )
        if "status" in exhaustion_table.columns:
            status_counts = exhaustion_table["status"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion status breakdown: %s", status_counts,
            )
        if "quality_label" in exhaustion_table.columns:
            quality_counts = exhaustion_table["quality_label"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion quality breakdown: %s", quality_counts,
            )
        preview_cols = [
            c for c in [
                "ticker", "status", "quality_label",
                "selling_exhaustion_score", "reversal_trigger_score",
                "rsi_14", "price_5d_change", "sector",
            ] if c in exhaustion_table.columns
        ]
        if preview_cols:
            logger.info(
                "Selling exhaustion top candidates:\n%s",
                exhaustion_table[preview_cols].head(10).to_string(index=False),
            )

        if "sector" in exhaustion_table.columns:
            sector_counts = exhaustion_table["sector"].value_counts(dropna=False).to_dict()
            logger.info(
                "Selling exhaustion by sector: %s", sector_counts,
            )
    else:
        logger.info("Selling exhaustion: no candidates detected")

    report = build_report_v2(result)
    result["report_v2"] = report
    result["report_text_v2"] = to_text_v2(report)

    _log_portfolio_selection_gaps(result)

    _emit_v2_signals(market, result, bench_df)

    header = report.get("header", {})
    portfolio_section = report.get("portfolio", {})
    action_summary = report.get("actions", {})
    rotation_section = report.get("rotation", {})
    exhaustion_section = report.get("selling_exhaustion", {})

    logger.info(
        "Strategy result summary: "
        "processed=%s candidates=%s selected=%s "
        "actions=%s exhaustion=%s",
        header.get("processed_names", 0),
        portfolio_section.get("candidate_count", 0),
        portfolio_section.get("selected_count", 0),
        action_summary,
        exhaustion_section.get("count", 0),
    )

    if rotation_section.get("available"):
        qc = rotation_section.get("quadrant_counts", {})
        parts = []
        for q in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(q, {})
            count = info.get("count", 0)
            sectors = info.get("sectors", [])
            parts.append(f"{q}={count}({','.join(sectors[:3])})")
        logger.info("Sector rotation: %s", "  ".join(parts))

    thematic_rotation = report.get("thematic_rotation", {})
    if thematic_rotation.get("available"):
        tqc = thematic_rotation.get("quadrant_counts", {})
        parts = []
        for q in ("leading", "improving", "weakening", "lagging"):
            info = tqc.get(q, {})
            count = info.get("count", 0)
            themes = info.get("themes", info.get("sectors", []))
            parts.append(f"{q}={count}({','.join(themes[:4])})")
        logger.info("Thematic rotation: %s", "  ".join(parts))
    elif available_thematic_map:
        logger.info(
            "Thematic rotation: not yet computed by pipeline "
            "(available themes: %s — update pipeline_v2 to consume "
            "config['thematic_etf_frames'])",
            list(available_thematic_map.keys()),
        )

    rot_exp = portfolio_section.get("rotation_exposure", [])
    if rot_exp:
        exp_parts = [
            f"{e.get('quadrant', '?')}={e.get('weight_pct', 0):.1f}%"
            for e in rot_exp
        ]
        logger.info("Portfolio rotation exposure: %s", "  ".join(exp_parts))

    rotation_lines = _display_sector_rotation_v2(result)
    etf_lines = _display_etf_ranking(result, max_rows=40)

    all_display_lines = rotation_lines + etf_lines
    if all_display_lines:
        display_block = "\n".join(all_display_lines)
        logger.info("Sector Rotation & ETF Ranking:\n%s", display_block)
        result["sector_rotation_display"] = display_block

    return result


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

def main(argv=None):
    args = build_parser().parse_args(argv)
    log_file = setup_logging(args.verbose)
    try:
        logger.info("Starting runner_v2")
        logger.info("Log file: %s", log_file)
        logger.info("Market: %s", args.market)
        logger.info("Start date: %s", args.start_date)
        logger.info("End date: %s", args.end_date)
        logger.info("Verbose logging: %s", args.verbose)

        if _HAS_SIGNAL_WRITER:
            logger.info("Signal writer: enabled → results/signals/")
        else:
            logger.info("Signal writer: not available (install signal_writer.py for combined reports)")

        if _HAS_HTML_REPORT:
            logger.info("HTML report: enabled")
        else:
            logger.info("HTML report: not available (install utils/html_report.py)")

        if _HAS_EMAIL and not args.no_email:
            logger.info("Email report: enabled")
        elif args.no_email:
            logger.info("Email report: disabled via --no-email")
        else:
            logger.info("Email report: not available (install utils/email_report.py)")

        universe_frames, bench_df, breadth_df = load_market_data_v2(
            market=args.market,
            start_date=args.start_date,
            end_date=args.end_date,
            parquet_path=args.parquet_path,
        )

        result = run_strategy_v2(
            market=args.market,
            universe_frames=universe_frames,
            bench_df=bench_df,
            breadth_df=breadth_df,
        )

        # ── Console summary (existing) ────────────────────────────────
        run_log = RunLogger(f"runner_v2_{args.market}")
        print_run_summary(result, args.market, run_log)

        # ── HTML Report: build, save, print link ──────────────────────
        html_path = None
        html_content = None
        if _HAS_HTML_REPORT:
            try:
                html_content = build_html_report(result, args.market)
                html_path = save_html_report(html_content, args.market)
                logger.info("HTML report: %s", html_path)

                # Print a clickable file-URL
                try:
                    file_url = html_path.as_uri()
                except Exception:
                    file_url = f"file:///{html_path.as_posix()}"

                print(f"\n{'─' * 72}")
                print(f"📊  HTML Report saved → {html_path}")
                print(f"    Open in browser:    {file_url}")
            except Exception as exc:
                logger.error("HTML report generation failed: %s", exc)
        else:
            print(f"\n{'─' * 72}")
            print("📊  HTML report: skipped (utils/html_report.py not found)")

        # ── Email Report ──────────────────────────────────────────────
        if _HAS_EMAIL and not args.no_email and html_content:
            try:
                run_date = _resolve_run_date(bench_df, result)
                subject = (
                    f"Smart Money Rotation — {args.market.upper()} — {run_date}"
                )
                ok = send_report_email(
                    html_content=html_content,
                    subject=subject,
                    html_path=html_path,
                )
                if ok:
                    print(f"📧  Report emailed successfully")
                else:
                    print(f"📧  Email not sent (check common/credential.py)")
            except Exception as exc:
                logger.error("Email sending failed: %s", exc)
                print(f"📧  Email failed: {exc}")
        elif args.no_email:
            print(f"📧  Email skipped (--no-email)")
        else:
            print(f"📧  Email not available (install utils/email_report.py + common/credential.py)")

        print(f"{'─' * 72}\n")

        logger.info("runner_v2 completed")

        if args.print_report and result.get("report_text_v2"):
            print(result["report_text_v2"])

        return result

    except Exception as exc:
        logger.exception("runner_v2 failed: %s", exc)
        raise


if __name__ == "__main__":
    main()

#################################
"""
refactor/report_v2.py

Report builder for the v2 pipeline.

Consumes the dict returned by ``run_pipeline_v2`` and produces:

    1.  A structured ``dict`` (``build_report_v2``) suitable for JSON
        serialisation, dashboards, or downstream programmatic use.
    2.  A plain-text rendering (``to_text_v2``) for logging, email, or
        terminal display.

The report is organised into sections:

    header              – market, universe sizes, processing counts.
    regime              – breadth, volatility, target exposure.
    rotation            – sector rotation heatmap and quadrant summary.
    scoring             – sub-score distribution statistics.
    actions             – action count breakdown.
    portfolio           – selected names, weights, sector tilt,
                          rotation exposure.
    review              – top-ranked review table rows.
    selling_exhaustion  – reversal watch candidates.
    skipped             – names excluded from scoring and why.
"""
from __future__ import annotations

import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def _safe_str(value, default: str = "unknown") -> str:
    if value is None:
        return default
    s = str(value).strip()
    return default if s.lower() in ("", "nan", "none") else s


def _df_shape(df: pd.DataFrame | None) -> tuple[int, int]:
    if df is None:
        return (0, 0)
    return tuple(df.shape)


def _df_cols(df: pd.DataFrame | None) -> list[str]:
    if df is None:
        return []
    return list(df.columns)


def _preview_df(
    df: pd.DataFrame | None,
    cols: list[str],
    n: int = 10,
) -> list[dict]:
    if df is None or df.empty:
        return []
    use = [c for c in cols if c in df.columns]
    if not use:
        return []
    return df[use].head(n).to_dict(orient="records")


def _count_actions(actions: pd.DataFrame | None) -> dict[str, int]:
    summary = {"STRONG_BUY": 0, "BUY": 0, "HOLD": 0, "SELL": 0}
    if actions is None or actions.empty or "action_v2" not in actions.columns:
        return summary
    counts = actions["action_v2"].value_counts(dropna=False).to_dict()
    for k in summary:
        summary[k] = int(counts.get(k, 0))
    return summary


# ── Column-name resolution helper ────────────────────────────────────────
# rotation_v2 uses short names (rs_mom, etf_composite, …) while earlier
# report code assumed longer names (rs_momentum, excess_return_20d, …).
# _resolve_col tries a prioritised list and returns the first match.

def _resolve_col(row_or_dict, *candidates, default=""):
    """Return the value for the first key in *candidates* that exists
    and is not None / NaN.  Works with dicts and pd.Series."""
    for key in candidates:
        val = None
        if isinstance(row_or_dict, dict):
            val = row_or_dict.get(key)
        else:
            val = getattr(row_or_dict, key, None)
        if val is not None:
            try:
                if isinstance(val, float) and math.isnan(val):
                    continue
            except (TypeError, ValueError):
                pass
            return val
    return default


# ═══════════════════════════════════════════════════════════════════════════════
# Section builders
# ═══════════════════════════════════════════════════════════════════════════════

def _build_header(result: dict, latest: pd.DataFrame) -> dict:
    market = result.get("market", "UNKNOWN")
    leadership = result.get("leadership_snapshot", pd.DataFrame())
    return {
        "market": market,
        "tradable_universe_size": len(result.get("scored", pd.DataFrame())),
        "leadership_universe_size": len(leadership) if leadership is not None else 0,
        "processed_names": 0 if latest is None else len(latest),
        "skipped_names": len(result.get("skipped_table", pd.DataFrame())),
        "rsi_field": "rsi14",
    }


def _build_regime_section(meta: dict, breadth_info: dict | None) -> dict:
    breadth_info = breadth_info or {}
    return {
        "breadth_regime": _safe_str(meta.get("breadth_regime", breadth_info.get("breadth_regime"))),
        "breadth_score": _safe_float(breadth_info.get("breadthscore")),
        "vol_regime": _safe_str(meta.get("vol_regime")),
        "target_exposure": _safe_float(meta.get("target_exposure", 0.0)),
        "dispersion20": _safe_float(breadth_info.get("dispersion20")),
    }


def _build_rotation_section(result: dict) -> dict:
    """Build sector rotation heatmap and quadrant summary.

    .. note::
       Column names emitted by ``rotation_v2`` are short-form
       (``rs_mom``, ``blended_score``, …).  This function resolves
       both short and legacy long-form names so the heatmap is
       populated regardless of which version of rotation was used.
    """
    sector_summary = result.get("sector_summary", pd.DataFrame())
    sector_regimes = result.get("sector_regimes", {})

    if sector_summary is None or sector_summary.empty:
        return {
            "available": False,
            "heatmap": [],
            "quadrant_counts": {},
        }

    # ── Columns we want in the heatmap (short-form preferred) ─────────
    # We list both the canonical short name AND legacy aliases so that
    # the intersection with actual columns picks up whatever is present.
    display_cols = [
        "sector", "etf", "regime",
        "rs_rank", "momentum_rank",
        "rs_level",
        "rs_mom", "rs_momentum",                         # ← CHANGED: added short-form
        "blended_score",                                  # ← ADDED
        "rrg_quadrant",                                   # ← ADDED
        "etf_composite",                                  # ← ADDED
        "theme_avg_score",                                # ← ADDED
        "excess_20d", "excess_return_20d",                # ← CHANGED: try both names
        "excess_ret_20d", "ret_vs_bench_20d",             # ← ADDED extra fallbacks
    ]
    cols = [c for c in display_cols if c in sector_summary.columns]

    # Sort by blended_score (descending) or rs_rank (ascending) ────────
    if "blended_score" in sector_summary.columns:                       # ← CHANGED
        sorted_df = sector_summary.sort_values(
            "blended_score", ascending=False,
        )
    elif "rs_rank" in sector_summary.columns:
        sorted_df = sector_summary.sort_values("rs_rank")
    else:
        sorted_df = sector_summary

    heatmap = []
    for _, row in sorted_df.iterrows():
        entry = {}
        for c in cols:
            val = row.get(c)
            if isinstance(val, float) and not math.isnan(val):
                entry[c] = (
                    round(val, 4)
                    if c not in ("rs_rank", "momentum_rank")
                    else int(val)
                )
            elif isinstance(val, (int,)):
                entry[c] = int(val)
            else:
                entry[c] = _safe_str(val, "")
        heatmap.append(entry)

    # Quadrant summary
    quadrant_counts = {}
    if "regime" in sector_summary.columns:
        for regime in ("leading", "improving", "weakening", "lagging"):
            names = sector_summary.loc[
                sector_summary["regime"] == regime, "sector"
            ].tolist()
            quadrant_counts[regime] = {
                "count": len(names),
                "sectors": names,
            }

    return {
        "available": True,
        "heatmap": heatmap,
        "quadrant_counts": quadrant_counts,
    }


def _build_scoring_section(scored: pd.DataFrame) -> dict:
    """Sub-score distribution statistics."""
    if scored is None or scored.empty:
        return {"available": False}

    sub_scores = [
        "scoretrend", "scoreparticipation", "scorerisk",
        "scoreregime", "scorerotation", "scorepenalty",
        "scorecomposite_v2",
    ]
    stats = {}
    for col in sub_scores:
        if col not in scored.columns:
            continue
        s = pd.to_numeric(scored[col], errors="coerce").dropna()
        if s.empty:
            continue
        stats[col] = {
            "mean": round(float(s.mean()), 4),
            "median": round(float(s.median()), 4),
            "min": round(float(s.min()), 4),
            "max": round(float(s.max()), 4),
            "std": round(float(s.std()), 4),
        }

    # Composite distribution buckets
    comp = pd.to_numeric(
        scored.get("scorecomposite_v2", pd.Series(dtype=float)),
        errors="coerce",
    ).dropna()
    buckets = {}
    if not comp.empty:
        buckets = {
            ">=0.75": int((comp >= 0.75).sum()),
            "0.62-0.75": int(((comp >= 0.62) & (comp < 0.75)).sum()),
            "0.50-0.62": int(((comp >= 0.50) & (comp < 0.62)).sum()),
            "<0.50": int((comp < 0.50).sum()),
        }

    return {
        "available": True,
        "sub_scores": stats,
        "composite_buckets": buckets,
    }


def _build_portfolio_section(
    portfolio: dict,
    review: pd.DataFrame,
) -> dict:
    selected = portfolio.get("selected", pd.DataFrame())
    meta = portfolio.get("meta", {}) or {}

    # Selected preview
    selected_preview = []
    if selected is not None and not selected.empty:
        keep = [
            c for c in [
                "selection_rank", "ticker", "portfolio_weight_pct",
                "action_v2", "conviction_v2",
                "scoreadjusted_v2", "scorecomposite_v2",
                "sector", "sectrsregime", "theme",
                "rsi14", "adx14", "relativevolume",
                "effective_sector_cap", "selection_reason",
            ] if c in selected.columns
        ]
        selected_preview = selected[keep].head(15).to_dict(orient="records")

    # Top picks from review table
    top_picks = []
    if review is not None and not review.empty:
        review_keep = [
            c for c in [
                "ticker", "recommendation", "composite_score",
                "score_percentile", "rsi_14", "adx_14",
                "relative_volume", "price_vs_ema30_pct",
                "leadership_strength", "overextended_flag",
                "sector", "sectrsregime", "theme",
                "conviction_v2", "why_this_name",
            ] if c in review.columns
        ]
        top_picks = review[review_keep].head(15).to_dict(orient="records")

    return {
        "selected_count": int(meta.get("selected_count", 0)),
        "candidate_count": int(meta.get("candidate_count", 0)),
        "actual_exposure": _safe_float(meta.get("actual_exposure", 0.0)),
        "cash_reserve": _safe_float(meta.get("cash_reserve", 1.0)),
        "sector_tilt": meta.get("sector_tilt", []),
        "rotation_exposure": meta.get("rotation_exposure", []),
        "top_picks": top_picks,
        "selected_preview": selected_preview,
    }


def _build_exhaustion_section(result: dict) -> dict:
    df = result.get("selling_exhaustion_table", pd.DataFrame())
    if df is None or df.empty:
        return {"available": False, "count": 0, "names": []}

    keep = [
        c for c in [
            "ticker", "status", "quality_label",
            "selling_exhaustion_score", "reversal_trigger_score",
            "rsi_14", "rsi_turn_up_1d", "bullish_close_1d",
            "close_above_prior_high", "volume_reexpansion_1d",
            "relative_volume", "adx_14", "price_5d_change",
            "sector", "theme", "decision_hint",
        ] if c in df.columns
    ]
    names = df[keep].head(15).to_dict(orient="records")

    return {
        "available": True,
        "count": len(df),
        "names": names,
    }


def _build_skipped_section(result: dict) -> dict:
    df = result.get("skipped_table", pd.DataFrame())
    if df is None or df.empty:
        return {"count": 0, "names": []}

    keep = [
        c for c in [
            "ticker", "instrument_type", "status_v2",
            "missing_critical_count_v2", "missing_critical_fields_v2",
            "scoreability_reason_v2", "sector", "sectrsregime",
        ] if c in df.columns
    ]
    names = df[keep].head(20).to_dict(orient="records")
    return {"count": len(df), "names": names}


# ═══════════════════════════════════════════════════════════════════════════════
# Logging helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _log_report_inputs(
    result: dict,
    portfolio: dict,
    selected: pd.DataFrame,
    actions: pd.DataFrame,
    review: pd.DataFrame,
    latest: pd.DataFrame,
) -> None:
    logger.info(
        "build_report_v2 input shapes: selected=%s actions=%s "
        "review=%s latest=%s",
        _df_shape(selected),
        _df_shape(actions),
        _df_shape(review),
        _df_shape(latest),
    )
    logger.info(
        "build_report_v2 keys: portfolio_keys=%s result_keys=%s",
        sorted(list(portfolio.keys())) if isinstance(portfolio, dict) else [],
        sorted(list(result.keys())) if isinstance(result, dict) else [],
    )


def _log_portfolio_meta(meta: dict, market: str) -> None:
    logger.info(
        "build_report_v2 market=%s selected_count=%s "
        "candidate_count=%s target_exposure=%s",
        market,
        meta.get("selected_count", 0),
        meta.get("candidate_count", 0),
        meta.get("target_exposure", 0.0),
    )
    logger.info(
        "build_report_v2 regime meta: breadth_regime=%s vol_regime=%s",
        meta.get("breadth_regime", "unknown"),
        meta.get("vol_regime", "unknown"),
    )


def _log_review_summary(review: pd.DataFrame | None) -> None:
    if review is None or review.empty:
        logger.warning("build_report_v2: review table is empty")
        return
    logger.info(
        "build_report_v2 review summary: rows=%d cols=%d",
        len(review), len(review.columns),
    )
    if "recommendation" in review.columns:
        logger.info(
            "build_report_v2 review recommendations=%s",
            review["recommendation"].value_counts(dropna=False).to_dict(),
        )


def _log_selected_summary(selected: pd.DataFrame | None) -> None:
    if selected is None or selected.empty:
        logger.warning("build_report_v2: selected portfolio is empty")
        return
    logger.info(
        "build_report_v2 selected summary: rows=%d cols=%d",
        len(selected), len(selected.columns),
    )
    if "sector" in selected.columns:
        logger.info(
            "build_report_v2 selected sector=%s",
            selected["sector"].value_counts(dropna=False).to_dict(),
        )
    if "sectrsregime" in selected.columns:
        logger.info(
            "build_report_v2 selected sectrsregime=%s",
            selected["sectrsregime"].value_counts(dropna=False).to_dict(),
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def build_report_v2(result: dict) -> dict:
    """
    Build a structured report from the pipeline output dict.

    Parameters
    ----------
    result : dict
        Output of ``run_pipeline_v2``.

    Returns
    -------
    dict
        Structured report with sections: header, regime, rotation,
        scoring, actions, portfolio, selling_exhaustion, skipped.
    """
    portfolio = result.get("portfolio", {}) or {}
    selected = portfolio.get("selected", pd.DataFrame())
    actions = result.get("action_table", pd.DataFrame())
    review = result.get("review_table", pd.DataFrame())
    meta = portfolio.get("meta", {}) or {}
    latest = result.get("latest", pd.DataFrame())
    scored = result.get("scored", pd.DataFrame())
    breadth_info = result.get("breadth_info", {}) or {}
    market = result.get("market", "UNKNOWN")

    _log_report_inputs(result, portfolio, selected, actions, review, latest)
    _log_portfolio_meta(meta, market)
    _log_review_summary(review)
    _log_selected_summary(selected)

    action_summary = _count_actions(actions)
    logger.info("build_report_v2 action summary=%s", action_summary)

    report = {
        "header": _build_header(result, latest),
        "regime": _build_regime_section(meta, breadth_info),
        "rotation": _build_rotation_section(result),
        "scoring": _build_scoring_section(scored),
        "actions": action_summary,
        "portfolio": _build_portfolio_section(portfolio, review),
        "selling_exhaustion": _build_exhaustion_section(result),
        "skipped": _build_skipped_section(result),
    }

    logger.info(
        "build_report_v2 output ready: top_picks=%d selected=%d "
        "exhaustion=%d skipped=%d rotation_heatmap=%d",
        len(report["portfolio"]["top_picks"]),
        len(report["portfolio"]["selected_preview"]),
        report["selling_exhaustion"].get("count", 0),
        report["skipped"].get("count", 0),
        len(report["rotation"].get("heatmap", [])),
    )
    return report


def to_text_v2(report: dict) -> str:
    """
    Render the structured report dict as a human-readable plain-text
    string.
    """
    h = report.get("header", {})
    r = report.get("regime", {})
    rot = report.get("rotation", {})
    sc = report.get("scoring", {})
    a = report.get("actions", {})
    p = report.get("portfolio", {})
    exh = report.get("selling_exhaustion", {})
    skp = report.get("skipped", {})

    sep = "=" * 92
    thin = "-" * 92

    lines = []

    # ── HEADER ────────────────────────────────────────────────────────────────
    lines.append(sep)
    lines.append("  CASH V2 PIPELINE REPORT")
    lines.append(sep)
    lines.append(f"  Market                  : {h.get('market', 'UNKNOWN')}")
    lines.append(f"  Tradable universe       : {h.get('tradable_universe_size', 0)}")
    lines.append(f"  Leadership universe     : {h.get('leadership_universe_size', 0)}")
    lines.append(f"  Processed (scored)      : {h.get('processed_names', 0)}")
    lines.append(f"  Skipped (not scoreable) : {h.get('skipped_names', 0)}")
    lines.append(f"  RSI field               : {h.get('rsi_field', 'rsi14')} (RSI 14)")
    lines.append("")

    # ── REGIME ────────────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  MARKET REGIME")
    lines.append(thin)
    lines.append(f"  Breadth regime          : {r.get('breadth_regime', 'unknown')}")
    lines.append(f"  Breadth score           : {_safe_float(r.get('breadth_score')):.3f}")
    lines.append(f"  Volatility regime       : {r.get('vol_regime', 'unknown')}")
    lines.append(f"  Target exposure         : {_safe_float(r.get('target_exposure')):.1%}")
    lines.append(f"  Dispersion (20d)        : {_safe_float(r.get('dispersion20')):.4f}")
    lines.append("")

    # ── SECTOR ROTATION ───────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SECTOR ROTATION (RRG Quadrants)")
    lines.append(thin)

    if rot.get("available"):
        # Quadrant summary
        qc = rot.get("quadrant_counts", {})
        for quadrant in ("leading", "improving", "weakening", "lagging"):
            info = qc.get(quadrant, {})
            count = info.get("count", 0)
            sectors = ", ".join(info.get("sectors", [])) or "none"
            lines.append(f"  {quadrant.upper():<12s}  ({count})  {sectors}")
        lines.append("")

        # Heatmap table
        heatmap = rot.get("heatmap", [])
        if heatmap:
            lines.append(
                f"  {'Rank':>4s}  {'Sector':<22s}  {'ETF':<5s}  "
                f"{'Regime':<11s}  {'RS Level':>9s}  {'RS Mom':>8s}  "
                f"{'Blended':>8s}  {'Excess 20d':>10s}"       # ← CHANGED: added Blended
            )
            lines.append("  " + "-" * 88)                     # ← CHANGED: wider rule
            for idx, entry in enumerate(heatmap, 1):
                sector  = _resolve_col(entry, "sector", default="")
                etf     = _resolve_col(entry, "etf", default="")
                regime  = _resolve_col(entry, "regime", default="")
                rs_lvl  = _resolve_col(entry, "rs_level", default="")
                # ── CHANGED: resolve rs_mom with fallback to rs_momentum ──
                rs_mom  = _resolve_col(
                    entry, "rs_mom", "rs_momentum", default="",
                )
                blended = _resolve_col(
                    entry, "blended_score", default="",
                )
                # ── CHANGED: resolve excess with multiple fallback names ──
                excess  = _resolve_col(
                    entry,
                    "excess_20d", "excess_return_20d",
                    "excess_ret_20d", "ret_vs_bench_20d",
                    default="",
                )
                lines.append(
                    f"  {idx:>4d}  {str(sector):<22s}  {str(etf):<5s}  "
                    f"{str(regime):<11s}  "
                    f"{_safe_float(rs_lvl):>9.4f}  "
                    f"{_safe_float(rs_mom):>8.4f}  "
                    f"{_safe_float(blended):>8.4f}  "
                    f"{_safe_float(excess):>10.4f}"
                )
    else:
        lines.append("  Sector rotation data not available")
    lines.append("")

    # ── SCORING SUMMARY ───────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SCORING SUMMARY")
    lines.append(thin)

    if sc.get("available"):
        sub = sc.get("sub_scores", {})
        for col in (
            "scoretrend", "scoreparticipation", "scorerisk",
            "scoreregime", "scorerotation", "scorepenalty",
            "scorecomposite_v2",
        ):
            s = sub.get(col)
            if s:
                lines.append(
                    f"  {col:<22s}  mean={s['mean']:.4f}  "
                    f"median={s['median']:.4f}  "
                    f"min={s['min']:.4f}  max={s['max']:.4f}  "
                    f"std={s['std']:.4f}"
                )

        buckets = sc.get("composite_buckets", {})
        if buckets:
            bucket_str = "  ".join(
                f"{label}: {count}" for label, count in buckets.items()
            )
            lines.append(f"  Distribution: {bucket_str}")
    else:
        lines.append("  Scoring data not available")
    lines.append("")

    # ── ACTIONS ───────────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  ACTION SUMMARY")
    lines.append(thin)
    lines.append(
        f"  STRONG_BUY={a.get('STRONG_BUY', 0)}  "
        f"BUY={a.get('BUY', 0)}  "
        f"HOLD={a.get('HOLD', 0)}  "
        f"SELL={a.get('SELL', 0)}"
    )
    lines.append(
        f"  Candidates: {p.get('candidate_count', 0)}  "
        f"Selected: {p.get('selected_count', 0)}  "
        f"Exposure: {_safe_float(p.get('actual_exposure')):.1%}  "
        f"Cash reserve: {_safe_float(p.get('cash_reserve')):.1%}"
    )
    lines.append("")

    # ── REVIEW TABLE ──────────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  REVIEW TABLE (Top Picks)")
    lines.append(thin)

    top = p.get("top_picks", [])
    if not top:
        lines.append("  No signals")
    else:
        for i, row in enumerate(top, 1):
            ticker = str(row.get("ticker", "?"))
            rec = str(row.get("recommendation", "?"))
            score = _safe_float(row.get("composite_score"))
            pct = _safe_float(row.get("score_percentile"))
            rsi = _safe_float(row.get("rsi_14"))
            adx = _safe_float(row.get("adx_14"))
            rv = _safe_float(row.get("relative_volume"))
            lead = _safe_float(row.get("leadership_strength"))
            ext = row.get("overextended_flag", "")
            sect = row.get("sector", "Unknown")
            sr = row.get("sectrsregime", "unknown")
            conv = row.get("conviction_v2", "")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  {rec:<10s}  "
                f"score={score:.3f}  pct={pct:.0%}  "
                f"rsi={rsi:.0f}  adx={adx:.0f}  rv={rv:.2f}  "
                f"lead={lead:.2f}  ext={ext}  "
                f"conv={conv}  {sect}({sr})"
            )
    lines.append("")

    # ── SELECTED PORTFOLIO ────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SELECTED PORTFOLIO")
    lines.append(thin)

    sel = p.get("selected_preview", [])
    if not sel:
        lines.append("  No positions selected")
    else:
        for i, row in enumerate(sel, 1):
            ticker = str(row.get("ticker", "?"))
            weight = _safe_float(row.get("portfolio_weight_pct"))
            score = _safe_float(
                row.get("scoreadjusted_v2", row.get("scorecomposite_v2")),
            )
            action = row.get("action_v2", "?")
            conv = row.get("conviction_v2", "")
            sect = row.get("sector", "Unknown")
            sr = row.get("sectrsregime", "unknown")
            theme = row.get("theme", "Unknown")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  wt={weight:>5.1f}%  "
                f"score={score:.3f}  {action:<10s}  conv={conv}  "
                f"{sect}({sr})  theme={theme}"
            )

        # Sector tilt
        tilt = p.get("sector_tilt", [])
        if tilt:
            lines.append("")
            lines.append("  Sector tilt:")
            for entry in tilt:
                lines.append(
                    f"    {entry.get('sector', '?'):<20s}  "
                    f"regime={entry.get('regime', '?'):<10s}  "
                    f"wt={entry.get('weight_pct', 0):>5.1f}%  "
                    f"cap={entry.get('effective_cap_pct', 0):>5.1f}%  "
                    f"room={entry.get('headroom_pct', 0):>+5.1f}%  "
                    f"names={entry.get('count', 0)}"
                )

        # Rotation exposure
        rot_exp = p.get("rotation_exposure", [])
        if rot_exp:
            lines.append("")
            lines.append("  Rotation quadrant exposure:")
            for entry in rot_exp:
                tickers = ", ".join(entry.get("tickers", []))
                lines.append(
                    f"    {entry.get('quadrant', '?'):<10s}  "
                    f"wt={entry.get('weight_pct', 0):>5.1f}%  "
                    f"names={entry.get('count', 0)}  "
                    f"[{tickers}]"
                )
    lines.append("")

    # ── SELLING EXHAUSTION ────────────────────────────────────────────────────
    lines.append(thin)
    lines.append("  SELLING EXHAUSTION (Reversal Watch)")
    lines.append(thin)

    if exh.get("available"):
        lines.append(f"  Total candidates: {exh.get('count', 0)}")
        for i, row in enumerate(exh.get("names", []), 1):
            ticker = str(row.get("ticker", "?"))
            status = row.get("status", "?")
            quality = row.get("quality_label", "?")
            exh_score = _safe_float(row.get("selling_exhaustion_score"))
            trig = _safe_float(row.get("reversal_trigger_score"))
            rsi_val = _safe_float(row.get("rsi_14"))
            rsi_up = row.get("rsi_turn_up_1d", "")
            bull_close = row.get("bullish_close_1d", "")
            vol_reexp = row.get("volume_reexpansion_1d", "")
            p5d = _safe_float(row.get("price_5d_change"))
            sect = row.get("sector", "Unknown")

            lines.append(
                f"  {i:>2}. {ticker:<10s}  {status:<22s}  "
                f"quality={quality}  exh={exh_score:.0f}  "
                f"trig={trig:.0f}  rsi={rsi_val:.0f}  "
                f"rsi_up={rsi_up}  bull={bull_close}  "
                f"vol_re={vol_reexp}  5d={p5d:+.1%}  {sect}"
            )
    else:
        lines.append("  No selling exhaustion candidates")
    lines.append("")

    # ── SKIPPED NAMES ─────────────────────────────────────────────────────────
    skipped_count = skp.get("count", 0)
    if skipped_count > 0:
        lines.append(thin)
        lines.append(f"  SKIPPED NAMES ({skipped_count})")
        lines.append(thin)
        for row in skp.get("names", []):
            ticker = str(row.get("ticker", "?"))
            reason = row.get("scoreability_reason_v2", "unknown")
            missing = row.get("missing_critical_fields_v2", "")
            sr = row.get("sectrsregime", "unknown")
            lines.append(
                f"  {ticker:<10s}  sect_regime={sr}  "
                f"reason={reason}  missing=[{missing}]"
            )
        lines.append("")

    lines.append(sep)
    lines.append("  END OF REPORT")
    lines.append(sep)

    text = "\n".join(lines)
    logger.info(
        "to_text_v2 complete: sections=8 lines=%d chars=%d",
        len(lines), len(text),
    )
    return text