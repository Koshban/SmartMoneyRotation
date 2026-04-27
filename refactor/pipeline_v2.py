"""refactor/pipeline_v2.py"""
from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd

from compute.indicators import compute_all_indicators

from common.sector_map import get_sector_or_class

from .strategy.adapters_v2 import ensure_columns
from .strategy.breadth_v2 import compute_breadth
from .strategy.portfolio_v2 import (
    build_portfolio_v2,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_MAX_SECTOR_WEIGHT,
    DEFAULT_MAX_THEME_NAMES,
    DEFAULT_MAX_SINGLE_WEIGHT,
    DEFAULT_MIN_WEIGHT,
)
from .strategy.regime_v2 import classify_volatility_regime
from .strategy.rotation_v2 import compute_sector_rotation
from .strategy.rs_v2 import compute_rs_zscores, enrich_rs_regimes
from .strategy.scoring_v2 import compute_composite_v2
from .strategy.signals_v2 import apply_convergence_v2, apply_signals_v2
from refactor.common.config_refactor import (
    VOLREGIMEPARAMS,
    SCORINGWEIGHTS_V2,
    SCORINGPARAMS_V2,
    SIGNALPARAMS_V2,
    CONVERGENCEPARAMS_V2,
    ACTIONPARAMS_V2,
    BREADTHPARAMS,          # ← NEW
    ROTATIONPARAMS,         # ← NEW
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


def _is_missing_value(value) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _classify_breadth_regime(breadth_df: pd.DataFrame | None) -> dict:
    if breadth_df is None or breadth_df.empty:
        return {
            "breadth_regime": "unknown",
            "breadthscore": None,
            "dispersion20": None,
            "dispersion": None,
        }
    row = breadth_df.iloc[-1]
    regime = row.get("breadthregime", row.get("breadth_regime", "unknown"))
    score = row.get("breadthscore", row.get("breadth_score", None))
    disp20 = row.get("dispersion20", None)
    disp = row.get("dispersion_daily", row.get("dispersion", None))
    return {
        "breadth_regime": regime,
        "breadthscore": score,
        "dispersion20": disp20,
        "dispersion": disp,
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


def _log_action_diagnostics(out: pd.DataFrame, params: dict) -> None:
    if out.empty:
        logger.info("Action diagnostics skipped because input frame is empty")
        return

    sb = params["strong_buy"]
    bu = params["buy"]
    ho = params["hold"]
    se = params["sell"]
    sc = params["strong_context"]
    wc = params["weak_context"]
    hm = params["healthy_momentum"]
    dm = params["decent_momentum"]
    oe = params["overextended"]

    score = out.get("scoreadjusted_v2", out.get("scorecomposite_v2", pd.Series(0.0, index=out.index))).fillna(0.0)
    pct = out.get("score_percentile_v2", pd.Series(0.0, index=out.index)).fillna(0.0)
    entry = out.get("sigeffectiveentrymin_v2", pd.Series(0.60, index=out.index)).fillna(0.60)
    confirmed = out.get("sigconfirmed_v2", pd.Series(0, index=out.index)).fillna(0).astype(int)
    exit_sig = out.get("sigexit_v2", pd.Series(0, index=out.index)).fillna(0).astype(int)
    breadth = out.get("breadthregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    vol = out.get("volregime", pd.Series("calm", index=out.index)).fillna("calm")
    leadership = out.get("leadership_strength", pd.Series(0.0, index=out.index)).fillna(0.0)
    rs_regime = out.get("rsregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    sector_regime = out.get("sectrsregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    rsi = out.get("rsi14", pd.Series(50.0, index=out.index)).fillna(50.0)
    adx = out.get("adx14", pd.Series(20.0, index=out.index)).fillna(20.0)
    relvol = out.get("relativevolume", pd.Series(1.0, index=out.index)).fillna(1.0)
    short_ext = out.get("closevsema30pct", pd.Series(0.0, index=out.index)).fillna(0.0)

    diag_rows = []
    fail_counts = {
        "exit_or_weak": 0, "below_sell_floor": 0, "strong_buy_not_met": 0,
        "buy_not_met": 0, "hold_not_met": 0, "weak_context": 0,
        "not_confirmed": 0, "pct_below_buy": 0, "score_below_buy": 0,
        "momentum_not_decent": 0, "momentum_not_healthy": 0,
        "overextended": 0, "relvol_below_strong_buy": 0,
    }

    for i in out.index:
        s = float(score.loc[i]); pv = float(pct.loc[i]); e = float(entry.loc[i])
        c = int(confirmed.loc[i]); x = int(exit_sig.loc[i])
        b = str(breadth.loc[i]); v = str(vol.loc[i]); l = float(leadership.loc[i])
        r = str(rs_regime.loc[i]); sr = str(sector_regime.loc[i])
        rv = float(relvol.loc[i]); ri = float(rsi.loc[i]); ax = float(adx.loc[i])
        ext = float(short_ext.loc[i])
        ticker = out.loc[i, "ticker"] if "ticker" in out.columns else str(i)

        strong_context = (
            (b in sc["breadth_regimes"] and v in sc["vol_regimes"])
            or l >= sc["min_leadership"]
        )
        weak_ctx = (
            b in wc["breadth_regimes"]
            or v in wc["vol_regimes"]
            or sr in wc["sector_regimes"]
        )
        healthy_mom = (
            r in hm["allowed_rs"]
            and sr not in hm.get("blocked_sector", [])
            and ri >= hm["min_rsi"]
            and ax >= hm["min_adx"]
        )
        decent_mom = (
            r in dm["allowed_rs"]
            and ri >= dm["min_rsi"]
            and ax >= dm["min_adx"]
        )
        overext = ext >= oe["max_ema_pct"] or ri >= oe["max_rsi"]

        sb_score_floor = max(sb["min_score"], e + sb["score_above_entry"])
        bu_score_floor = max(bu["min_score"], e + bu["score_above_entry"])
        ho_score_floor = max(ho["min_score"], e - ho["score_below_entry"])

        strong_buy_ready = (
            (not sb["requires_confirmation"] or c == 1)
            and pv >= sb["min_percentile"]
            and s >= sb_score_floor
            and (not sb["requires_strong_context"] or strong_context)
            and healthy_mom
            and rv >= sb["min_rvol"]
            and (not sb["blocks_overextended"] or not overext)
        )
        buy_ready = (
            (not bu["requires_confirmation"] or c == 1)
            and pv >= bu["min_percentile"]
            and s >= bu_score_floor
            and (not bu["requires_decent_momentum"] or decent_mom)
            and (not bu["blocks_weak_context"] or not weak_ctx)
        )
        hold_ready = (
            pv >= ho["min_percentile"]
            and s >= ho_score_floor
            and (not ho["blocks_weak_context"] or not weak_ctx)
        )

        reasons = []
        if x == 1 and (
            s < max(se["floor_score"], e - se["exit_score_below_entry"])
            or pv <= se["exit_percentile_floor"]
            or weak_ctx
        ):
            fail_counts["exit_or_weak"] += 1
            reasons.append("exit_signal_path")
        if s < se["floor_score"] or pv <= se["floor_percentile"]:
            fail_counts["below_sell_floor"] += 1
            reasons.append("below_sell_floor")
        if not strong_buy_ready:
            fail_counts["strong_buy_not_met"] += 1
            if sb["requires_confirmation"] and c != 1:
                fail_counts["not_confirmed"] += 1
                reasons.append("strong_buy:no_confirmation")
            if pv < sb["min_percentile"]:
                reasons.append(f"strong_buy:pct<{sb['min_percentile']:.2f}")
            if s < sb_score_floor:
                reasons.append(f"strong_buy:score<{sb_score_floor:.3f}")
            if sb["requires_strong_context"] and not strong_context:
                reasons.append("strong_buy:context_not_strong")
            if not healthy_mom:
                fail_counts["momentum_not_healthy"] += 1
                reasons.append("strong_buy:momentum_not_healthy")
            if rv < sb["min_rvol"]:
                fail_counts["relvol_below_strong_buy"] += 1
                reasons.append(f"strong_buy:rvol<{sb['min_rvol']:.2f}")
            if sb["blocks_overextended"] and overext:
                fail_counts["overextended"] += 1
                reasons.append("strong_buy:overextended")
        if not buy_ready:
            fail_counts["buy_not_met"] += 1
            if bu["requires_confirmation"] and c != 1:
                reasons.append("buy:no_confirmation")
            if pv < bu["min_percentile"]:
                fail_counts["pct_below_buy"] += 1
                reasons.append(f"buy:pct<{bu['min_percentile']:.2f}")
            if s < bu_score_floor:
                fail_counts["score_below_buy"] += 1
                reasons.append(f"buy:score<{bu_score_floor:.3f}")
            if bu["requires_decent_momentum"] and not decent_mom:
                fail_counts["momentum_not_decent"] += 1
                reasons.append("buy:momentum_not_decent")
            if bu["blocks_weak_context"] and weak_ctx:
                fail_counts["weak_context"] += 1
                reasons.append("buy:weak_context")
        if not hold_ready:
            fail_counts["hold_not_met"] += 1
            reasons.append("hold:not_met")

        diag_rows.append({
            "ticker": ticker, "score": round(s, 4), "pct": round(pv, 4),
            "entry": round(e, 4), "confirmed": c, "exit_sig": x,
            "breadth": b, "vol": v, "lead": round(l, 3),
            "rs": r, "sectrs": sr, "rsi14": round(ri, 2), "adx14": round(ax, 2),
            "rvol": round(rv, 2), "ema30ext": round(ext, 4),
            "strong_buy_ready": strong_buy_ready, "buy_ready": buy_ready,
            "hold_ready": hold_ready, "reasons": "; ".join(reasons[:8]),
        })

    diag_df = pd.DataFrame(diag_rows)
    action_counts = out["action_v2"].value_counts(dropna=False).to_dict() if "action_v2" in out.columns else {}
    logger.info("Action diagnostics summary: actions=%s fail_counts=%s", action_counts, fail_counts)
    if logger.isEnabledFor(logging.DEBUG):
        sort_cols = [c for c in ["buy_ready", "hold_ready", "score", "pct"] if c in diag_df.columns]
        diag_view = diag_df.sort_values(sort_cols, ascending=[False, False, False, False]) if sort_cols else diag_df
        logger.debug("Top action diagnostics rows:\n%s", diag_view.head(50).to_string(index=False))
        near_buys = diag_df[(diag_df["buy_ready"] == False) & (diag_df["hold_ready"] == True)].head(30)
        if not near_buys.empty:
            logger.debug("Near-buy names that failed BUY gate:\n%s", near_buys.to_string(index=False))


def _generate_actions(df: pd.DataFrame, params=None) -> pd.DataFrame:
    ap = params if params is not None else ACTIONPARAMS_V2

    out = _add_score_percentiles(df.copy())
    if out.empty:
        out["action_v2"] = pd.Series(dtype=object)
        out["conviction_v2"] = pd.Series(dtype=object)
        out["action_reason_v2"] = pd.Series(dtype=object)
        out["action_sort_key_v2"] = pd.Series(dtype=float)
        return out

    sb = ap["strong_buy"]
    bu = ap["buy"]
    ho = ap["hold"]
    se = ap["sell"]
    sc = ap["strong_context"]
    wc = ap["weak_context"]
    hm = ap["healthy_momentum"]
    dm = ap["decent_momentum"]
    oe = ap["overextended"]
    cv = ap["conviction"]

    logger.info(
        "Action params: sb_pct=%.2f sb_score=%.2f bu_pct=%.2f bu_score=%.2f "
        "ho_pct=%.2f ho_score=%.2f sell_floor=%.2f sell_pct=%.2f "
        "weak_ctx_breadth=%s weak_ctx_vol=%s weak_ctx_sector=%s "
        "dm_rs=%s dm_rsi=%.0f dm_adx=%.0f",
        sb["min_percentile"], sb["min_score"],
        bu["min_percentile"], bu["min_score"],
        ho["min_percentile"], ho["min_score"],
        se["floor_score"], se["floor_percentile"],
        wc["breadth_regimes"], wc["vol_regimes"], wc["sector_regimes"],
        dm["allowed_rs"], dm["min_rsi"], dm["min_adx"],
    )

    score = out.get("scoreadjusted_v2", out.get("scorecomposite_v2", pd.Series(0.0, index=out.index))).fillna(0.0)
    pct = out.get("score_percentile_v2", pd.Series(0.0, index=out.index)).fillna(0.0)
    entry = out.get("sigeffectiveentrymin_v2", pd.Series(0.60, index=out.index)).fillna(0.60)
    confirmed = out.get("sigconfirmed_v2", pd.Series(0, index=out.index)).fillna(0).astype(int)
    exit_sig = out.get("sigexit_v2", pd.Series(0, index=out.index)).fillna(0).astype(int)
    breadth = out.get("breadthregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    vol = out.get("volregime", pd.Series("calm", index=out.index)).fillna("calm")
    leadership = out.get("leadership_strength", pd.Series(0.0, index=out.index)).fillna(0.0)
    rs_regime = out.get("rsregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    sector_regime = out.get("sectrsregime", pd.Series("unknown", index=out.index)).fillna("unknown")
    rsi = out.get("rsi14", pd.Series(50.0, index=out.index)).fillna(50.0)
    adx = out.get("adx14", pd.Series(20.0, index=out.index)).fillna(20.0)
    relvol = out.get("relativevolume", pd.Series(1.0, index=out.index)).fillna(1.0)
    short_ext = out.get("closevsema30pct", pd.Series(0.0, index=out.index)).fillna(0.0)

    actions, reasons, convictions, sort_keys = [], [], [], []
    action_rank = {"STRONG_BUY": 4, "BUY": 3, "HOLD": 2, "SELL": 1}

    for i in out.index:
        s = float(score.loc[i]); pv = float(pct.loc[i]); e = float(entry.loc[i])
        c = int(confirmed.loc[i]); x = int(exit_sig.loc[i])
        b = str(breadth.loc[i]); v = str(vol.loc[i]); l = float(leadership.loc[i])
        r = str(rs_regime.loc[i]); sr = str(sector_regime.loc[i])
        rv = float(relvol.loc[i]); ri = float(rsi.loc[i]); ax = float(adx.loc[i])
        ext = float(short_ext.loc[i])

        strong_context = (
            (b in sc["breadth_regimes"] and v in sc["vol_regimes"])
            or l >= sc["min_leadership"]
        )
        weak_ctx = (
            b in wc["breadth_regimes"]
            or v in wc["vol_regimes"]
            or sr in wc["sector_regimes"]
        )
        healthy_mom = (
            r in hm["allowed_rs"]
            and sr not in hm.get("blocked_sector", [])
            and ri >= hm["min_rsi"]
            and ax >= hm["min_adx"]
        )
        decent_mom = (
            r in dm["allowed_rs"]
            and ri >= dm["min_rsi"]
            and ax >= dm["min_adx"]
        )
        overext = ext >= oe["max_ema_pct"] or ri >= oe["max_rsi"]

        if x == 1 and (
            s < max(se["floor_score"], e - se["exit_score_below_entry"])
            or pv <= se["exit_percentile_floor"]
            or weak_ctx
        ):
            action = "SELL"
            reason = "Exit condition active with weak relative rank or hostile regime"

        elif s < se["floor_score"] or pv <= se["floor_percentile"]:
            action = "SELL"
            reason = "Bottom-ranked score in the current market set"

        elif (
            (not sb["requires_confirmation"] or c == 1)
            and pv >= sb["min_percentile"]
            and s >= max(sb["min_score"], e + sb["score_above_entry"])
            and (not sb["requires_strong_context"] or strong_context)
            and healthy_mom
            and rv >= sb["min_rvol"]
            and (not sb["blocks_overextended"] or not overext)
        ):
            action = "STRONG_BUY"
            reason = "Top-decile score with confirmation, momentum, and supportive regime"

        elif (
            (not bu["requires_confirmation"] or c == 1)
            and pv >= bu["min_percentile"]
            and s >= max(bu["min_score"], e + bu["score_above_entry"])
            and (not bu["requires_decent_momentum"] or decent_mom)
            and (not bu["blocks_weak_context"] or not weak_ctx)
        ):
            action = "BUY"
            reason = "Upper-tier score with confirmation and acceptable momentum"

        elif (
            pv >= ho["min_percentile"]
            and s >= max(ho["min_score"], e - ho["score_below_entry"])
            and (not ho["blocks_weak_context"] or not weak_ctx)
        ):
            action = "HOLD"
            reason = "Mid-ranked score worth monitoring but not strong enough to buy"

        else:
            action = "SELL"
            reason = "Below hold band after percentile and regime adjustment"

        conviction = (
            "high" if (pv >= cv["high_pct"] or s >= cv["high_score"])
            else "medium" if (pv >= cv["medium_pct"] or s >= cv["medium_score"])
            else "low"
        )

        actions.append(action)
        reasons.append(reason)
        convictions.append(conviction)
        sort_keys.append(action_rank[action] * 10 + pv + s / 10.0)

    out["action_v2"] = actions
    out["conviction_v2"] = convictions
    out["action_reason_v2"] = reasons
    out["action_sort_key_v2"] = sort_keys

    _log_action_diagnostics(out, params=ap)

    sort_score_col = "scoreadjusted_v2" if "scoreadjusted_v2" in out.columns else "scorecomposite_v2"
    return out.sort_values(
        ["action_sort_key_v2", sort_score_col], ascending=[False, False]
    ).reset_index(drop=True)


def _build_review_table(action_table: pd.DataFrame, action_params: dict | None = None) -> pd.DataFrame:
    """FIX 3 (critical): overextended thresholds from config, not hardcoded."""
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

        leadership = _lookup_group_strength(
            pd.Series({"ticker": ticker, "sector": tail.iloc[-1].get("sector", "Unknown"), "theme": tail.iloc[-1].get("theme", "Unknown")}),
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
            "sector": tail.iloc[-1].get("sector", "Unknown"),
            "theme": tail.iloc[-1].get("theme", "Unknown"),
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
) -> dict:
    if bench_df is None or bench_df.empty:
        raise ValueError("bench_df is required and cannot be empty")

    config = config or {}

    # ── unpack ALL config blocks (critical fix 1 + moderate fixes 7–9) ────────
    vol_regime_params  = config.get("vol_regime_params")   or config.get("VOLREGIMEPARAMS", None)
    scoring_weights    = config.get("scoring_weights")     or config.get("SCORINGWEIGHTS_V2", None)
    scoring_params     = config.get("scoring_params")      or config.get("SCORINGPARAMS_V2", None)
    signal_params      = config.get("signal_params")       or config.get("SIGNALPARAMS_V2", None)
    convergence_params = config.get("convergence_params")  or config.get("CONVERGENCEPARAMS_V2", None)
    action_params      = config.get("action_params")       or config.get("ACTIONPARAMS_V2", None)
    breadth_params     = config.get("breadth_params")      or config.get("BREADTHPARAMS", None)       # FIX 8
    rotation_params    = config.get("rotation_params")     or config.get("ROTATIONPARAMS", None)      # FIX 9

    logger.info(
        "run_pipeline_v2 start: market=%s tradable=%d leadership=%d",
        market, len(tradable_frames), len(leadership_frames or {}),
    )

    # ── A. volatility regime from benchmark (FIX 1: config-driven) ────────────
    regime_df = classify_volatility_regime(bench_df, params=vol_regime_params)

    # ── B. merge all symbol frames into one working universe ──────────────────
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

    # ── C. BREADTH (FIX 8: config-driven) ─────────────────────────────────────
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
        }
        breadth_source = "none"

    logger.info(
        "Breadth context (source=%s): regime=%s score=%s dispersion20=%s",
        breadth_source,
        breadth_info.get("breadth_regime", "unknown"),
        breadth_info.get("breadthscore"),
        breadth_info.get("dispersion20"),
    )

    # ── D. CROSS-SECTIONAL RS ─────────────────────────────────────────────────
    all_symbol_frames = compute_rs_zscores(all_symbol_frames, bench_df)
    all_symbol_frames = enrich_rs_regimes(all_symbol_frames)

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

    # ── D2. SECTOR ROTATION (FIX 9: config-driven) ───────────────────────────
    rotation_result = compute_sector_rotation(
        all_symbol_frames,
        bench_df,
        market=market,
        params=rotation_params,
    )
    sector_regimes = rotation_result["sector_regimes"]
    ticker_regimes = rotation_result["ticker_regimes"]
    sector_summary = rotation_result["sector_summary"]

    # ── E. leadership snapshot ────────────────────────────────────────────────
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
    logger.info(
        "Regime context: breadth=%s breadthscore=%s vol=%s volscore=%s "
        "leading_sectors=%s lagging_sectors=%s",
        breadth_info.get("breadth_regime", "unknown"),
        breadth_info.get("breadthscore"),
        last_vol.get("volregime", "unknown"),
        last_vol.get("volregimescore", None),
        leading_sectors or ["none"],
        lagging_sectors or ["none"],
    )

    # ── rotation recommendation mapping (FIX 7: from convergence config) ──────
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

        prepared = _prepare_frame(df)
        row = prepared.iloc[-1].to_dict()
        row["ticker"] = ticker
        row["instrument_type"] = _instrument_type(ticker)

        # ── attach pipeline-level context ─────────────────────────────────────
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

        _row_sr = row.get("sectrsregime")
        if _row_sr is None or str(_row_sr).lower() in ("unknown", "nan", ""):
            row["sectrsregime"] = ticker_regimes.get(ticker, "unknown")

        # ── rotation recommendation (FIX 7: from config map) ──────────────────
        _row_rr = row.get("rotationrec")
        if _row_rr is None or str(_row_rr).lower() in ("unknown", "nan", ""):
            row["rotationrec"] = _sect_to_rec.get(
                str(row.get("sectrsregime", "unknown")).lower(),
                _sect_to_rec_default,
            )

        row["theme"] = row.get("theme") or "Unknown"

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
                "rotationrec=%s breadth=%s vol=%s dispersion20=%s sector=%s",
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
            )
            prep_logged += 1

    latest = pd.DataFrame(latest_rows) if latest_rows else pd.DataFrame()
    skipped_table = _build_skipped_table(skipped_rows)
    logger.info("Latest tradable snapshot rows=%d skipped=%d", len(latest), len(skipped_table))

    if not latest.empty:
        latest["leadership_strength"] = latest.apply(lambda row: _lookup_group_strength(row, leadership_snapshot), axis=1)
        latest = ensure_columns(latest)
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

        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in [
                "ticker", "close", "rsi14", "adx14", "atr14pct", "relativevolume",
                "rszscore", "rsregime", "sectrsregime", "rotationrec",
                "leadership_strength", "breadthregime", "breadthscore",
                "volregime", "realizedvol20d", "gaprate20", "dispersion20",
                "scoreable_v2", "missing_critical_fields_v2", "sector", "theme",
            ] if c in latest.columns]
            logger.debug("Latest snapshot preview:\n%s", latest[cols].head(30).to_string(index=False))

    scored = compute_composite_v2(latest, weights=scoring_weights, params=scoring_params) if not latest.empty else pd.DataFrame()
    logger.info("Scored rows=%d", len(scored))
    if not scored.empty:
        _ap = action_params if action_params is not None else ACTIONPARAMS_V2
        _lead_w = _ap.get("leadership_boost_weight", 0.10)
        scored["scorecomposite_v2"] = (
            scored["scorecomposite_v2"]
            + _lead_w * scored.get("leadership_strength", 0.0)
        ).clip(0, 1)

        # FIX 13: use config entry threshold for diagnostics
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
            cols = [c for c in ["ticker", "scoretrend", "scoreparticipation", "scorerisk", "scoreregime", "scorepenalty", "scorecomposite_v2", "rsi14", "adx14", "relativevolume"] if c in scored.columns]
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

    # FIX 3 (critical): pass action_params to review table
    review_table = _build_review_table(action_table, action_params=action_params) if not action_table.empty else pd.DataFrame()

    selling_exhaustion_table = _build_selling_exhaustion_table(
        tradable_enriched,
        breadth_info.get("breadth_regime", "unknown"),
        last_vol.get("volregime", "unknown"),
        leadership_snapshot,
    )

    # FIX 2 (critical): forward ALL portfolio params
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
        "sector_summary": sector_summary,
        "sector_regimes": sector_regimes,
        "leadership_snapshot": leadership_snapshot,
    }