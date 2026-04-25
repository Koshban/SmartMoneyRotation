""" refactor/pipeline_v2.py """
from __future__ import annotations

import logging
import math
import pandas as pd

from compute.indicators import compute_all_indicators

from .strategy.adapters_v2 import ensure_columns
from .strategy.portfolio_v2 import build_portfolio_v2
from .strategy.regime_v2 import classify_volatility_regime
from .strategy.scoring_v2 import compute_composite_v2
from .strategy.signals_v2 import apply_convergence_v2, apply_signals_v2

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
        return {"breadth_regime": "unknown", "breadthscore": None}
    row = breadth_df.iloc[-1]
    regime = row.get("breadthregime", row.get("breadth_regime", "unknown"))
    score = row.get("breadthscore", row.get("breadth_score", None))
    return {"breadth_regime": regime, "breadthscore": score}


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

    missing_counts = []
    missing_fields = []
    reasons = []
    scoreable_flags = []

    for _, row in out.iterrows():
        row_missing = [c for c in required_cols if _is_missing_value(row.get(c))]
        row_missing.extend(unavailable_required)

        if available_optional:
            optional_missing = [c for c in available_optional if _is_missing_value(row.get(c))]
            if len(optional_missing) == len(available_optional):
                row_missing.append("dispersion_proxy_missing")

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
        mn = float(out["rszscore"].min())
        mx = float(out["rszscore"].max())
        denom = max(mx - mn, 1e-9)
        out["leadership_strength"] = (out["rszscore"] - mn) / denom
        logger.info("Leadership normalization applied: rszscore min=%.4f max=%.4f", mn, mx)
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
        return float(direct["leadership_strength"].max())

    theme_col = leadership_snapshot["theme"] if "theme" in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    theme_match = leadership_snapshot[theme_col.eq(theme)]
    if not theme_match.empty:
        return float(theme_match["leadership_strength"].max())

    sector_col = leadership_snapshot["sector"] if "sector" in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    sector_match = leadership_snapshot[sector_col.eq(sector)]
    if not sector_match.empty:
        return float(sector_match["leadership_strength"].max())

    broad = leadership_snapshot[leadership_snapshot["ticker"].isin(["SPY", "QQQ", "IWM"])]
    if not broad.empty:
        return float(broad["leadership_strength"].mean())

    return 0.0


def _add_score_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["score_percentile_v2"] = pd.Series(dtype=float)
        return out
    base_score = out.get("scoreadjusted_v2", out.get("scorecomposite_v2", pd.Series(0.0, index=out.index)))
    out["score_percentile_v2"] = base_score.rank(pct=True, method="average").fillna(0.0)
    return out


def _log_action_diagnostics(out: pd.DataFrame) -> None:
    if out.empty:
        logger.info("Action diagnostics skipped because input frame is empty")
        return
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
        "exit_or_weak": 0,
        "below_sell_floor": 0,
        "strong_buy_not_met": 0,
        "buy_not_met": 0,
        "hold_not_met": 0,
        "weak_context": 0,
        "not_confirmed": 0,
        "pct_below_buy": 0,
        "score_below_buy": 0,
        "momentum_not_decent": 0,
        "momentum_not_healthy": 0,
        "overextended": 0,
        "relvol_below_strong_buy": 0,
    }

    for i in out.index:
        s = float(score.loc[i]); p = float(pct.loc[i]); e = float(entry.loc[i]); c = int(confirmed.loc[i]); x = int(exit_sig.loc[i])
        b = str(breadth.loc[i]); v = str(vol.loc[i]); l = float(leadership.loc[i]); r = str(rs_regime.loc[i]); sr = str(sector_regime.loc[i])
        rv = float(relvol.loc[i]); ri = float(rsi.loc[i]); ax = float(adx.loc[i]); ext = float(short_ext.loc[i])
        ticker = out.loc[i, "ticker"] if "ticker" in out.columns else str(i)

        strong_context = (b == "strong" and v == "calm") or l >= 0.60
        weak_context = b in {"weak", "critical"} or v == "chaotic" or sr == "lagging"
        healthy_momentum = r in {"leading", "improving"} and sr != "lagging" and ri >= 52 and ax >= 22
        decent_momentum = r in {"leading", "improving"} and ri >= 45 and ax >= 16
        overextended = ext >= 0.045 or ri >= 74

        strong_buy_ready = c == 1 and p >= 0.90 and s >= max(0.76, e + 0.08) and strong_context and healthy_momentum and rv >= 1.10 and not overextended
        buy_ready = c == 1 and p >= 0.65 and s >= max(0.62, e + 0.02) and decent_momentum and not weak_context
        hold_ready = p >= 0.35 and s >= max(0.54, e - 0.06) and not weak_context

        reasons = []
        if x == 1 and (s < max(0.50, e - 0.05) or p <= 0.20 or weak_context):
            fail_counts["exit_or_weak"] += 1
            reasons.append("exit_signal_path")
        if s < 0.50 or p <= 0.15:
            fail_counts["below_sell_floor"] += 1
            reasons.append("below_sell_floor")
        if not strong_buy_ready:
            fail_counts["strong_buy_not_met"] += 1
            if c != 1:
                fail_counts["not_confirmed"] += 1
                reasons.append("strong_buy:no_confirmation")
            if p < 0.90:
                reasons.append(f"strong_buy:pct<{0.90:.2f}")
            if s < max(0.76, e + 0.08):
                reasons.append(f"strong_buy:score<{max(0.76, e + 0.08):.3f}")
            if not strong_context:
                reasons.append("strong_buy:context_not_strong")
            if not healthy_momentum:
                fail_counts["momentum_not_healthy"] += 1
                reasons.append("strong_buy:momentum_not_healthy")
            if rv < 1.10:
                fail_counts["relvol_below_strong_buy"] += 1
                reasons.append("strong_buy:rvol<1.10")
            if overextended:
                fail_counts["overextended"] += 1
                reasons.append("strong_buy:overextended")
        if not buy_ready:
            fail_counts["buy_not_met"] += 1
            if c != 1:
                reasons.append("buy:no_confirmation")
            if p < 0.65:
                fail_counts["pct_below_buy"] += 1
                reasons.append("buy:pct<0.65")
            if s < max(0.62, e + 0.02):
                fail_counts["score_below_buy"] += 1
                reasons.append(f"buy:score<{max(0.62, e + 0.02):.3f}")
            if not decent_momentum:
                fail_counts["momentum_not_decent"] += 1
                reasons.append("buy:momentum_not_decent")
            if weak_context:
                fail_counts["weak_context"] += 1
                reasons.append("buy:weak_context")
        if not hold_ready:
            fail_counts["hold_not_met"] += 1
            reasons.append("hold:not_met")
        diag_rows.append({
            "ticker": ticker,
            "score": round(s, 4),
            "pct": round(p, 4),
            "entry": round(e, 4),
            "confirmed": c,
            "exit_sig": x,
            "breadth": b,
            "vol": v,
            "lead": round(l, 3),
            "rs": r,
            "sectrs": sr,
            "rsi14": round(ri, 2),
            "adx14": round(ax, 2),
            "rvol": round(rv, 2),
            "ema30ext": round(ext, 4),
            "strong_buy_ready": strong_buy_ready,
            "buy_ready": buy_ready,
            "hold_ready": hold_ready,
            "reasons": "; ".join(reasons[:8]),
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


def _generate_actions(df: pd.DataFrame) -> pd.DataFrame:
    out = _add_score_percentiles(df.copy())
    if out.empty:
        out["action_v2"] = pd.Series(dtype=object)
        out["conviction_v2"] = pd.Series(dtype=object)
        out["action_reason_v2"] = pd.Series(dtype=object)
        out["action_sort_key_v2"] = pd.Series(dtype=float)
        return out

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
        s = float(score.loc[i]); p = float(pct.loc[i]); e = float(entry.loc[i]); c = int(confirmed.loc[i]); x = int(exit_sig.loc[i])
        b = str(breadth.loc[i]); v = str(vol.loc[i]); l = float(leadership.loc[i]); r = str(rs_regime.loc[i]); sr = str(sector_regime.loc[i])
        rv = float(relvol.loc[i]); ri = float(rsi.loc[i]); ax = float(adx.loc[i]); ext = float(short_ext.loc[i])

        strong_context = (b == "strong" and v == "calm") or l >= 0.60
        weak_context = b in {"weak", "critical"} or v == "chaotic" or sr == "lagging"
        healthy_momentum = r in {"leading", "improving"} and sr != "lagging" and ri >= 52 and ax >= 22
        decent_momentum = r in {"leading", "improving"} and ri >= 45 and ax >= 16
        overextended = ext >= 0.045 or ri >= 74

        if x == 1 and (s < max(0.50, e - 0.05) or p <= 0.20 or weak_context):
            action = "SELL"; reason = "Exit condition active with weak relative rank or hostile regime"
        elif s < 0.50 or p <= 0.15:
            action = "SELL"; reason = "Bottom-ranked score in the current market set"
        elif c == 1 and p >= 0.90 and s >= max(0.76, e + 0.08) and strong_context and healthy_momentum and rv >= 1.10 and not overextended:
            action = "STRONG_BUY"; reason = "Top-decile score with confirmation, momentum, and supportive regime"
        elif c == 1 and p >= 0.65 and s >= max(0.62, e + 0.02) and decent_momentum and not weak_context:
            action = "BUY"; reason = "Upper-tier score with confirmation and acceptable momentum"
        elif p >= 0.35 and s >= max(0.54, e - 0.06) and not weak_context:
            action = "HOLD"; reason = "Mid-ranked score worth monitoring but not strong enough to buy"
        else:
            action = "SELL"; reason = "Below hold band after percentile and regime adjustment"

        conviction = "high" if (p >= 0.90 or s >= 0.84) else ("medium" if (p >= 0.60 or s >= 0.68) else "low")
        actions.append(action)
        reasons.append(reason)
        convictions.append(conviction)
        sort_keys.append(action_rank[action] * 10 + p + s / 10.0)

    out["action_v2"] = actions
    out["conviction_v2"] = convictions
    out["action_reason_v2"] = reasons
    out["action_sort_key_v2"] = sort_keys
    _log_action_diagnostics(out)
    sort_score_col = "scoreadjusted_v2" if "scoreadjusted_v2" in out.columns else "scorecomposite_v2"
    return out.sort_values(["action_sort_key_v2", sort_score_col], ascending=[False, False]).reset_index(drop=True)


def _build_review_table(action_table: pd.DataFrame) -> pd.DataFrame:
    if action_table.empty:
        return pd.DataFrame()

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
        (review.get("price_vs_ema30_pct", pd.Series(0.0, index=review.index)).fillna(0) >= 0.045) |
        (review.get("rsi_14", pd.Series(50.0, index=review.index)).fillna(50) >= 74)
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
            "ticker": ticker,
            "instrument_type": _instrument_type(ticker),
            "status": setup,
            "quality_label": quality,
            "selling_exhaustion_score": exhaustion_score,
            "reversal_trigger_score": trigger_score,
            "rsi_14": last_rsi,
            "rsi_down_streak_3d": rsi_down_streak,
            "rsi_turn_up_1d": "YES" if rsi_turn_up else "NO",
            "close_down_streak_3d": down_streak,
            "bullish_close_1d": "YES" if bullish_close else "NO",
            "close_above_prior_high": "YES" if close_above_prior_high else "NO",
            "relative_volume": last_vol,
            "volume_down_streak_3d": vol_down_streak,
            "volume_reexpansion_1d": "YES" if volume_reexpansion else "NO",
            "adx_14": last_adx,
            "adx_stabilizing_1d": "YES" if adx_stabilizing else "NO",
            "price_5d_change": price_5d_change,
            "price_vs_ema30_pct": last_ext,
            "atr_14_pct": last_atr,
            "gap_rate_20": last_gap,
            "leadership_strength": leadership,
            "breadthregime": breadth_regime,
            "volregime": vol_regime,
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


def run_pipeline_v2(
    tradable_frames: dict[str, pd.DataFrame],
    bench_df: pd.DataFrame,
    breadth_df: pd.DataFrame | None = None,
    market: str = "US",
    leadership_frames: dict[str, pd.DataFrame] | None = None,
    portfolio_params: dict | None = None,
) -> dict:
    if bench_df is None or bench_df.empty:
        raise ValueError("bench_df is required and cannot be empty")

    logger.info("run_pipeline_v2 start: market=%s tradable=%d leadership=%d", market, len(tradable_frames), len(leadership_frames or {}))
    regime_df = classify_volatility_regime(bench_df)
    breadth_info = _classify_breadth_regime(breadth_df)
    leadership_snapshot = _normalize_leadership(_build_leadership_snapshot(leadership_frames or {}))
    last_vol = regime_df.iloc[-1]
    logger.info(
        "Regime context: breadth=%s breadthscore=%s vol=%s volscore=%s",
        breadth_info.get("breadth_regime", "unknown"),
        breadth_info.get("breadthscore", None),
        last_vol.get("volregime", "unknown"),
        last_vol.get("volregimescore", None),
    )

    latest_rows = []
    skipped_rows = []
    prep_logged = 0

    for ticker, df in tradable_frames.items():
        if df is None or df.empty:
            logger.debug("Skipping empty tradable frame for %s", ticker)
            continue

        prepared = _prepare_frame(df)
        row = prepared.iloc[-1].to_dict()
        row["ticker"] = ticker
        row["instrument_type"] = _instrument_type(ticker)
        row["volregime"] = row.get("volregime", last_vol.get("volregime", "calm"))
        row["volregimescore"] = row.get("volregimescore", last_vol.get("volregimescore", 0.0))
        row["breadthregime"] = row.get("breadthregime", breadth_info.get("breadth_regime", "unknown"))
        row["breadthscore"] = row.get("breadthscore", breadth_info.get("breadthscore", 0.5))
        row["sector"] = row.get("sector", "Unknown")
        row["theme"] = row.get("theme", "Unknown")

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
                "Prepared %s last-row snapshot: close=%.4f rsi14=%.2f adx14=%.2f atr14pct=%.4f rvol=%.2f score_inputs(rs=%s sect=%s breadth=%s vol=%s)",
                ticker,
                float(row.get("close", 0.0) or 0.0),
                float(row.get("rsi14", 50.0) or 50.0),
                float(row.get("adx14", 20.0) or 20.0),
                float(row.get("atr14pct", 0.03) or 0.03),
                float(row.get("relativevolume", 1.0) or 1.0),
                row.get("rsregime", "unknown"),
                row.get("sectrsregime", "unknown"),
                row.get("breadthregime", "unknown"),
                row.get("volregime", "unknown"),
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
            "Latest snapshot diagnostics: avg_rsi14=%.2f avg_adx14=%.2f avg_rvol=%.2f avg_lead=%.2f",
            float(latest["rsi14"].fillna(50.0).mean()),
            float(latest["adx14"].fillna(20.0).mean()),
            float(latest["relativevolume"].fillna(1.0).mean()),
            float(latest["leadership_strength"].fillna(0.0).mean()),
        )
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "close", "rsi14", "adx14", "atr14pct", "relativevolume", "leadership_strength", "breadthregime", "volregime", "scoreable_v2", "missing_critical_fields_v2", "sector", "theme"] if c in latest.columns]
            logger.debug("Latest snapshot preview:\n%s", latest[cols].head(30).to_string(index=False))

    scored = compute_composite_v2(latest) if not latest.empty else pd.DataFrame()
    logger.info("Scored rows=%d", len(scored))
    if not scored.empty:
        scored["scorecomposite_v2"] = (scored["scorecomposite_v2"] + 0.10 * scored.get("leadership_strength", 0.0)).clip(0, 1)
        logger.info(
            "Score diagnostics: min=%.4f median=%.4f max=%.4f >=0.62=%d >=0.50=%d",
            float(scored["scorecomposite_v2"].min()),
            float(scored["scorecomposite_v2"].median()),
            float(scored["scorecomposite_v2"].max()),
            int((scored["scorecomposite_v2"] >= 0.62).sum()),
            int((scored["scorecomposite_v2"] >= 0.50).sum()),
        )
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "scoretrend", "scoreparticipation", "scorerisk", "scoreregime", "scorepenalty", "scorecomposite_v2", "rsi14", "adx14", "relativevolume"] if c in scored.columns]
            logger.debug("Top scored names:\n%s", scored.sort_values("scorecomposite_v2", ascending=False)[cols].head(30).to_string(index=False))

    signaled = apply_signals_v2(scored) if not scored.empty else pd.DataFrame()
    logger.info("Signals rows=%d", len(signaled))
    if not signaled.empty and logger.isEnabledFor(logging.DEBUG):
        cols = [c for c in ["ticker", "scorecomposite_v2", "sigeffectiveentrymin_v2", "sigconfirmed_v2", "sigexit_v2", "rsi14", "adx14"] if c in signaled.columns]
        logger.debug("Signal preview:\n%s", signaled.sort_values("scorecomposite_v2", ascending=False)[cols].head(30).to_string(index=False))

    converged = apply_convergence_v2(signaled) if not signaled.empty else pd.DataFrame()
    logger.info("Converged rows=%d", len(converged))
    if not converged.empty and logger.isEnabledFor(logging.DEBUG):
        cols = [c for c in ["ticker", "scorecomposite_v2", "scoreadjusted_v2", "sigeffectiveentrymin_v2", "sigconfirmed_v2", "sigexit_v2", "rsi14", "adx14", "relativevolume"] if c in converged.columns]
        logger.debug("Converged preview:\n%s", converged.sort_values(cols[1] if len(cols) > 1 else converged.columns[0], ascending=False)[cols].head(30).to_string(index=False))

    action_table = _generate_actions(converged) if not converged.empty else pd.DataFrame()
    logger.info("Action table rows=%d", len(action_table))
    if not action_table.empty:
        logger.info("Action counts=%s", action_table["action_v2"].value_counts().to_dict())
        if logger.isEnabledFor(logging.DEBUG):
            cols = [c for c in ["ticker", "action_v2", "conviction_v2", "scorecomposite_v2", "scoreadjusted_v2", "score_percentile_v2", "rsi14", "adx14", "relativevolume", "action_reason_v2"] if c in action_table.columns]
            logger.debug("Action table preview:\n%s", action_table[cols].head(50).to_string(index=False))

    review_table = _build_review_table(action_table) if not action_table.empty else pd.DataFrame()
    selling_exhaustion_table = _build_selling_exhaustion_table(
        tradable_frames,
        breadth_info.get("breadth_regime", "unknown"),
        last_vol.get("volregime", "unknown"),
        leadership_snapshot,
    )
    params = portfolio_params or {}
    portfolio = (
        build_portfolio_v2(
            action_table,
            max_positions=params.get("max_positions", 8),
            max_sector_weight=params.get("max_sector_weight", 0.35),
            max_theme_names=params.get("max_theme_names", 2),
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
        len(latest),
        len(skipped_table),
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
        "leadership_snapshot": leadership_snapshot,
    }

##################################################################
""" refactor/demo_runner.py """
from __future__ import annotations
import pandas as pd
from .strategy.regime_v2 import classify_volatility_regime
from .strategy.adapters_v2 import ensure_columns, attach_benchmark_regime, attach_breadth_context
from .strategy.scoring_v2 import compute_composite_v2
from .strategy.signals_v2 import apply_signals_v2, apply_convergence_v2
from .pipeline_v2 import run_pipeline_v2


def run_demo() -> dict:
    idx = pd.date_range('2025-01-01', periods=8, freq='D')
    bench = pd.DataFrame({'close':[100,102,101,104,103,105,107,106], 'high':[101,103,102,105,104,106,108,107], 'low':[99,101,100,103,102,104,106,105]}, index=idx)
    regime = classify_volatility_regime(bench)
    breadth = pd.DataFrame({'breadthscore':[0.52,0.54,0.56,0.59,0.61,0.58,0.57,0.60], 'breadthregime':['neutral','neutral','neutral','strong','strong','neutral','neutral','strong']}, index=idx)
    names = {
        'NVDA': {'sector':'Technology','theme':'AI'},
        'CRWD': {'sector':'Technology','theme':'Cybersecurity'},
        'CEG': {'sector':'Utilities','theme':'Nuclear'},
        'PLTR': {'sector':'Technology','theme':'AI'},
    }
    universe_frames = {}
    for i, (ticker, meta) in enumerate(names.items()):
        df = pd.DataFrame({
            'ticker': ticker,
            'sector': meta['sector'],
            'theme': meta['theme'],
            'close': [100+i,102+i,103+i,104+i,105+i,106+i,107+i,108+i],
            'rszscore': [0.1+i*0.2,0.2+i*0.2,0.4+i*0.2,0.7+i*0.2,0.8+i*0.2,0.9+i*0.2,1.0+i*0.2,1.1+i*0.2],
            'sectrszscore': [0.2,0.2,0.3,0.4,0.4,0.5,0.5,0.6],
            'rsaccel20': [0.0,0.01,0.02,0.02,0.03,0.03,0.04,0.04],
            'closevsema30pct': [0.00,0.01,0.02,0.03,0.02,0.01,0.02,0.01],
            'closevssma50pct': [0.03,0.04,0.04,0.05,0.05,0.05,0.06,0.05],
            'relativevolume': [1.0,1.1,1.1,1.2,1.2,1.2,1.3,1.3],
            'obvslope10d': [0.00,0.01,0.01,0.02,0.02,0.03,0.03,0.03],
            'adlineslope10d': [0.00,0.00,0.01,0.01,0.01,0.02,0.02,0.02],
            'dollarvolume20d': [2e7,2e7,2.2e7,2.3e7,2.5e7,2.7e7,2.8e7,3e7],
            'atr14pct': [0.03,0.03,0.03,0.04,0.04,0.04,0.03,0.03],
            'amihud20': [0.001]*8,
            'gaprate20': [0.06]*8,
            'rsi14': [52,54,55,58,57,56,58,55],
            'adx14': [18,19,20,22,22,23,24,24],
            'rsregime': ['improving','improving','improving','leading','leading','leading','leading','leading'],
            'sectrsregime': ['neutral','neutral','leading','leading','leading','leading','leading','leading'],
            'rotationrec': ['HOLD','BUY','BUY','BUY','BUY','BUY','BUY','STRONGBUY'],
        }, index=idx)
        universe_frames[ticker] = df
    try:
        return run_pipeline_v2(universe_frames, bench, breadth, market='US')
    except Exception as e:
        return {'error': str(e), 'portfolio': {'selected': pd.DataFrame(), 'meta': {'target_exposure': 0.0, 'reason': 'repo universe not found in sandbox demo'}}}

if __name__ == '__main__':
    result = run_demo()
    print(result.get('portfolio', {}).get('meta', result.get('error')))
    selected = result.get('portfolio', {}).get('selected', pd.DataFrame())
    if selected is None or selected.empty:
        print('no selections')
    else:
        cols = [c for c in ['ticker','sector','theme','scoreadjusted_v2','target_weight'] if c in selected.columns]
        print(selected[cols])

############################################################
""" refactor/report_v2.py"""
from __future__ import annotations

import logging
import math
import pandas as pd

logger = logging.getLogger(__name__)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def _df_shape(df: pd.DataFrame | None) -> tuple[int, int]:
    if df is None:
        return (0, 0)
    return tuple(df.shape)


def _df_cols(df: pd.DataFrame | None) -> list[str]:
    if df is None:
        return []
    return list(df.columns)


def _preview_df(df: pd.DataFrame | None, cols: list[str], n: int = 10) -> list[dict]:
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


def _log_report_inputs(result: dict, portfolio: dict, selected: pd.DataFrame, actions: pd.DataFrame, review: pd.DataFrame, latest: pd.DataFrame) -> None:
    logger.info(
        "build_report_v2 input shapes: selected=%s actions=%s review=%s latest=%s",
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
    logger.info(
        "build_report_v2 columns: selected=%s actions=%s review=%s latest=%s",
        _df_cols(selected),
        _df_cols(actions),
        _df_cols(review),
        _df_cols(latest),
    )


def _log_portfolio_meta(meta: dict, market: str, leadership: list, tradable: list) -> None:
    logger.info(
        "build_report_v2 market=%s tradable=%d leadership=%d selected_count=%s candidate_count=%s target_exposure=%s",
        market,
        len(tradable),
        len(leadership),
        meta.get("selected_count", 0),
        meta.get("candidate_count", 0),
        meta.get("target_exposure", 0.0),
    )
    logger.info(
        "build_report_v2 regime meta: breadth_regime=%s vol_regime=%s",
        meta.get("breadth_regime", "unknown"),
        meta.get("vol_regime", "unknown"),
    )


def _log_review_summary(review: pd.DataFrame | None, top: list[dict]) -> None:
    if review is None or review.empty:
        logger.warning("build_report_v2: review table is empty")
        return
    logger.info(
        "build_report_v2 review summary: rows=%d cols=%d top_rows=%d",
        len(review),
        len(review.columns),
        len(top),
    )
    if "recommendation" in review.columns:
        logger.info("build_report_v2 review recommendations=%s", review["recommendation"].value_counts(dropna=False).to_dict())
    if "action_v2" in review.columns:
        logger.info("build_report_v2 review action_v2=%s", review["action_v2"].value_counts(dropna=False).to_dict())
    if "ticker" in review.columns and logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "build_report_v2 review preview:\n%s",
            review.head(30).to_string(index=False),
        )


def _log_selected_summary(selected: pd.DataFrame | None, selected_preview: list[dict]) -> None:
    if selected is None or selected.empty:
        logger.warning("build_report_v2: selected portfolio is empty")
        return
    logger.info(
        "build_report_v2 selected summary: rows=%d cols=%d preview_rows=%d",
        len(selected),
        len(selected.columns),
        len(selected_preview),
    )
    if "action_v2" in selected.columns:
        logger.info("build_report_v2 selected action_v2=%s", selected["action_v2"].value_counts(dropna=False).to_dict())
    if "sector" in selected.columns:
        logger.info("build_report_v2 selected sector=%s", selected["sector"].value_counts(dropna=False).to_dict())
    if "theme" in selected.columns:
        logger.info("build_report_v2 selected theme=%s", selected["theme"].value_counts(dropna=False).to_dict())
    if logger.isEnabledFor(logging.DEBUG):
        keep = [c for c in ["ticker", "target_weight", "scoreadjusted_v2", "action_v2", "sector", "theme"] if c in selected.columns]
        if keep:
            logger.debug("build_report_v2 selected preview:\n%s", selected[keep].head(30).to_string(index=False))


def build_report_v2(result: dict) -> dict:
    portfolio = result.get("portfolio", {}) or {}
    selected = portfolio.get("selected", pd.DataFrame())
    actions = result.get("action_table", pd.DataFrame())
    review = result.get("review_table", pd.DataFrame())
    meta = portfolio.get("meta", {}) or {}
    latest = result.get("latest", pd.DataFrame())
    market = result.get("market", "UNKNOWN")
    leadership = result.get("leadership_universe", []) or []
    tradable = result.get("tradable_universe", []) or []

    _log_report_inputs(result, portfolio, selected, actions, review, latest)
    _log_portfolio_meta(meta, market, leadership, tradable)

    top = []
    if review is not None and not review.empty:
        top = review.head(10).to_dict(orient="records")
    _log_review_summary(review, top)

    selected_preview = []
    if selected is not None and not selected.empty:
        keep = [c for c in ["ticker", "target_weight", "scoreadjusted_v2", "action_v2", "sector", "theme"] if c in selected.columns]
        selected_preview = selected[keep].head(10).to_dict(orient="records")
    _log_selected_summary(selected, selected_preview)

    action_summary = _count_actions(actions)
    logger.info("build_report_v2 action summary=%s", action_summary)

    report = {
        "header": {
            "market": market,
            "tradable_universe_size": len(tradable),
            "leadership_universe_size": len(leadership),
            "processed_names": 0 if latest is None else len(latest),
            "rsi_field": "rsi14",
        },
        "regime": {
            "breadth_regime": meta.get("breadth_regime", "unknown"),
            "vol_regime": meta.get("vol_regime", "unknown"),
            "target_exposure": _safe_float(meta.get("target_exposure", 0.0)),
        },
        "actions": action_summary,
        "portfolio": {
            "selected_count": int(meta.get("selected_count", 0)),
            "candidate_count": int(meta.get("candidate_count", 0)),
            "top_picks": top,
            "selected_preview": selected_preview,
        },
    }

    logger.info(
        "build_report_v2 output ready: top_picks=%d selected_preview=%d",
        len(top),
        len(selected_preview),
    )
    return report


def to_text_v2(report: dict) -> str:
    h = report["header"]
    r = report["regime"]
    a = report["actions"]
    p = report["portfolio"]

    logger.info(
        "to_text_v2 start: market=%s top_picks=%d selected_preview=%d",
        h.get("market", "UNKNOWN"),
        len(p.get("top_picks", [])),
        len(p.get("selected_preview", [])),
    )

    lines = []
    lines.append("CASH V2 REPORT")
    lines.append("-" * 90)
    lines.append(f"Market                  : {h['market']}")
    lines.append(f"Tradable universe size  : {h['tradable_universe_size']}")
    lines.append(f"Leadership universe size: {h['leadership_universe_size']}")
    lines.append(f"Processed names         : {h['processed_names']}")
    lines.append(f"RSI field used          : {h['rsi_field']} (RSI 14)")
    lines.append(f"Breadth regime          : {r['breadth_regime']}")
    lines.append(f"Vol regime              : {r['vol_regime']}")
    lines.append(f"Target exposure         : {_safe_float(r['target_exposure']):.2%}")
    lines.append(f"Action counts           : STRONG_BUY={a['STRONG_BUY']} BUY={a['BUY']} HOLD={a['HOLD']} SELL={a['SELL']}")
    lines.append(f"Candidates              : {p['candidate_count']}")
    lines.append(f"Selected                : {p['selected_count']}")
    lines.append("")

    lines.append("REVIEW TABLE")
    lines.append("-" * 90)
    if not p["top_picks"]:
        lines.append("No signals")
    else:
        for i, row in enumerate(p["top_picks"], 1):
            lines.append(
                f"{i:>2}. {str(row.get('ticker', '?')):12s} {str(row.get('recommendation', '?')):10s} "
                f"score={_safe_float(row.get('composite_score', 0)):.3f} pct={_safe_float(row.get('score_percentile', 0)):.0%} "
                f"rsi14={_safe_float(row.get('rsi_14', 0)):.1f} adx14={_safe_float(row.get('adx_14', 0)):.1f} rv={_safe_float(row.get('relative_volume', 0)):.2f} "
                f"lead={_safe_float(row.get('leadership_strength', 0)):.2f} sector={row.get('sector', 'Unknown')} theme={row.get('theme', 'Unknown')}"
            )

    if p.get("selected_preview"):
        lines.append("")
        lines.append("SELECTED PORTFOLIO")
        lines.append("-" * 90)
        for i, row in enumerate(p["selected_preview"], 1):
            lines.append(
                f"{i:>2}. {str(row.get('ticker', '?')):12s} wt={_safe_float(row.get('target_weight', 0)):.2%} "
                f"score={_safe_float(row.get('scoreadjusted_v2', 0)):.3f} action={row.get('action_v2', '?')} "
                f"sector={row.get('sector', 'Unknown')} theme={row.get('theme', 'Unknown')}"
            )

    text = "\n".join(lines)
    logger.info("to_text_v2 complete: lines=%d chars=%d", len(lines), len(text))
    return text

##############################################################
from __future__ import annotations


import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


import pandas as pd


from common.config import DATA_DIR, LOGS_DIR
from .common.market_config_v2 import get_market_config_v2
from .pipeline_v2 import run_pipeline_v2
from .report_v2 import build_report_v2, to_text_v2


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



def run_strategy_v2(market: str, universe_frames: dict[str, pd.DataFrame], bench_df: pd.DataFrame, breadth_df: pd.DataFrame | None = None) -> dict[str, Any]:
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


    result = run_pipeline_v2(
        tradable_frames=tradable_frames,
        leadership_frames=leadership_frames,
        bench_df=bench_df,
        breadth_df=breadth_df,
        market=cfg["market"],
        portfolio_params={
            "max_positions": cfg["max_positions"],
            "max_sector_weight": cfg["max_sector_weight"],
            "max_theme_names": cfg["max_theme_names"],
        },
    )
    result["market_config_v2"] = cfg
    result["tradable_universe"] = sorted(tradable)
    result["leadership_universe"] = sorted(leadership)
    skipped_table = result.get("skipped_table", pd.DataFrame())
    if isinstance(skipped_table, pd.DataFrame) and not skipped_table.empty:
        sample_cols = [c for c in ["ticker", "missing_critical_fields_v2"] if c in skipped_table.columns]
        logger.info(
            "Skipped symbols due to scoreability gate: count=%d sample=%s",
            len(skipped_table),
            skipped_table[sample_cols].head(20).to_dict(orient="records") if sample_cols else skipped_table.head(20).to_dict(orient="records"),
        )
    report = build_report_v2(result)
    result["report_v2"] = report
    result["report_text_v2"] = to_text_v2(report)
    logger.info(
        "Strategy result summary: processed=%s candidates=%s selected=%s action_counts=%s",
        report.get("processed_names"),
        report.get("candidate_count"),
        report.get("selected_count"),
        report.get("action_counts"),
    )
    return result



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
        logger.info("runner_v2 completed")
        if args.print_report and result.get("report_text_v2"):
            print(result["report_text_v2"])
        return result
    except Exception as exc:
        logger.exception("runner_v2 failed: %s", exc)
        raise



if __name__ == "__main__":
    main()

###############################################################
