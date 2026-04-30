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