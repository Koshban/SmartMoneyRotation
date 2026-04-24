from __future__ import annotations
import pandas as pd
from .strategy.regime_v2 import classify_volatility_regime
from .strategy.scoring_v2 import compute_composite_v2
from .strategy.signals_v2 import apply_signals_v2, apply_convergence_v2
from .strategy.portfolio_v2 import build_portfolio_v2


def _classify_breadth_regime(breadth_df: pd.DataFrame | None) -> dict:
    if breadth_df is None or breadth_df.empty:
        return {'breadth_regime': 'unknown', 'breadthscore': None}
    row = breadth_df.iloc[-1]
    regime = row.get('breadthregime', row.get('breadth_regime', 'unknown'))
    score = row.get('breadthscore', row.get('breadth_score', None))
    return {'breadth_regime': regime, 'breadthscore': score}


def _build_leadership_snapshot(leadership_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for ticker, df in leadership_frames.items():
        if df is None or df.empty:
            continue
        row = df.iloc[-1].to_dict()
        row['ticker'] = ticker
        rows.append(row)
    snap = pd.DataFrame(rows)
    if snap.empty:
        return snap
    if 'rszscore' in snap.columns:
        snap = snap.sort_values('rszscore', ascending=False)
    return snap


def _normalize_leadership(snapshot: pd.DataFrame) -> pd.DataFrame:
    if snapshot.empty:
        return snapshot
    out = snapshot.copy()
    if 'rszscore' in out.columns:
        mn = float(out['rszscore'].min())
        mx = float(out['rszscore'].max())
        denom = max(mx - mn, 1e-9)
        out['leadership_strength'] = (out['rszscore'] - mn) / denom
    else:
        out['leadership_strength'] = 0.0
    return out


def _instrument_type(ticker: str) -> str:
    if '.' in ticker and (ticker.endswith('.HK') or ticker.endswith('.NS') or ticker.endswith('.BO')):
        return 'stock'
    etf_like = {'SPY','QQQ','IWM','DIA','MDY','XLK','XLF','XLE','XLV','XLI','XLC','XLY','XLP','XLU','XLRE','XLB','SOXX','SMH','XBI','IBB','IGV','SKYY','HACK','CIBR','BOTZ','AIQ','QTUM','FINX','TAN','ICLN','LIT','DRIV','URA','NLR','URNM','IBIT','BLOK','MTUM','ITA','ARKK','ARKG','KWEB','EEM','EFA','VWO','FXI','EWJ','EWZ','INDA','EWG','EWT','EWY','TLT','IEF','HYG','LQD','TIP','AGG','GLD','SLV','USO','UNG','DBA','DBC'}
    return 'etf' if ticker in etf_like else 'stock'


def _lookup_group_strength(row: pd.Series, leadership_snapshot: pd.DataFrame) -> float:
    if leadership_snapshot.empty:
        return 0.0
    ticker = row.get('ticker')
    sector = row.get('sector', 'Unknown')
    theme = row.get('theme', 'Unknown')
    direct = leadership_snapshot[leadership_snapshot['ticker'].eq(ticker)]
    if not direct.empty:
        return float(direct['leadership_strength'].max())
    theme_col = leadership_snapshot['theme'] if 'theme' in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    theme_match = leadership_snapshot[theme_col.eq(theme)]
    if not theme_match.empty:
        return float(theme_match['leadership_strength'].max())
    sector_col = leadership_snapshot['sector'] if 'sector' in leadership_snapshot.columns else pd.Series(index=leadership_snapshot.index, dtype=object)
    sector_match = leadership_snapshot[sector_col.eq(sector)]
    if not sector_match.empty:
        return float(sector_match['leadership_strength'].max())
    broad = leadership_snapshot[leadership_snapshot['ticker'].isin(['SPY', 'QQQ', 'IWM'])]
    if not broad.empty:
        return float(broad['leadership_strength'].mean())
    return 0.0


def _add_score_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out['score_percentile_v2'] = []
        return out
    score = out.get('scoreadjusted_v2', out.get('scorecomposite_v2')).rank(pct=True, method='average')
    out['score_percentile_v2'] = score.fillna(0.0)
    return out


def _generate_actions(df: pd.DataFrame) -> pd.DataFrame:
    out = _add_score_percentiles(df.copy())
    if out.empty:
        out['action_v2'] = []
        out['conviction_v2'] = []
        out['action_reason_v2'] = []
        out['action_sort_key_v2'] = []
        return out

    score = out.get('scoreadjusted_v2', out.get('scorecomposite_v2', pd.Series(0.0, index=out.index))).fillna(0.0)
    pct = out.get('score_percentile_v2', pd.Series(0.0, index=out.index)).fillna(0.0)
    entry = out.get('sigeffectiveentrymin_v2', pd.Series(0.60, index=out.index)).fillna(0.60)
    confirmed = out.get('sigconfirmed_v2', pd.Series(0, index=out.index)).fillna(0).astype(int)
    exit_sig = out.get('sigexit_v2', pd.Series(0, index=out.index)).fillna(0).astype(int)
    breadth = out.get('breadthregime', pd.Series('unknown', index=out.index)).fillna('unknown')
    vol = out.get('volregime', pd.Series('calm', index=out.index)).fillna('calm')
    leadership = out.get('leadership_strength', pd.Series(0.0, index=out.index)).fillna(0.0)
    rs_regime = out.get('rsregime', pd.Series('unknown', index=out.index)).fillna('unknown')
    sector_regime = out.get('sectrsregime', pd.Series('unknown', index=out.index)).fillna('unknown')
    rsi = out.get('rsi14', pd.Series(50.0, index=out.index)).fillna(50.0)
    adx = out.get('adx14', pd.Series(20.0, index=out.index)).fillna(20.0)
    relvol = out.get('relativevolume', pd.Series(1.0, index=out.index)).fillna(1.0)
    short_ext = out.get('closevsema30pct', pd.Series(0.0, index=out.index)).fillna(0.0)

    actions, reasons, convictions, sort_keys = [], [], [], []
    action_rank = {'STRONG_BUY': 4, 'BUY': 3, 'HOLD': 2, 'SELL': 1}

    for i in out.index:
        s = float(score.loc[i])
        p = float(pct.loc[i])
        e = float(entry.loc[i])
        c = int(confirmed.loc[i])
        x = int(exit_sig.loc[i])
        b = str(breadth.loc[i])
        v = str(vol.loc[i])
        l = float(leadership.loc[i])
        r = str(rs_regime.loc[i])
        sr = str(sector_regime.loc[i])
        rv = float(relvol.loc[i])
        ri = float(rsi.loc[i])
        ax = float(adx.loc[i])
        ext = float(short_ext.loc[i])

        strong_context = (b == 'strong' and v == 'calm') or l >= 0.60
        weak_context = b in {'weak', 'critical'} or v == 'chaotic' or sr == 'lagging'
        healthy_momentum = r in {'leading', 'improving'} and sr != 'lagging' and ri >= 52 and ax >= 22
        decent_momentum = r in {'leading', 'improving'} and ri >= 45 and ax >= 16
        overextended = ext >= 0.045 or ri >= 74

        if x == 1 and (s < max(0.50, e - 0.05) or p <= 0.20 or weak_context):
            action = 'SELL'
            reason = 'Exit condition active with weak relative rank or hostile regime'
        elif s < 0.50 or p <= 0.15:
            action = 'SELL'
            reason = 'Bottom-ranked score in the current market set'
        elif c == 1 and p >= 0.90 and s >= max(0.76, e + 0.08) and strong_context and healthy_momentum and rv >= 1.10 and not overextended:
            action = 'STRONG_BUY'
            reason = 'Top-decile score with confirmation, momentum, and supportive regime'
        elif c == 1 and p >= 0.65 and s >= max(0.62, e + 0.02) and decent_momentum and not weak_context:
            action = 'BUY'
            reason = 'Upper-tier score with confirmation and acceptable momentum'
        elif p >= 0.35 and s >= max(0.54, e - 0.06) and not weak_context:
            action = 'HOLD'
            reason = 'Mid-ranked score worth monitoring but not strong enough to buy'
        else:
            action = 'SELL'
            reason = 'Below hold band after percentile and regime adjustment'

        if p >= 0.90 or s >= 0.84:
            conviction = 'high'
        elif p >= 0.60 or s >= 0.68:
            conviction = 'medium'
        else:
            conviction = 'low'

        actions.append(action)
        reasons.append(reason)
        convictions.append(conviction)
        sort_keys.append(action_rank[action] * 10 + p + s / 10.0)

    out['action_v2'] = actions
    out['conviction_v2'] = convictions
    out['action_reason_v2'] = reasons
    out['action_sort_key_v2'] = sort_keys
    out = out.sort_values(['action_sort_key_v2', 'scoreadjusted_v2'], ascending=[False, False]).reset_index(drop=True)
    return out


def _build_review_table(action_table: pd.DataFrame) -> pd.DataFrame:
    if action_table.empty:
        return pd.DataFrame()
    review = action_table.rename(columns={
        'action_v2': 'recommendation',
        'scoreadjusted_v2': 'composite_score',
        'score_percentile_v2': 'score_percentile',
        'rsi14': 'rsi_14',
        'adx14': 'adx_14',
        'relativevolume': 'relative_volume',
        'closevsema30pct': 'price_vs_ema30_pct',
        'closevssma50pct': 'price_vs_sma50_pct',
        'rsaccel20': 'rs_accel_20',
        'gaprate20': 'gap_rate_20',
        'atr14pct': 'atr_14_pct',
    }).copy()

    review['overextended_flag'] = ((review.get('price_vs_ema30_pct', 0).fillna(0) >= 0.045) | (review.get('rsi_14', 50).fillna(50) >= 74)).map({True: 'YES', False: 'NO'})

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
        return ', '.join(parts)

    review['why_this_name'] = review.apply(why, axis=1)
    keep = [
        'ticker', 'recommendation', 'composite_score', 'score_percentile', 'rsi_14', 'adx_14',
        'relative_volume', 'price_vs_ema30_pct', 'price_vs_sma50_pct', 'rs_accel_20', 'atr_14_pct',
        'gap_rate_20', 'leadership_strength', 'overextended_flag', 'sector', 'theme',
        'breadthregime', 'volregime', 'rsregime', 'sectrsregime', 'instrument_type',
        'conviction_v2', 'action_reason_v2', 'why_this_name'
    ]
    cols = [c for c in keep if c in review.columns]
    return review[cols].copy()


def _build_selling_exhaustion_table(tradable_frames: dict[str, pd.DataFrame], breadth_regime: str, vol_regime: str, leadership_snapshot: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker, df in tradable_frames.items():
        if df is None or len(df) < 6:
            continue
        tail = df.tail(6).copy()
        rsi_col = tail['rsi14'] if 'rsi14' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        vol_col = tail['relativevolume'] if 'relativevolume' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        close_col = tail['close'] if 'close' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        high_col = tail['high'] if 'high' in tail.columns else close_col
        adx_col = tail['adx14'] if 'adx14' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        ext_col = tail['closevsema30pct'] if 'closevsema30pct' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        gap_col = tail['gaprate20'] if 'gaprate20' in tail.columns else pd.Series([None] * len(tail), index=tail.index)
        atr_col = tail['atr14pct'] if 'atr14pct' in tail.columns else pd.Series([None] * len(tail), index=tail.index)

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
        leadership = _lookup_group_strength(pd.Series({'ticker': ticker, 'sector': df.iloc[-1].get('sector', 'Unknown'), 'theme': df.iloc[-1].get('theme', 'Unknown')}), leadership_snapshot)
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

        trigger_score = 0
        trigger_score += 1 if rsi_turn_up else 0
        trigger_score += 1 if bullish_close else 0
        trigger_score += 1 if volume_reexpansion else 0
        trigger_score += 1 if close_above_prior_high else 0
        trigger_score += 1 if adx_stabilizing else 0

        exhaustion_score = 0
        exhaustion_score += 2 if oversold else 0
        exhaustion_score += 1 if rsi_down_streak >= 2 else 0
        exhaustion_score += 1 if weak_participation else 0
        exhaustion_score += 1 if down_streak >= 2 else 0
        exhaustion_score += 1 if weak_trend else 0
        exhaustion_score += 1 if stretched_down else 0
        exhaustion_score += 1 if fast_drop else 0

        if exhaustion_score < 4:
            continue

        if trigger_score >= 3:
            setup = 'TRIGGERED_REVERSAL'
        elif trigger_score >= 1:
            setup = 'EARLY_REVERSAL_SIGNAL'
        else:
            setup = 'WATCH_REVERSAL' if oversold and weak_participation else 'WEAK_SELLING'

        if trigger_score >= 4 and exhaustion_score >= 6:
            quality = 'HIGH_RISK_HIGH_REWARD'
        elif trigger_score >= 4 and exhaustion_score >= 4:
            quality = 'HIGH_QUALITY_BOUNCE'
        elif trigger_score >= 2:
            quality = 'EARLY'
        else:
            quality = 'TOO_EARLY'

        rows.append({
            'ticker': ticker,
            'instrument_type': _instrument_type(ticker),
            'status': setup,
            'quality_label': quality,
            'selling_exhaustion_score': exhaustion_score,
            'reversal_trigger_score': trigger_score,
            'rsi_14': last_rsi,
            'rsi_down_streak_3d': rsi_down_streak,
            'rsi_turn_up_1d': 'YES' if rsi_turn_up else 'NO',
            'close_down_streak_3d': down_streak,
            'bullish_close_1d': 'YES' if bullish_close else 'NO',
            'close_above_prior_high': 'YES' if close_above_prior_high else 'NO',
            'relative_volume': last_vol,
            'volume_down_streak_3d': vol_down_streak,
            'volume_reexpansion_1d': 'YES' if volume_reexpansion else 'NO',
            'adx_14': last_adx,
            'adx_stabilizing_1d': 'YES' if adx_stabilizing else 'NO',
            'price_5d_change': price_5d_change,
            'price_vs_ema30_pct': last_ext,
            'atr_14_pct': last_atr,
            'gap_rate_20': last_gap,
            'leadership_strength': leadership,
            'breadthregime': breadth_regime,
            'volregime': vol_regime,
            'sector': df.iloc[-1].get('sector', 'Unknown'),
            'theme': df.iloc[-1].get('theme', 'Unknown'),
            'decision_hint': 'Use only with confirmation; stronger when RSI turns up, price firms, and volume re-expands',
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(['reversal_trigger_score', 'selling_exhaustion_score', 'rsi_14', 'price_5d_change'], ascending=[False, False, True, True]).reset_index(drop=True)


def run_pipeline_v2(tradable_frames: dict[str, pd.DataFrame], bench_df: pd.DataFrame, breadth_df: pd.DataFrame | None = None, market: str = 'US', leadership_frames: dict[str, pd.DataFrame] | None = None, portfolio_params: dict | None = None) -> dict:
    regime_df = classify_volatility_regime(bench_df)
    breadth_info = _classify_breadth_regime(breadth_df)
    leadership_snapshot = _normalize_leadership(_build_leadership_snapshot(leadership_frames or {}))
    latest_rows = []
    last_vol = regime_df.iloc[-1]
    for ticker, df in tradable_frames.items():
        if df is None or df.empty:
            continue
        row = df.iloc[-1].to_dict()
        row['ticker'] = ticker
        row['instrument_type'] = _instrument_type(ticker)
        row['volregime'] = last_vol.get('volregime', 'calm')
        row['volregimescore'] = last_vol.get('volregimescore', 0.0)
        row['breadthregime'] = breadth_info.get('breadth_regime', 'unknown')
        row['breadthscore'] = breadth_info.get('breadthscore', 0.5)
        row['sector'] = row.get('sector', 'Unknown')
        row['theme'] = row.get('theme', 'Unknown')
        latest_rows.append(row)
    latest = pd.DataFrame(latest_rows) if latest_rows else pd.DataFrame()
    if not latest.empty:
        latest['leadership_strength'] = latest.apply(lambda row: _lookup_group_strength(row, leadership_snapshot), axis=1)
    scored = compute_composite_v2(latest) if not latest.empty else pd.DataFrame()
    if not scored.empty:
        scored['scorecomposite_v2'] = (scored['scorecomposite_v2'] + 0.10 * scored.get('leadership_strength', 0.0)).clip(0, 1)
    signaled = apply_signals_v2(scored) if not scored.empty else pd.DataFrame()
    converged = apply_convergence_v2(signaled) if not signaled.empty else pd.DataFrame()
    action_table = _generate_actions(converged) if not converged.empty else pd.DataFrame()
    review_table = _build_review_table(action_table) if not action_table.empty else pd.DataFrame()
    selling_exhaustion_table = _build_selling_exhaustion_table(
        tradable_frames,
        breadth_info.get('breadth_regime', 'unknown'),
        last_vol.get('volregime', 'unknown'),
        leadership_snapshot,
    )
    params = portfolio_params or {}
    buy_candidates = action_table[action_table['action_v2'].isin(['STRONG_BUY','BUY'])].copy() if not action_table.empty else pd.DataFrame()
    portfolio = build_portfolio_v2(
        buy_candidates,
        max_positions=params.get('max_positions', 8),
        max_sector_weight=params.get('max_sector_weight', 0.35),
        max_theme_names=params.get('max_theme_names', 2),
    ) if not buy_candidates.empty else {
        'selected': pd.DataFrame(),
        'meta': {
            'selected_count': 0,
            'candidate_count': 0,
            'target_exposure': 0.0,
            'breadth_regime': breadth_info.get('breadth_regime', 'unknown'),
            'vol_regime': last_vol.get('volregime', 'unknown'),
        },
    }
    return {
        'market': market,
        'latest': latest,
        'scored': scored,
        'signals': signaled,
        'converged': converged,
        'action_table': action_table,
        'review_table': review_table,
        'selling_exhaustion_table': selling_exhaustion_table,
        'portfolio': portfolio,
        'regime_df': regime_df,
        'breadth_info': breadth_info,
        'leadership_snapshot': leadership_snapshot,
    }
