"""phase2/strategy/scoring_v2.py """

from __future__ import annotations
import numpy as np, pandas as pd
from common.config_refactor import SCORINGWEIGHTS_V2, SCORINGPARAMS_V2

def _s(x, lo, hi): return pd.Series(np.clip((x-lo)/(hi-lo), 0.0, 1.0), index=x.index)
def _inv(x, lo, hi): return 1.0 - _s(x, lo, hi)

def compute_composite_v2(df: pd.DataFrame) -> pd.DataFrame:
    p = SCORINGPARAMS_V2; w = SCORINGWEIGHTS_V2; out = df.copy()
    stock_rs = _s(out['rszscore'].fillna(0), -1.0, 2.0); sector_rs = _s(out.get('sectrszscore', pd.Series(0,index=out.index)).fillna(0), -1.0, 2.0); rs_accel = _s(out.get('rsaccel20', pd.Series(0,index=out.index)).fillna(0), -0.10, 0.15); trend_confirm = _s(out.get('closevsema30pct', pd.Series(0,index=out.index)).fillna(0), -0.03, 0.10)
    out['scoretrend'] = (p['trend']['w_stock_rs']*stock_rs + p['trend']['w_sector_rs']*sector_rs + p['trend']['w_rs_accel']*rs_accel + p['trend']['w_trend_confirm']*trend_confirm).clip(0,1)
    rvol = _s(out.get('relativevolume', pd.Series(1,index=out.index)).fillna(1), 0.8, 2.2); obv = _s(out.get('obvslope10d', pd.Series(0,index=out.index)).fillna(0), -0.05, 0.12); adl = _s(out.get('adlineslope10d', pd.Series(0,index=out.index)).fillna(0), -0.05, 0.12); dvol = _s(np.log1p(out.get('dollarvolume20d', pd.Series(0,index=out.index)).fillna(0)), 10, 18)
    out['scoreparticipation'] = (p['participation']['w_rvol']*rvol + p['participation']['w_obv']*obv + p['participation']['w_adline']*adl + p['participation']['w_dollar_volume']*dvol).clip(0,1)
    atrp = out.get('atr14pct', pd.Series(0,index=out.index)).fillna(0); illiq = out.get('amihud20', pd.Series(0,index=out.index)).fillna(0); gap = out.get('gaprate20', pd.Series(0,index=out.index)).fillna(0); extension = out.get('closevssma50pct', pd.Series(0,index=out.index)).fillna(0).abs()
    vol_pen = _inv(atrp, 0.02, p['penalties']['atrp_high']); liq_pen = _inv(illiq, 0.0, p['penalties']['illiquidity_bad']); gap_pen = _inv(gap, 0.05, 0.30); ext_pen = 1.0 - pd.Series(np.select([extension >= p['penalties']['extension_bad'], extension >= p['penalties']['extension_warn']], [1.0, 0.5], default=0.0), index=out.index)
    out['scorerisk'] = (p['risk']['w_vol_penalty']*vol_pen + p['risk']['w_liquidity_penalty']*liq_pen + p['risk']['w_gap_penalty']*gap_pen + p['risk']['w_extension_penalty']*ext_pen).clip(0,1)
    breadth = out.get('breadthscore', pd.Series(0.5,index=out.index)).fillna(0.5); volreg = out.get('volregimescore', pd.Series(0.0,index=out.index)).fillna(0.0)
    out['scoreregime'] = (p['regime']['w_breadth']*breadth + p['regime']['w_vol_regime']*(1.0-volreg)).clip(0,1)
    composite = w['trend']*out['scoretrend'] + w['participation']*out['scoreparticipation'] + w['risk']*out['scorerisk'] + w['regime']*out['scoreregime']
    rsi = out.get('rsi14', pd.Series(50,index=out.index)).fillna(50); adx = out.get('adx14', pd.Series(20,index=out.index)).fillna(20); rsi_low = p['penalties']['rsi_soft_low']; rsi_high = p['penalties']['rsi_soft_high']
    rsi_penalty = pd.Series(np.where(rsi < rsi_low, (rsi_low-rsi)/30.0, np.where(rsi > rsi_high, (rsi-rsi_high)/30.0, 0.0)), index=out.index).clip(0,0.15)
    adx_penalty = pd.Series(np.where(adx < p['penalties']['adx_soft_min'], (p['penalties']['adx_soft_min']-adx)/30.0, 0.0), index=out.index).clip(0,0.10)
    out['scorepenalty'] = (rsi_penalty + adx_penalty).clip(0,0.20); out['scorecomposite_v2'] = (composite - out['scorepenalty']).clip(0,1)
    return out


""" phase2/strategy/signals_v2.py """
from __future__ import annotations
import numpy as np, pandas as pd
from common.config_refactor import SIGNALPARAMS_V2, CONVERGENCEPARAMS_V2

def apply_signals_v2(df: pd.DataFrame) -> pd.DataFrame:
    p = SIGNALPARAMS_V2; out = df.copy()
    out['sig_vol_ok'] = ~out.get('volregime', pd.Series('calm', index=out.index)).isin(p['hard_block_vol_regimes'])
    out['sig_breadth_ok'] = ~out.get('breadthregime', pd.Series('unknown', index=out.index)).isin(p['hard_block_breadth_regimes'])
    out['sig_rs_ok'] = out.get('rsregime', pd.Series('unknown', index=out.index)).isin(p['allowed_rs_regimes'])
    out['sig_sector_ok'] = ~out.get('sectrsregime', pd.Series('unknown', index=out.index)).isin(p['blocked_sector_regimes'])
    vol_adj = out.get('volregime', pd.Series('calm', index=out.index)).map(p['regime_entry_adjustment']).fillna(0); breadth_adj = out.get('breadthregime', pd.Series('unknown', index=out.index)).map(p['breadth_entry_adjustment']).fillna(0)
    out['sigeffectiveentrymin_v2'] = p['base_entry_threshold'] + vol_adj + breadth_adj
    pullback_shape = (out.get('scoretrend', pd.Series(0,index=out.index)) >= p['pullback_min_trend']) & (out.get('closevsema30pct', pd.Series(0,index=out.index)).between(-0.05, p['pullback_max_short_extension'])) & (out.get('rsi14', pd.Series(50,index=out.index)) <= p['pullback_rsi_max'])
    continuation_shape = (out.get('scoretrend', pd.Series(0,index=out.index)) >= p['continuation_min_trend']) & (out.get('scoreparticipation', pd.Series(0,index=out.index)) >= 0.50)
    out['sig_setup_continuation'] = continuation_shape; out['sig_setup_pullback'] = pullback_shape & out.get('volregime', pd.Series('calm', index=out.index)).isin(['volatile','chaotic']); out['sig_setup_any'] = out['sig_setup_continuation'] | out['sig_setup_pullback']
    base_ok = out['sig_vol_ok'] & out['sig_breadth_ok'] & out['sig_rs_ok'] & out['sig_sector_ok'] & out['sig_setup_any']
    out['sigconfirmed_v2'] = (base_ok & (out['scorecomposite_v2'] >= out['sigeffectiveentrymin_v2'])).astype(int)
    size_mult = out.get('volregime', pd.Series('calm', index=out.index)).map(p['size_multipliers']).fillna(1.0); raw_size = 0.04 + 0.08 * ((out['scorecomposite_v2'] - p['base_entry_threshold']) / (1 - p['base_entry_threshold']))
    out['sigpositionpct_v2'] = np.where(out['sigconfirmed_v2'].eq(1), np.clip(raw_size, 0.0, 0.12) * size_mult, 0.0)
    out['sigexit_v2'] = ((out['scorecomposite_v2'] <= p['base_exit_threshold']) | (out.get('volregime', pd.Series('calm', index=out.index)) == 'chaotic') | (out.get('sectrsregime', pd.Series('unknown', index=out.index)) == 'lagging')).astype(int)
    return out

def apply_convergence_v2(df: pd.DataFrame) -> pd.DataFrame:
    p = CONVERGENCEPARAMS_V2; out = df.copy(); rotation_long = out.get('rotationrec', pd.Series('HOLD', index=out.index)).isin(['BUY','STRONGBUY','HOLD']); score_long = out.get('sigconfirmed_v2', pd.Series(0, index=out.index)).eq(1)
    labels = np.select([rotation_long & score_long, rotation_long & ~score_long, ~rotation_long & score_long, out.get('rotationrec', pd.Series('HOLD', index=out.index)).eq('CONFLICT')], ['aligned_long','rotation_long_only','score_long_only','mixed'], default='avoid')
    out['convergence_label_v2'] = labels; out['convergence_tier_v2'] = pd.Series(labels, index=out.index).map(p['tiers']).fillna(0)
    adj = out.get('volregime', pd.Series('calm', index=out.index)).map(p['adjustments']).fillna(0); boost = np.where(out['convergence_label_v2'] == 'aligned_long', adj, np.where(out['convergence_label_v2'] == 'mixed', -adj, 0.0))
    out['scoreadjusted_v2'] = (out['scorecomposite_v2'] + boost).clip(0,1)
    return out.sort_values(['convergence_tier_v2','scoreadjusted_v2'], ascending=[False,False])



""" phase2/strategy/portfolio_v2.py """
from __future__ import annotations
import numpy as np
import pandas as pd

VOL_MULT = {'calm': 1.00, 'volatile': 0.75, 'chaotic': 0.40}
BREADTH_EXPOSURE = {'strong': 1.00, 'neutral': 0.80, 'weak': 0.45, 'critical': 0.20, 'unknown': 0.80}


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
        if under.any() and excess > 0:
            w.loc[under] = w.loc[under] + excess * (w.loc[under] / w.loc[under].sum())
    return w / w.sum()


def build_portfolio_v2(latest: pd.DataFrame, max_positions: int = 8, max_sector_weight: float = 0.35, max_theme_names: int = 2) -> dict:
    df = latest.copy()
    if 'ticker' not in df.columns:
        df['ticker'] = df.index.astype(str)
    candidates = df[df['sigconfirmed_v2'].eq(1)].copy()
    if candidates.empty:
        return {'selected': pd.DataFrame(), 'meta': {'target_exposure': 0.0, 'reason': 'no confirmed names'}}

    breadth = str(candidates['breadthregime'].mode().iloc[0]) if 'breadthregime' in candidates.columns else 'unknown'
    vol = str(candidates['volregime'].mode().iloc[0]) if 'volregime' in candidates.columns else 'calm'
    target_exposure = BREADTH_EXPOSURE.get(breadth, 0.8) * VOL_MULT.get(vol, 1.0)

    candidates = candidates.sort_values(['convergence_tier_v2', 'scoreadjusted_v2'], ascending=[False, False])
    picks = []
    sector_weight = {}
    theme_count = {}

    for _, row in candidates.iterrows():
        if len(picks) >= max_positions:
            break
        sector = row.get('sector', 'Unknown')
        theme = row.get('theme', 'Unknown')
        if theme_count.get(theme, 0) >= max_theme_names:
            continue
        picks.append(row)
        theme_count[theme] = theme_count.get(theme, 0) + 1
        sector_weight[sector] = sector_weight.get(sector, 0) + 1

    selected = pd.DataFrame(picks)
    if selected.empty:
        return {'selected': pd.DataFrame(), 'meta': {'target_exposure': 0.0, 'reason': 'all candidates clipped by diversification'}}

    raw = selected['scoreadjusted_v2'] * selected['sigpositionpct_v2'].clip(lower=0.001)
    weights = _cap_weights(raw, min(0.20, max_sector_weight)) * target_exposure
    selected = selected.assign(target_weight=weights.values)

    for sector, idx in selected.groupby('sector').groups.items():
        sector_sum = selected.loc[idx, 'target_weight'].sum()
        if sector_sum > max_sector_weight:
            selected.loc[idx, 'target_weight'] *= max_sector_weight / sector_sum

    if selected['target_weight'].sum() > 0:
        selected['target_weight'] *= target_exposure / selected['target_weight'].sum()

    meta = {
        'target_exposure': float(target_exposure),
        'breadth_regime': breadth,
        'vol_regime': vol,
        'candidate_count': int(len(candidates)),
        'selected_count': int(len(selected)),
    }
    return {'selected': selected.sort_values('target_weight', ascending=False), 'meta': meta}


""" phase2/strategy/regime_v2.py """
from __future__ import annotations
import numpy as np, pandas as pd
from common.config_refactor import VOLREGIMEPARAMS

def _clip01(x): return np.clip(x, 0.0, 1.0)

def classify_volatility_regime(bench: pd.DataFrame, dispersion: pd.Series | None = None) -> pd.DataFrame:
    p = VOLREGIMEPARAMS; df = bench.copy(); close = df['close']; high = df.get('high', close); low = df.get('low', close); prev = close.shift(1)
    tr = pd.concat([(high-low).abs(), (high-prev).abs(), (low-prev).abs()], axis=1).max(axis=1)
    atrp = tr.rolling(p['atrp_window']).mean() / close
    rv = close.pct_change().rolling(p['realized_vol_window']).std() * np.sqrt(252)
    gap = ((close / prev - 1.0).abs() > 0.02).rolling(p['gap_window']).mean()
    dispersion = (pd.Series(index=df.index, data=np.nan) if dispersion is None else dispersion.reindex(df.index))
    atrp_s = _clip01((atrp - p['calm_atrp_max']) / (p['volatile_atrp_max'] - p['calm_atrp_max']))
    rv_s = _clip01((rv - p['calm_rvol_max']) / (p['volatile_rvol_max'] - p['calm_rvol_max']))
    gap_s = _clip01((gap - p['volatile_gap_rate']) / (p['chaotic_gap_rate'] - p['volatile_gap_rate']))
    disp_s = _clip01((dispersion - p['calm_dispersion_max']) / (p['volatile_dispersion_max'] - p['calm_dispersion_max']))
    w = p['score_weights']; score = w['atrp']*pd.Series(atrp_s,index=df.index).fillna(0)+w['realized_vol']*pd.Series(rv_s,index=df.index).fillna(0)+w['gap_rate']*pd.Series(gap_s,index=df.index).fillna(0)+w['dispersion']*pd.Series(disp_s,index=df.index).fillna(0)
    label = np.select([score >= 0.75, score >= 0.35], ['chaotic', 'volatile'], default='calm')
    return pd.DataFrame({'volregime': label, 'volregimescore': score.clip(0,1), 'atrp_bench': atrp, 'realizedvol_bench': rv, 'gaprate_bench': gap, 'dispersion_bench': dispersion}, index=df.index)

""" phase2/strategy/adapters_v2.py """
from __future__ import annotations
import numpy as np
import pandas as pd


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    defaults = {
        'rszscore': 0.0,
        'sectrszscore': 0.0,
        'rsaccel20': 0.0,
        'closevsema30pct': 0.0,
        'closevssma50pct': 0.0,
        'relativevolume': 1.0,
        'obvslope10d': 0.0,
        'adlineslope10d': 0.0,
        'dollarvolume20d': 0.0,
        'atr14pct': 0.03,
        'amihud20': 0.0,
        'gaprate20': 0.0,
        'breadthscore': 0.5,
        'breadthregime': 'unknown',
        'rsi14': 50.0,
        'adx14': 20.0,
        'rsregime': 'unknown',
        'sectrsregime': 'unknown',
        'rotationrec': 'HOLD',
        'ticker': 'UNKNOWN',
        'sector': 'Unknown',
        'theme': 'Unknown',
        'close': np.nan,
    }
    for col, val in defaults.items():
        if col not in out.columns:
            out[col] = val
    return out


def attach_benchmark_regime(stock_df: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    out = stock_df.copy()
    cols = ['volregime', 'volregimescore', 'atrp_bench', 'realizedvol_bench', 'gaprate_bench', 'dispersion_bench']
    aligned = regime_df[cols].reindex(out.index).ffill()
    return out.join(aligned, how='left')


def attach_breadth_context(stock_df: pd.DataFrame, breadth_df: pd.DataFrame | None) -> pd.DataFrame:
    if breadth_df is None or breadth_df.empty:
        return ensure_columns(stock_df)
    out = stock_df.copy()
    use_cols = [c for c in ['breadthscore', 'breadthregime'] if c in breadth_df.columns]
    if not use_cols:
        return ensure_columns(out)
    return ensure_columns(out.join(breadth_df[use_cols].reindex(out.index).ffill(), rsuffix='_breadth'))

