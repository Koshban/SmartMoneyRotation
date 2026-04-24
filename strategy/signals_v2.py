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
