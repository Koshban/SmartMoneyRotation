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
