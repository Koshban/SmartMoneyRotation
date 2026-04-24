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
