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
