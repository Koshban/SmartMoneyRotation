import pandas as pd
from strategy.regime_v2 import classify_volatility_regime
from strategy.scoring_v2 import compute_composite_v2
from strategy.signals_v2 import apply_signals_v2, apply_convergence_v2

idx = pd.date_range('2025-01-01', periods=5, freq='D')
bench = pd.DataFrame({
    'close':[100,102,101,104,103],
    'high':[101,103,102,105,104],
    'low':[99,101,100,103,102],
}, index=idx)
reg = classify_volatility_regime(bench)

df = pd.DataFrame({
    'rszscore':[0.2,0.5,0.8,1.0,1.2],
    'sectrszscore':[0.1,0.3,0.4,0.6,0.7],
    'rsaccel20':[0.00,0.02,0.04,0.03,0.05],
    'closevsema30pct':[0.01,0.02,0.03,0.01,0.00],
    'closevssma50pct':[0.03,0.04,0.06,0.02,0.01],
    'relativevolume':[1.1,1.2,1.4,1.3,1.5],
    'obvslope10d':[0.01,0.02,0.03,0.02,0.04],
    'adlineslope10d':[0.01,0.01,0.02,0.01,0.02],
    'dollarvolume20d':[1e7,2e7,3e7,2.5e7,3.5e7],
    'atr14pct':[0.03,0.03,0.04,0.04,0.03],
    'amihud20':[0.002,0.002,0.003,0.003,0.002],
    'gaprate20':[0.05,0.06,0.08,0.07,0.06],
    'breadthscore':[0.55,0.60,0.62,0.58,0.65],
    'breadthregime':['neutral','neutral','strong','strong','strong'],
    'rsi14':[55,57,60,52,50],
    'adx14':[18,20,22,21,24],
    'rsregime':['improving','improving','leading','leading','leading'],
    'sectrsregime':['neutral','neutral','leading','leading','leading'],
    'rotationrec':['HOLD','BUY','BUY','BUY','STRONGBUY'],
}, index=idx).join(reg)

out = apply_convergence_v2(apply_signals_v2(compute_composite_v2(df)))
assert 'scoreadjusted_v2' in out.columns
assert out['scoreadjusted_v2'].between(0,1).all()
