import yfinance as yf
from cash.compute.indicators import compute_all_indicators

# Pull 1 year of SPY as test data
raw = yf.download('SPY', period='1y', progress=False)
raw.columns = [c.lower() for c in raw.columns]
print(f'Raw shape: {raw.shape}')

df = compute_all_indicators(raw)
print(f'After indicators: {df.shape}')
print()
print('Last row (tail 1):')
print(df.iloc[-1].to_string())
print()
print('Columns added:')
print([c for c in df.columns if c not in ['open','high','low','close','volume']])
