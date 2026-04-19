import yfinance as yf
import pandas as pd
from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs

def clean(raw):
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw

# Pull test data
stock = clean(yf.download('AAPL', period='2y', progress=False))
bench = clean(yf.download('SPY',  period='2y', progress=False))

# Run indicators first (optional but realistic)
stock = compute_all_indicators(stock)

# Run relative strength
stock = compute_all_rs(stock, bench)

print(f'Final shape: {stock.shape}')
print()

# Show RS columns for last 5 days
rs_cols = [c for c in stock.columns if c.startswith('rs_')]
print('RS columns:', rs_cols)
print()
print(stock[rs_cols].tail(5).to_string())
print()

# Regime distribution
print('Regime distribution:')
print(stock['rs_regime'].value_counts().to_string())
