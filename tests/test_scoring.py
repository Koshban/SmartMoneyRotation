import yfinance as yf
import pandas as pd
from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score

def clean(raw):
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [c.lower() for c in raw.columns]
    return raw

# Pull test data
stock = clean(yf.download('AAPL', period='2y', progress=False))
bench = clean(yf.download('SPY',  period='2y', progress=False))

# Step 1: Technical indicators
stock = compute_all_indicators(stock)

# Step 2: Relative strength vs benchmark
stock = compute_all_rs(stock, bench)

# Step 3: Composite score
stock = compute_composite_score(stock)

print(f'Final shape: {stock.shape}')
print()

# Show score columns for last 5 days
score_cols = [c for c in stock.columns if c.startswith('score_')]
print(stock[score_cols].tail(5).to_string())
print()

# Score distribution
comp = stock['score_composite'].dropna()
print(f'Composite stats:')
print(f'  mean:   {comp.mean():.4f}')
print(f'  std:    {comp.std():.4f}')
print(f'  min:    {comp.min():.4f}')
print(f'  max:    {comp.max():.4f}')
print(f'  latest: {comp.iloc[-1]:.4f}')
print()

# Regime breakdown with average composite score
print('Average composite by RS regime:')
grouped = stock.groupby('rs_regime')['score_composite'].mean()
print(grouped.sort_values(ascending=False).to_string())
