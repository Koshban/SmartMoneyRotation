import pandas as pd
import numpy as np

df = pd.read_parquet("data/us_options.parquet")
print("Checking > 32767")
print(df[['volume', 'oi']].describe())
print(df.loc[df['oi'] > 32767, ['symbol', 'expiry', 'strike', 'oi']].head(20))
print(df.loc[df['volume'] > 32767, ['symbol', 'expiry', 'strike', 'volume']].head(20))


# Check for inf in all numeric columns
print("Checking for inf")
for col in ['volume', 'oi', 'date', 'bid', 'ask', 'last', 'strike',
            'iv', 'delta', 'gamma', 'theta', 'vega', 'rho', 'underlying_price']:
    if col in df.columns:
        inf_count = np.isinf(df[col].dropna()).sum()
        if inf_count > 0:
            print(f"{col}: {inf_count} inf values")
            print(df.loc[np.isinf(df[col]), ['symbol', 'expiry', col]].head())

print("Checking for extreme values")
print(df[['volume', 'oi', 'date']].describe())
print(df.loc[df['volume'] > 2_147_483_647])
print(df.loc[df['oi'] > 2_147_483_647])