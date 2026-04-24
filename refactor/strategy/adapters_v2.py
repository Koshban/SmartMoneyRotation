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
