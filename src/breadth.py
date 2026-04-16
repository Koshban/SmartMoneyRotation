# breadth.py
"""
Theme-level breadth and correlation metrics.
These are computed across the ticker group, not per-ticker.
"""

import numpy as np
import pandas as pd
from typing import Dict, List

from config import UNIVERSE, INTRA_CORR_WINDOW


def theme_breadth(
    data: Dict[str, pd.DataFrame], universe: dict = UNIVERSE
) -> Dict[str, pd.DataFrame]:
    """
    For each theme, compute daily:
      - pct_above_20d : fraction of constituents with close > 20-day SMA
      - pct_above_50d : fraction of constituents with close > 50-day SMA
      - adv_decline    : (advances - declines) / total  (1-day change)
    Returns {theme_name: DataFrame}.
    """
    result = {}
    for theme, tickers in universe.items():
        valid = [t for t in tickers if t in data]
        if len(valid) < 2:
            continue

        # Build a common date index (inner join)
        common_idx = data[valid[0]].index
        for t in valid[1:]:
            common_idx = common_idx.intersection(data[t].index)
        if len(common_idx) < 50:
            continue

        above_20 = pd.Series(0.0, index=common_idx)
        above_50 = pd.Series(0.0, index=common_idx)
        advances = pd.Series(0.0, index=common_idx)
        n = len(valid)

        for t in valid:
            c = data[t]["Close"].reindex(common_idx)
            above_20 += (c > c.rolling(20).mean()).astype(float)
            above_50 += (c > c.rolling(50).mean()).astype(float)
            advances += (c.diff() > 0).astype(float)

        result[theme] = pd.DataFrame(
            {
                "pct_above_20d": above_20 / n,
                "pct_above_50d": above_50 / n,
                "adv_decline": (2 * advances - n) / n,   # ranges -1 to +1
            },
            index=common_idx,
        )

    return result


def intra_theme_correlation(
    data: Dict[str, pd.DataFrame],
    universe: dict = UNIVERSE,
    window: int = INTRA_CORR_WINDOW,
) -> Dict[str, pd.Series]:
    """
    Average pairwise rolling correlation of daily returns within each theme.
    High values (>0.7) signal systematic/thematic rotation.
    Computed only for the most recent `window` days for efficiency.
    """
    result = {}
    for theme, tickers in universe.items():
        valid = [t for t in tickers if t in data]
        if len(valid) < 3:
            continue

        # Build returns matrix on common index
        common_idx = data[valid[0]].index
        for t in valid[1:]:
            common_idx = common_idx.intersection(data[t].index)

        rets = pd.DataFrame(
            {t: data[t]["Close"].reindex(common_idx).pct_change() for t in valid}
        ).dropna()

        if len(rets) < window:
            continue

        # Rolling mean pairwise correlation
        def _avg_pairwise(df_window):
            cm = df_window.corr()
            mask = np.triu(np.ones_like(cm, dtype=bool), k=1)
            vals = cm.values[mask]
            return np.nanmean(vals) if len(vals) > 0 else np.nan

        corr_series = rets.rolling(window).apply(
            lambda _: _avg_pairwise(rets.loc[_[: window if False else None].index]),  # placeholder
            raw=False,
        )

        # More efficient approach: compute once for latest window
        recent = rets.iloc[-window:]
        cm = recent.corr()
        mask = np.triu(np.ones_like(cm, dtype=bool), k=1)
        avg_corr_latest = float(np.nanmean(cm.values[mask]))

        # Store a simple series with the latest value broadcast
        # For a full rolling version, the loop below is cleaner:
        rolling_vals = []
        for i in range(window, len(rets) + 1):
            chunk = rets.iloc[i - window : i]
            cm = chunk.corr()
            mask = np.triu(np.ones_like(cm, dtype=bool), k=1)
            rolling_vals.append(float(np.nanmean(cm.values[mask])))

        result[theme] = pd.Series(
            rolling_vals,
            index=rets.index[window - 1 :],
            name=f"{theme}_intra_corr",
        )

    return result