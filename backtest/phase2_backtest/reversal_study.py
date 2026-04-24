from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable
import numpy as np
import pandas as pd


@dataclass
class ReversalStudyConfig:
    market: str = 'US'
    horizons: tuple[int, ...] = (1, 3, 5, 10)
    allowed_statuses: tuple[str, ...] = ('TRIGGERED_REVERSAL', 'EARLY_REVERSAL_SIGNAL')
    output_dir: str = 'output/phase2_backtest/results'


QUALITY_ORDER = ['TOO_EARLY', 'EARLY', 'HIGH_QUALITY_BOUNCE', 'HIGH_RISK_HIGH_REWARD']


def load_universe_history(*args, **kwargs) -> Dict[str, pd.DataFrame]:
    raise NotImplementedError('Replace with your own loader or use the helper load_from_parquet_files(...) below.')


def _standardize_history_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    if 'date' not in df.columns and 'datetime' in df.columns:
        df = df.rename(columns={'datetime': 'date'})
    if 'date' not in df.columns:
        raise ValueError('Expected a date/datetime column in parquet data')
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').set_index('date')
    if 'symbol' not in df.columns:
        raise ValueError('Expected symbol column in parquet data')
    return df


def load_from_parquet_files(paths: list[str] | None = None, market: str = 'us') -> Dict[str, pd.DataFrame]:
    from pathlib import Path
    if paths is None:
        paths = [f'data/{market.lower()}_cash.parquet', 'data/universe_ohlcv.parquet']
    frames: Dict[str, pd.DataFrame] = {}
    seen = set()
    for raw in paths:
        path = Path(raw)
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        if df.empty:
            continue
        df = _standardize_history_frame(df)
        for sym, g in df.groupby('symbol'):
            if sym in seen:
                continue
            g = g.copy().sort_index()
            if 'close' not in g.columns and 'adj_close' in g.columns:
                g['close'] = g['adj_close']
            frames[sym] = g
            seen.add(sym)
    return frames


def build_reversal_panel(frames: Dict[str, pd.DataFrame], asof: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for ticker, df in frames.items():
        hist = df.loc[df.index <= asof]
        if hist.empty:
            continue
        last = hist.iloc[-1]
        status = last.get('status', None)
        quality = last.get('quality_label', None)
        if status is None or quality is None:
            continue
        rows.append({
            'date': asof,
            'ticker': ticker,
            'status': status,
            'quality_label': quality,
            'selling_exhaustion_score': float(last.get('selling_exhaustion_score', np.nan)),
            'reversal_trigger_score': float(last.get('reversal_trigger_score', np.nan)),
            'close': float(last.get('close', np.nan)),
        })
    return pd.DataFrame(rows)


def compute_forward_returns(price_df: pd.DataFrame, horizons: Iterable[int]) -> pd.DataFrame:
    out = price_df.copy()
    for h in horizons:
        out[f'fwd_{h}d'] = out.groupby('ticker')['close'].shift(-h) / out['close'] - 1.0
    return out


def run_reversal_study(frames: Dict[str, pd.DataFrame], config: ReversalStudyConfig) -> dict:
    common_dates = sorted(set.intersection(*[set(df.index) for df in frames.values() if not df.empty]))
    daily = [build_reversal_panel(frames, pd.Timestamp(dt)) for dt in common_dates]
    panel = pd.concat([x for x in daily if not x.empty], ignore_index=True) if daily else pd.DataFrame()
    if panel.empty:
        return {'reversal_panel': panel, 'quality_summary': pd.DataFrame()}

    close_panel = pd.concat(
        [df[['close']].assign(ticker=t).reset_index(names='date') for t, df in frames.items() if 'close' in df.columns],
        ignore_index=True,
    )
    close_panel = compute_forward_returns(close_panel, config.horizons)
    merged = panel.merge(close_panel, on=['date', 'ticker', 'close'], how='left')
    merged = merged[merged['status'].isin(config.allowed_statuses)].copy()

    summary_rows = []
    for quality in QUALITY_ORDER:
        part = merged[merged['quality_label'] == quality]
        if part.empty:
            continue
        row = {'quality_label': quality, 'count': int(len(part))}
        for h in config.horizons:
            col = f'fwd_{h}d'
            vals = part[col].dropna()
            row[f'{col}_mean'] = float(vals.mean()) if not vals.empty else np.nan
            row[f'{col}_median'] = float(vals.median()) if not vals.empty else np.nan
            row[f'{col}_hit_rate'] = float((vals > 0).mean()) if not vals.empty else np.nan
        summary_rows.append(row)
    quality_summary = pd.DataFrame(summary_rows)

    outdir = Path(config.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    merged.to_csv(outdir / f'reversal_panel_{config.market.lower()}.csv', index=False)
    quality_summary.to_csv(outdir / f'reversal_quality_summary_{config.market.lower()}.csv', index=False)

    return {'reversal_panel': merged, 'quality_summary': quality_summary}


if __name__ == '__main__':
    raise SystemExit('Import this file and call load_from_parquet_files(...) to backtest parquet history.')
