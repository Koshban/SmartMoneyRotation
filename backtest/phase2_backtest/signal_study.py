from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional
import numpy as np
import pandas as pd


@dataclass
class SignalStudyConfig:
    market: str = 'US'
    horizons: tuple[int, ...] = (1, 3, 5, 10, 20)
    top_n: Optional[int] = None
    min_names_per_day: int = 10
    output_dir: str = 'output/phase2_backtest/results'


RECOMMENDATION_ORDER = ['SELL', 'HOLD', 'BUY', 'STRONG_BUY']


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
    df = df.sort_values('date')
    df = df.set_index('date')
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


def build_daily_snapshot(frames: Dict[str, pd.DataFrame], asof: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for ticker, df in frames.items():
        hist = df.loc[df.index <= asof].copy()
        if hist.empty:
            continue
        last = hist.iloc[-1]
        if 'close' not in hist.columns:
            continue
        composite = float(last.get('composite_score', np.nan))
        recommendation = last.get('recommendation', None)
        if pd.isna(composite) or recommendation is None:
            continue
        rows.append({
            'date': asof,
            'ticker': ticker,
            'recommendation': recommendation,
            'composite_score': composite,
            'score_percentile': float(last.get('score_percentile', np.nan)),
            'close': float(last['close']),
        })
    snap = pd.DataFrame(rows)
    if snap.empty:
        return snap
    if snap['score_percentile'].isna().all():
        snap['score_percentile'] = snap['composite_score'].rank(pct=True, method='average')
    return snap.sort_values(['composite_score', 'ticker'], ascending=[False, True]).reset_index(drop=True)


def compute_forward_returns(price_df: pd.DataFrame, horizons: Iterable[int]) -> pd.DataFrame:
    out = price_df.copy()
    for h in horizons:
        out[f'fwd_{h}d'] = out.groupby('ticker')['close'].shift(-h) / out['close'] - 1.0
    return out


def run_signal_study(frames: Dict[str, pd.DataFrame], config: SignalStudyConfig) -> dict:
    common_dates = sorted(set.intersection(*[set(df.index) for df in frames.values() if not df.empty]))
    common_dates = [pd.Timestamp(d) for d in common_dates]
    daily_snaps = []
    for dt in common_dates:
        snap = build_daily_snapshot(frames, dt)
        if snap.empty or len(snap) < config.min_names_per_day:
            continue
        if config.top_n is not None:
            snap = snap.head(config.top_n).copy()
        daily_snaps.append(snap)
    signal_panel = pd.concat(daily_snaps, ignore_index=True) if daily_snaps else pd.DataFrame()
    if signal_panel.empty:
        return {'signal_panel': signal_panel, 'bucket_summary': pd.DataFrame(), 'daily_spread': pd.DataFrame()}

    close_panel = pd.concat(
        [df[['close']].assign(ticker=t).reset_index(names='date') for t, df in frames.items() if 'close' in df.columns],
        ignore_index=True,
    )
    close_panel = compute_forward_returns(close_panel, config.horizons)
    merged = signal_panel.merge(close_panel, on=['date', 'ticker', 'close'], how='left')

    bucket_rows = []
    for rec in RECOMMENDATION_ORDER:
        part = merged[merged['recommendation'] == rec]
        if part.empty:
            continue
        row = {'recommendation': rec, 'count': int(len(part))}
        for h in config.horizons:
            col = f'fwd_{h}d'
            vals = part[col].dropna()
            row[f'{col}_mean'] = float(vals.mean()) if not vals.empty else np.nan
            row[f'{col}_median'] = float(vals.median()) if not vals.empty else np.nan
            row[f'{col}_hit_rate'] = float((vals > 0).mean()) if not vals.empty else np.nan
        bucket_rows.append(row)
    bucket_summary = pd.DataFrame(bucket_rows)

    daily_spread_rows = []
    for dt, part in merged.groupby('date'):
        buys = part[part['recommendation'] == 'STRONG_BUY']
        sells = part[part['recommendation'] == 'SELL']
        if buys.empty or sells.empty:
            continue
        row = {'date': dt}
        for h in config.horizons:
            col = f'fwd_{h}d'
            b = buys[col].dropna()
            s = sells[col].dropna()
            row[f'spread_{h}d'] = float(b.mean() - s.mean()) if (not b.empty and not s.empty) else np.nan
        daily_spread_rows.append(row)
    daily_spread = pd.DataFrame(daily_spread_rows)

    outdir = Path(config.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    signal_panel.to_csv(outdir / f'signal_panel_{config.market.lower()}.csv', index=False)
    bucket_summary.to_csv(outdir / f'signal_bucket_summary_{config.market.lower()}.csv', index=False)
    daily_spread.to_csv(outdir / f'signal_daily_spread_{config.market.lower()}.csv', index=False)

    return {
        'signal_panel': signal_panel,
        'bucket_summary': bucket_summary,
        'daily_spread': daily_spread,
    }


if __name__ == '__main__':
    raise SystemExit('Import this file and call load_from_parquet_files(...) to backtest parquet history.')
