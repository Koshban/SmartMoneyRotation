from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Dict

import pandas as pd

from signal_study import SignalStudyConfig, run_signal_study
from portfolio_sim import PortfolioSimConfig, run_portfolio_sim
from reversal_study import ReversalStudyConfig, run_reversal_study


MARKET_FILE_MAP = {
    'US': 'data/us_cash.parquet',
    'HK': 'data/hk_cash.parquet',
    'IN': 'data/india_cash.parquet',
    'INDIA': 'data/india_cash.parquet',
    'ALL': 'data/universe_ohlcv.parquet',
}


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c).lower().strip() for c in df.columns]
    renames = {'adj close': 'adj_close', 'trade_date': 'date'}
    df.rename(columns={k: v for k, v in renames.items() if k in df.columns}, inplace=True)
    if 'date' not in df.columns:
        raise ValueError('Parquet input must include a date column')
    if 'symbol' not in df.columns:
        raise ValueError('Parquet input must include a symbol column')
    df['date'] = pd.to_datetime(df['date'])
    keep = [c for c in df.columns if c in {'symbol', 'open', 'high', 'low', 'close', 'adj_close', 'volume'}]
    df = df[keep].copy()
    if 'close' not in df.columns and 'adj_close' in df.columns:
        df['close'] = df['adj_close']
    required = ['open', 'high', 'low', 'close', 'volume']
    for col in required:
        if col not in df.columns:
            raise ValueError(f'Missing required column: {col}')
    for col in required:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['open', 'high', 'low', 'close'])
    df = df[df['close'] > 0].copy()
    df['volume'] = df['volume'].fillna(0)
    return df.sort_values(['symbol', 'date'])


def _resolve_market_file(market: str, parquet_path: str | None) -> Path:
    if parquet_path:
        return Path(parquet_path)
    key = market.upper()
    if key not in MARKET_FILE_MAP:
        raise ValueError(f'Unsupported market: {market}')
    return Path(MARKET_FILE_MAP[key])


def load_market_history(market: str, start_date: str, end_date: str, parquet_path: str | None = None) -> Dict[str, pd.DataFrame]:
    path = _resolve_market_file(market, parquet_path)
    if not path.exists():
        raise FileNotFoundError(f'Parquet file not found: {path}')
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    if end_ts < start_ts:
        raise ValueError('end_date must be on or after start_date')

    raw = pd.read_parquet(path)
    raw = _normalize_ohlcv(raw)
    raw = raw[(raw['date'] >= start_ts) & (raw['date'] <= end_ts)].copy()

    frames: Dict[str, pd.DataFrame] = {}
    for symbol, g in raw.groupby('symbol'):
        hist = g.drop(columns=['symbol']).copy()
        hist = hist.set_index('date').sort_index()
        hist = hist[~hist.index.duplicated(keep='last')]
        if not hist.empty:
            frames[str(symbol)] = hist
    return frames


def main() -> None:
    parser = argparse.ArgumentParser(description='Run sandbox backtests from parquet history over a specified date range.')
    parser.add_argument('--market', required=True, help='Market code: US, HK, IN, INDIA, or ALL')
    parser.add_argument('--start-date', required=True, help='Inclusive start date, e.g. 2024-01-01')
    parser.add_argument('--end-date', required=True, help='Inclusive end date, e.g. 2025-12-31')
    parser.add_argument('--parquet-path', default=None, help='Optional explicit parquet file path override')
    parser.add_argument('--run-signal-study', action='store_true', help='Run recommendation bucket signal study')
    parser.add_argument('--run-portfolio-sim', action='store_true', help='Run portfolio simulation')
    parser.add_argument('--run-reversal-study', action='store_true', help='Run reversal study')
    parser.add_argument('--max-positions', type=int, default=10)
    parser.add_argument('--rebalance-frequency', default='W-FRI')
    parser.add_argument('--initial-capital', type=float, default=100000.0)
    parser.add_argument('--output-dir', default='output/phase2_backtest/results')
    args = parser.parse_args()

    if not any([args.run_signal_study, args.run_portfolio_sim, args.run_reversal_study]):
        args.run_signal_study = True
        args.run_portfolio_sim = True
        args.run_reversal_study = True

    frames = load_market_history(args.market, args.start_date, args.end_date, args.parquet_path)
    if not frames:
        raise SystemExit('No data found in the selected date range')

    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    manifest = {
        'market': args.market.upper(),
        'start_date': args.start_date,
        'end_date': args.end_date,
        'parquet_path': str(_resolve_market_file(args.market, args.parquet_path)),
        'tickers_loaded': len(frames),
    }
    pd.DataFrame([manifest]).to_csv(outdir / f'run_manifest_{args.market.lower()}.csv', index=False)

    if args.run_signal_study:
        signal_cfg = SignalStudyConfig(market=args.market.upper(), output_dir=args.output_dir)
        run_signal_study(frames, signal_cfg)
        pd.DataFrame([asdict(signal_cfg)]).to_csv(outdir / f'signal_config_{args.market.lower()}.csv', index=False)

    if args.run_portfolio_sim:
        portfolio_cfg = PortfolioSimConfig(
            market=args.market.upper(),
            initial_capital=args.initial_capital,
            max_positions=args.max_positions,
            rebalance_frequency=args.rebalance_frequency,
            output_dir=args.output_dir,
        )
        run_portfolio_sim(frames, portfolio_cfg)
        pd.DataFrame([asdict(portfolio_cfg)]).to_csv(outdir / f'portfolio_config_{args.market.lower()}.csv', index=False)

    if args.run_reversal_study:
        reversal_cfg = ReversalStudyConfig(market=args.market.upper(), output_dir=args.output_dir)
        run_reversal_study(frames, reversal_cfg)
        pd.DataFrame([asdict(reversal_cfg)]).to_csv(outdir / f'reversal_config_{args.market.lower()}.csv', index=False)


if __name__ == '__main__':
    main()
