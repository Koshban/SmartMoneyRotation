from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import numpy as np
import pandas as pd


@dataclass
class PortfolioSimConfig:
    market: str = 'US'
    initial_capital: float = 100000.0
    max_positions: int = 10
    rebalance_frequency: str = 'W-FRI'
    commission_bps: float = 5.0
    slippage_bps: float = 10.0
    output_dir: str = 'output/phase2_backtest/results'


BUY_BUCKETS = {'BUY', 'STRONG_BUY'}


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


def build_rank_snapshot(frames: Dict[str, pd.DataFrame], asof: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for ticker, df in frames.items():
        hist = df.loc[df.index <= asof]
        if hist.empty:
            continue
        last = hist.iloc[-1]
        rec = last.get('recommendation', None)
        score = last.get('composite_score', np.nan)
        close = last.get('close', np.nan)
        if rec in BUY_BUCKETS and pd.notna(score) and pd.notna(close):
            rows.append({'ticker': ticker, 'date': asof, 'recommendation': rec, 'composite_score': float(score), 'close': float(close)})
    snap = pd.DataFrame(rows)
    if snap.empty:
        return snap
    return snap.sort_values(['composite_score', 'ticker'], ascending=[False, True]).reset_index(drop=True)


def run_portfolio_sim(frames: Dict[str, pd.DataFrame], config: PortfolioSimConfig) -> dict:
    common_dates = sorted(set.intersection(*[set(df.index) for df in frames.values() if not df.empty]))
    common_dates = pd.DatetimeIndex(common_dates)
    rebalance_dates = set(pd.date_range(common_dates.min(), common_dates.max(), freq=config.rebalance_frequency))

    cash = config.initial_capital
    positions: dict[str, float] = {}
    trades = []
    equity_rows = []

    for dt in common_dates:
        px = {}
        for ticker, df in frames.items():
            if dt in df.index and 'close' in df.columns:
                px[ticker] = float(df.loc[dt, 'close'])

        if dt in rebalance_dates:
            snap = build_rank_snapshot(frames, dt)
            target = snap.head(config.max_positions)['ticker'].tolist() if not snap.empty else []

            for ticker in list(positions):
                if ticker not in target and ticker in px:
                    trade_value = positions[ticker] * px[ticker]
                    cost = trade_value * (config.commission_bps + config.slippage_bps) / 10000.0
                    cash += trade_value - cost
                    trades.append({'date': dt, 'ticker': ticker, 'side': 'SELL', 'price': px[ticker], 'shares': positions[ticker], 'gross_value': trade_value, 'cost': cost})
                    del positions[ticker]

            if target:
                total_equity = cash + sum(positions.get(t, 0.0) * px.get(t, 0.0) for t in positions)
                target_value = total_equity / len(target)
                for ticker in target:
                    if ticker not in px:
                        continue
                    current_value = positions.get(ticker, 0.0) * px[ticker]
                    delta_value = target_value - current_value
                    if delta_value <= 0:
                        continue
                    trade_cost = delta_value * (config.commission_bps + config.slippage_bps) / 10000.0
                    required_cash = delta_value + trade_cost
                    if required_cash > cash:
                        delta_value = cash / (1.0 + (config.commission_bps + config.slippage_bps) / 10000.0)
                        trade_cost = delta_value * (config.commission_bps + config.slippage_bps) / 10000.0
                        required_cash = delta_value + trade_cost
                    shares = delta_value / px[ticker] if px[ticker] > 0 else 0.0
                    if shares <= 0:
                        continue
                    cash -= required_cash
                    positions[ticker] = positions.get(ticker, 0.0) + shares
                    trades.append({'date': dt, 'ticker': ticker, 'side': 'BUY', 'price': px[ticker], 'shares': shares, 'gross_value': delta_value, 'cost': trade_cost})

        holdings_value = sum(positions.get(t, 0.0) * px.get(t, 0.0) for t in positions)
        equity = cash + holdings_value
        equity_rows.append({'date': dt, 'cash': cash, 'holdings_value': holdings_value, 'equity': equity, 'positions': len(positions)})

    equity_curve = pd.DataFrame(equity_rows)
    trades_df = pd.DataFrame(trades)
    equity_curve['daily_return'] = equity_curve['equity'].pct_change().fillna(0.0)
    running_max = equity_curve['equity'].cummax()
    equity_curve['drawdown'] = equity_curve['equity'] / running_max - 1.0

    years = max((equity_curve['date'].iloc[-1] - equity_curve['date'].iloc[0]).days / 365.25, 1e-9)
    cagr = (equity_curve['equity'].iloc[-1] / equity_curve['equity'].iloc[0]) ** (1 / years) - 1 if len(equity_curve) > 1 else np.nan
    vol = equity_curve['daily_return'].std() * np.sqrt(252)
    sharpe = (equity_curve['daily_return'].mean() * 252) / vol if vol and not np.isnan(vol) else np.nan

    metrics = pd.DataFrame([{
        'market': config.market,
        'initial_capital': config.initial_capital,
        'ending_equity': float(equity_curve['equity'].iloc[-1]) if not equity_curve.empty else np.nan,
        'cagr': float(cagr) if pd.notna(cagr) else np.nan,
        'annualized_vol': float(vol) if pd.notna(vol) else np.nan,
        'sharpe': float(sharpe) if pd.notna(sharpe) else np.nan,
        'max_drawdown': float(equity_curve['drawdown'].min()) if not equity_curve.empty else np.nan,
        'trade_count': int(len(trades_df)),
        'turnover_gross': float(trades_df['gross_value'].sum()) if not trades_df.empty else 0.0,
        'total_cost': float(trades_df['cost'].sum()) if not trades_df.empty else 0.0,
    }])

    outdir = Path(config.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    equity_curve.to_csv(outdir / f'portfolio_equity_{config.market.lower()}.csv', index=False)
    trades_df.to_csv(outdir / f'portfolio_trades_{config.market.lower()}.csv', index=False)
    metrics.to_csv(outdir / f'portfolio_metrics_{config.market.lower()}.csv', index=False)

    return {'equity_curve': equity_curve, 'trades': trades_df, 'metrics': metrics}


if __name__ == '__main__':
    raise SystemExit('Import this file and call load_from_parquet_files(...) to backtest parquet history.')
