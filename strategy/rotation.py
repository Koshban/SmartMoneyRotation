"""
strategy/rotation.py
------------------
Core Smart Money Rotation engine.

Flow
====
  1. Compute composite relative strength (RS) for each sector ETF vs SPY
  2. Rank the 11 GICS sectors → Leading (top 3), Neutral, Lagging (bottom 3)
  3. Within each Leading sector, rank US single names by composite RS
  4. Select the top N stocks per Leading sector → BUY candidates
  5. Evaluate every current holding against sell rules → SELL / REDUCE / HOLD
  6. Enforce max-position cap, combine, and return RotationResult

Relative Strength
=================
  RS_composite(ticker) = Σ  weight_i × (ret_i(ticker) − ret_i(benchmark))

  Default weights:
      40 %  ×  21-day return   (≈ 1 month)   — recent momentum
      35 %  ×  63-day return   (≈ 3 months)
      25 %  × 126-day return   (≈ 6 months)  — persistence filter

Sell Rules (in priority order)
==============================
  1. Sector moved to Lagging (bottom 3)       → SELL
  2. Sector moved to Neutral (middle 5)        → REDUCE  (half position)
  3. Individual RS below threshold              → SELL
  4. None of the above                          → HOLD

All tunable knobs live in the RotationConfig dataclass.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum

import pandas as pd

from common.sector_map import (
    SECTOR_ETFS,
    get_sector,
    get_sector_or_class,
    get_us_tickers_for_sector,
)

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class RotationConfig:
    """Every tunable knob for the rotation engine."""

    # ── Benchmark ──────────────────────────────────────────────
    benchmark: str = "SPY"

    # ── Sector tier sizing ─────────────────────────────────────
    n_leading_sectors: int = 3
    n_lagging_sectors: int = 3      # bottom N → full SELL

    # ── RS lookback periods (trading days) and weights ─────────
    # Must sum to 1.0.
    rs_periods: dict[int, float] = field(default_factory=lambda: {
        21:  0.40,    # ≈ 1 month
        63:  0.35,    # ≈ 3 months
        126: 0.25,    # ≈ 6 months
    })

    # ── Stock selection within leading sectors ─────────────────
    stocks_per_sector: int = 3
    min_rs_score: float = 0.0       # floor RS to be BUY-eligible
    prefer_positive_rs_vs_sector: bool = True   # must also beat sector ETF

    # ── Sell thresholds ────────────────────────────────────────
    sell_if_sector_not_leading: bool = True
    sell_individual_rs_below: float = -0.05     # hard floor on individual RS

    # ── Portfolio constraints ──────────────────────────────────
    max_total_positions: int = 12
    max_per_sector: int = 4         # including sector ETF fallback

    # ── Data requirements ──────────────────────────────────────
    min_history_days: int = 130     # must have ≥ this many rows


# ═══════════════════════════════════════════════════════════════
#  ENUMS & DATA CLASSES
# ═══════════════════════════════════════════════════════════════

class Action(str, Enum):
    BUY    = "BUY"
    SELL   = "SELL"
    HOLD   = "HOLD"
    REDUCE = "REDUCE"          # sector drifted to Neutral → trim


@dataclass
class SectorScore:
    """One row in the sector ranking table."""
    sector: str
    etf: str
    composite_rs: float
    rank: int                  # 1 = strongest
    tier: str                  # Leading / Neutral / Lagging
    period_returns: dict[int, float] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (f"  {self.rank:2d}. {self.sector:28s} [{self.etf:4s}]  "
                f"RS {self.composite_rs:+.4f}  ({self.tier})")


@dataclass
class Recommendation:
    """A single actionable signal."""
    ticker: str
    action: Action
    sector: str               # GICS sector or asset-class label
    sector_rank: int           # 1–11 (99 if not a sector ticker)
    sector_tier: str           # Leading / Neutral / Lagging / n/a
    rs_composite: float        # ticker RS vs SPY
    rs_vs_sector_etf: float    # ticker RS vs its own sector ETF
    reason: str

    def __repr__(self) -> str:
        return (f"  {self.action.value:6s}  {self.ticker:8s}  │  "
                f"{self.sector:24s} (rank {self.sector_rank:2d}, {self.sector_tier:8s})  │  "
                f"RS {self.rs_composite:+.4f}  vs-sector {self.rs_vs_sector_etf:+.4f}  │  "
                f"{self.reason}")


@dataclass
class RotationResult:
    """Complete output of one rotation run."""
    as_of_date: date
    config: RotationConfig
    sector_rankings: list[SectorScore]
    recommendations: list[Recommendation]

    # ── convenience filters ────────────────────────────────────
    @property
    def leading_sectors(self) -> list[str]:
        return [s.sector for s in self.sector_rankings if s.tier == "Leading"]

    @property
    def lagging_sectors(self) -> list[str]:
        return [s.sector for s in self.sector_rankings if s.tier == "Lagging"]

    @property
    def buys(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.BUY]

    @property
    def sells(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.SELL]

    @property
    def reduces(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.REDUCE]

    @property
    def holds(self) -> list[Recommendation]:
        return [r for r in self.recommendations if r.action == Action.HOLD]


# ═══════════════════════════════════════════════════════════════
#  RELATIVE-STRENGTH MATH
# ═══════════════════════════════════════════════════════════════

def _period_returns(prices: pd.DataFrame, period: int) -> pd.Series:
    """
    Percentage return over the last `period` trading days
    for every column in the price matrix.
    """
    if len(prices) < period:
        return pd.Series(dtype=float)
    current = prices.iloc[-1]
    past = prices.iloc[-period]
    safe = past.replace(0, float("nan"))
    return (current - past) / safe


def composite_rs_all(
    prices: pd.DataFrame,
    config: RotationConfig,
) -> tuple[pd.Series, dict[str, dict[int, float]]]:
    """
    Compute composite RS (vs benchmark) for every ticker in the matrix.

    Returns
    -------
    rs_series : Series[ticker → float], sorted descending.
    raw_returns : dict[ticker → {period: return}] for drill-down.
    """
    bench = config.benchmark
    if bench not in prices.columns:
        raise ValueError(f"Benchmark '{bench}' missing from price data.")

    tickers = [c for c in prices.columns if c != bench]
    composite = pd.Series(0.0, index=tickers)
    raw_returns: dict[str, dict[int, float]] = {t: {} for t in tickers}

    for period, weight in config.rs_periods.items():
        rets = _period_returns(prices, period)
        if rets.empty:
            log.warning("Not enough data for %d-day lookback, skipping.", period)
            continue
        bench_ret = rets.get(bench, 0.0)
        for t in tickers:
            t_ret = rets.get(t, float("nan"))
            raw_returns[t][period] = t_ret if pd.notna(t_ret) else 0.0
            excess = (t_ret - bench_ret) if pd.notna(t_ret) else 0.0
            composite[t] += weight * excess

    return composite.sort_values(ascending=False), raw_returns


def _rs_vs(
    prices: pd.DataFrame,
    ticker: str,
    versus: str,
    config: RotationConfig,
) -> float:
    """Single ticker's composite RS vs an arbitrary benchmark."""
    if ticker not in prices.columns or versus not in prices.columns:
        return 0.0
    score = 0.0
    for period, weight in config.rs_periods.items():
        rets = _period_returns(prices, period)
        if rets.empty:
            continue
        t_ret = rets.get(ticker, 0.0)
        b_ret = rets.get(versus, 0.0)
        t_ret = t_ret if pd.notna(t_ret) else 0.0
        b_ret = b_ret if pd.notna(b_ret) else 0.0
        score += weight * (t_ret - b_ret)
    return score


# ═══════════════════════════════════════════════════════════════
#  STEP 1 — RANK SECTORS
# ═══════════════════════════════════════════════════════════════

def _rank_sectors(
    prices: pd.DataFrame,
    config: RotationConfig,
) -> list[SectorScore]:
    """
    Score each sector ETF by composite RS vs SPY, assign rank & tier.
    """
    rs_all, raw = composite_rs_all(prices, config)

    scored: list[SectorScore] = []
    for sector, etf in SECTOR_ETFS.items():
        scored.append(SectorScore(
            sector=sector,
            etf=etf,
            composite_rs=rs_all.get(etf, 0.0),
            rank=0,
            tier="",
            period_returns=raw.get(etf, {}),
        ))

    scored.sort(key=lambda s: s.composite_rs, reverse=True)

    n_lead = config.n_leading_sectors
    n_lag = config.n_lagging_sectors
    for i, s in enumerate(scored):
        s.rank = i + 1
        if i < n_lead:
            s.tier = "Leading"
        elif i >= len(scored) - n_lag:
            s.tier = "Lagging"
        else:
            s.tier = "Neutral"

    return scored


# ═══════════════════════════════════════════════════════════════
#  STEP 2 — PICK STOCKS IN LEADING SECTORS
# ═══════════════════════════════════════════════════════════════

def _pick_stocks(
    leading: list[SectorScore],
    prices: pd.DataFrame,
    config: RotationConfig,
    rs_all: pd.Series,
) -> list[Recommendation]:
    """
    For each leading sector, rank US single names by RS and return
    the top N as BUY recommendations.

    Falls back to the sector ETF itself when no single names have data.
    """
    buys: list[Recommendation] = []

    for ss in leading:
        sector = ss.sector
        etf = ss.etf
        candidates = get_us_tickers_for_sector(sector)

        # keep only those present in the price matrix
        available = [t for t in candidates if t in prices.columns]

        if not available:
            # fallback → buy the sector ETF
            log.info("No single-name data for %s — falling back to %s", sector, etf)
            buys.append(Recommendation(
                ticker=etf,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs_all.get(etf, 0.0),
                rs_vs_sector_etf=0.0,
                reason=f"Sector ETF fallback (no single-name data for {sector})",
            ))
            continue

        # score each candidate
        scored: list[tuple[str, float, float]] = []
        for t in available:
            rs = rs_all.get(t, 0.0)
            rs_vs_etf = _rs_vs(prices, t, etf, config)

            # eligibility gates
            if rs < config.min_rs_score:
                continue
            if config.prefer_positive_rs_vs_sector and rs_vs_etf < 0:
                continue

            scored.append((t, rs, rs_vs_etf))

        # sort by composite RS descending
        scored.sort(key=lambda x: x[1], reverse=True)

        picks = scored[: config.stocks_per_sector]

        if not picks:
            # all candidates below threshold → fall back to ETF
            log.info("All candidates in %s below RS threshold — using %s", sector, etf)
            buys.append(Recommendation(
                ticker=etf,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs_all.get(etf, 0.0),
                rs_vs_sector_etf=0.0,
                reason=f"Sector ETF fallback (no candidates above RS threshold)",
            ))
            continue

        for ticker, rs, rs_vs_etf in picks:
            buys.append(Recommendation(
                ticker=ticker,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs,
                rs_vs_sector_etf=rs_vs_etf,
                reason=f"Top RS in leading sector {sector} (rank {ss.rank})",
            ))

    return buys


# ═══════════════════════════════════════════════════════════════
#  STEP 3 — EVALUATE CURRENT HOLDINGS
# ═══════════════════════════════════════════════════════════════

def _evaluate_holdings(
    holdings: list[str],
    sector_scores: list[SectorScore],
    prices: pd.DataFrame,
    config: RotationConfig,
    rs_all: pd.Series,
) -> list[Recommendation]:
    """
    Walk every current holding through the sell-rule waterfall.

    Priority
    --------
    1.  Sector → Lagging         → SELL
    2.  Sector → Neutral         → REDUCE
    3.  Individual RS too weak   → SELL
    4.  Otherwise                → HOLD
    """
    tier_map = {s.sector: s.tier for s in sector_scores}
    rank_map = {s.sector: s.rank for s in sector_scores}
    leading_set = {s.sector for s in sector_scores if s.tier == "Leading"}

    recs: list[Recommendation] = []

    for ticker in holdings:
        sector = get_sector(ticker)
        label = get_sector_or_class(ticker)

        sector_etf = SECTOR_ETFS.get(sector, config.benchmark) if sector else config.benchmark
        rs = rs_all.get(ticker, 0.0)
        rs_vs_etf = _rs_vs(prices, ticker, sector_etf, config)
        s_rank = rank_map.get(sector, 99) if sector else 99
        s_tier = tier_map.get(sector, "n/a") if sector else "n/a"

        # ── Rule 1 & 2: sector drift ──────────────────────────
        if config.sell_if_sector_not_leading and sector and sector not in leading_set:
            if s_tier == "Lagging":
                recs.append(Recommendation(
                    ticker=ticker, action=Action.SELL,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} is Lagging (rank {s_rank}/11)",
                ))
            else:
                recs.append(Recommendation(
                    ticker=ticker, action=Action.REDUCE,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} drifted to Neutral (rank {s_rank}/11)",
                ))
            continue

        # ── Rule 3: individual RS collapse ─────────────────────
        if rs < config.sell_individual_rs_below:
            recs.append(Recommendation(
                ticker=ticker, action=Action.SELL,
                sector=label, sector_rank=s_rank, sector_tier=s_tier,
                rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                reason=(f"Individual RS {rs:+.3f} below floor "
                        f"({config.sell_individual_rs_below:+.3f})"),
            ))
            continue

        # ── Rule 4: everything fine ────────────────────────────
        recs.append(Recommendation(
            ticker=ticker, action=Action.HOLD,
            sector=label, sector_rank=s_rank, sector_tier=s_tier,
            rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
            reason=f"Sector {sector} still Leading (rank {s_rank}), RS OK",
        ))

    return recs


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_rotation(
    prices: pd.DataFrame,
    current_holdings: list[str] | None = None,
    config: RotationConfig | None = None,
) -> RotationResult:
    """
    Run the full Smart Money Rotation.

    Parameters
    ----------
    prices : DataFrame
        DatetimeIndex, columns = tickers, values = adjusted close.
        Must include the benchmark, the 11 sector ETFs, and any
        single names you want considered.  Missing tickers are
        silently skipped.

    current_holdings : list[str] or None
        Tickers currently in the portfolio.  Pass [] or None for
        a fresh start (only BUY signals emitted).

    config : RotationConfig or None
        Override any defaults.  None → use RotationConfig().

    Returns
    -------
    RotationResult
        .sector_rankings  — ordered list of SectorScore
        .recommendations  — ordered list of Recommendation
        .buys / .sells / .reduces / .holds — convenience accessors
    """
    if config is None:
        config = RotationConfig()
    if current_holdings is None:
        current_holdings = []

    # ── Validate data depth ────────────────────────────────────
    if len(prices) < config.min_history_days:
        raise ValueError(
            f"Need ≥ {config.min_history_days} rows of price data, "
            f"got {len(prices)}."
        )

    as_of = (prices.index[-1].date()
             if hasattr(prices.index[-1], "date")
             else prices.index[-1])

    log.info("Rotation as-of %s  |  %d days × %d tickers",
             as_of, prices.shape[0], prices.shape[1])

    # ── Step 1: rank sectors ───────────────────────────────────
    sector_rankings = _rank_sectors(prices, config)
    leading = [s for s in sector_rankings if s.tier == "Leading"]

    log.info("Leading : %s", [s.sector for s in leading])
    log.info("Lagging : %s", [s.sector for s in sector_rankings if s.tier == "Lagging"])

    # Pre-compute RS for all tickers once (reused everywhere)
    rs_all, _ = composite_rs_all(prices, config)

    # ── Step 2: pick stocks in leading sectors ─────────────────
    raw_buys = _pick_stocks(leading, prices, config, rs_all)

    # Remove tickers already held (they'll appear as HOLD instead)
    held_set = set(current_holdings)
    new_buys = [r for r in raw_buys if r.ticker not in held_set]

    # ── Step 3: evaluate current holdings ──────────────────────
    hold_sell = _evaluate_holdings(
        holdings=current_holdings,
        sector_scores=sector_rankings,
        prices=prices,
        config=config,
        rs_all=rs_all,
    )

    # ── Step 4: enforce max-position cap ───────────────────────
    n_keeping = sum(1 for r in hold_sell if r.action in (Action.HOLD, Action.REDUCE))
    open_slots = max(0, config.max_total_positions - n_keeping)
    new_buys = new_buys[:open_slots]

    # ── Step 5: combine & sort ─────────────────────────────────
    # Order: SELL first (liquidate), then REDUCE, then BUY (deploy),
    # then HOLD.  Within each group, sort by RS descending.
    _action_order = {Action.SELL: 0, Action.REDUCE: 1, Action.BUY: 2, Action.HOLD: 3}

    all_recs = hold_sell + new_buys
    all_recs.sort(key=lambda r: (_action_order[r.action], -r.rs_composite))

    result = RotationResult(
        as_of_date=as_of,
        config=config,
        sector_rankings=sector_rankings,
        recommendations=all_recs,
    )

    log.info("Result  : %d SELL  %d REDUCE  %d BUY  %d HOLD",
             len(result.sells), len(result.reduces),
             len(result.buys), len(result.holds))

    return result


# ═══════════════════════════════════════════════════════════════
#  DATABASE CONVENIENCE  (adapt to your data layer)
# ═══════════════════════════════════════════════════════════════

def load_price_matrix(
    engine,
    tickers: list[str] | None = None,
    lookback_days: int = 200,
) -> pd.DataFrame:
    """
    Pull adjusted-close price matrix from the daily_prices table.

    Parameters
    ----------
    engine : sqlalchemy Engine (or connection).
    tickers : explicit list, or None → full ETF universe + single names.
    lookback_days : calendar days to fetch (not trading days).

    Returns
    -------
    DataFrame[DatetimeIndex, ticker columns], NaN-forward-filled.

    Adapt the SQL to match your schema.  This assumes a table with
    columns: date, symbol, adj_close.
    """
    from datetime import timedelta

    from common.universe import ETF_UNIVERSE, get_all_single_names

    if tickers is None:
        tickers = sorted(set(ETF_UNIVERSE + get_all_single_names()))

    placeholders = ", ".join([f"'{t}'" for t in tickers])
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    query = f"""
        SELECT date, symbol, adj_close
        FROM   daily_prices
        WHERE  symbol IN ({placeholders})
          AND  date >= '{cutoff}'
        ORDER  BY date
    """

    df = pd.read_sql(query, engine, parse_dates=["date"])
    matrix = df.pivot(index="date", columns="symbol", values="adj_close")
    matrix = matrix.sort_index().ffill()

    return matrix


# ═══════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═══════════════════════════════════════════════════════════════

def print_result(result: RotationResult) -> None:
    """Pretty-print the full rotation output to stdout."""

    w = 72
    print(f"\n{'═' * w}")
    print(f"  SMART MONEY ROTATION  —  {result.as_of_date}")
    print(f"{'═' * w}")

    # ── Sector Rankings ────────────────────────────────────────
    print(f"\n  SECTOR RANKINGS")
    print(f"  {'─' * (w - 4)}")
    for s in result.sector_rankings:
        icon = "🟢" if s.tier == "Leading" else ("🔴" if s.tier == "Lagging" else "⚪")
        rets_str = "  ".join(
            f"{p}d: {r:+.1%}" for p, r in sorted(s.period_returns.items())
        )
        print(f"  {icon} {s.rank:2d}. {s.sector:28s} [{s.etf:4s}]  "
              f"RS {s.composite_rs:+.4f}   {rets_str}")

    # ── Sell ───────────────────────────────────────────────────
    if result.sells:
        print(f"\n  🔴 SELL  ({len(result.sells)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.sells:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Reduce ─────────────────────────────────────────────────
    if result.reduces:
        print(f"\n  🟡 REDUCE  ({len(result.reduces)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.reduces:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Buy ────────────────────────────────────────────────────
    if result.buys:
        print(f"\n  🟢 BUY  ({len(result.buys)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.buys:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  "
                  f"vs-sector {r.rs_vs_sector_etf:+.4f}  │  {r.reason}")

    # ── Hold ───────────────────────────────────────────────────
    if result.holds:
        print(f"\n  ⚪ HOLD  ({len(result.holds)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.holds:
            print(f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}  │  {r.reason}")

    # ── Summary ────────────────────────────────────────────────
    print(f"\n  {'─' * (w - 4)}")
    total = len(result.recommendations)
    print(f"  Summary: {len(result.sells)} sell  {len(result.reduces)} reduce  "
          f"{len(result.buys)} buy  {len(result.holds)} hold  "
          f"({total} total)")
    print(f"  Leading sectors : {', '.join(result.leading_sectors)}")
    print(f"  Lagging sectors : {', '.join(result.lagging_sectors)}")
    print(f"{'═' * w}\n")


# ═══════════════════════════════════════════════════════════════
#  CLI / EXAMPLE
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    """
    Example usage with a live database:

        from sqlalchemy import create_engine
        engine = create_engine("sqlite:///data/market.db")

        prices = load_price_matrix(engine, lookback_days=200)
        result = run_rotation(
            prices=prices,
            current_holdings=["NVDA", "CRWD", "CEG", "LMT", "AVGO"],
            config=RotationConfig(
                stocks_per_sector=3,
                max_total_positions=12,
            ),
        )
        print_result(result)
    """

    # ── Quick smoke test with synthetic data ───────────────────
    import numpy as np

    np.random.seed(42)
    n_days = 150

    all_tickers = (
        ["SPY"]
        + list(SECTOR_ETFS.values())
        + get_us_tickers_for_sector("Technology")[:5]
        + get_us_tickers_for_sector("Energy")[:3]
        + get_us_tickers_for_sector("Industrials")[:3]
    )
    all_tickers = sorted(set(all_tickers))

    dates = pd.bdate_range(end=date.today(), periods=n_days)
    fake_prices = pd.DataFrame(
        index=dates,
        columns=all_tickers,
        data=100 * np.cumprod(
            1 + np.random.normal(0.0003, 0.015, (n_days, len(all_tickers))),
            axis=0,
        ),
    )

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    result = run_rotation(
        prices=fake_prices,
        current_holdings=["NVDA", "CCJ", "LMT"],
    )
    print_result(result)