"""
backtest/phase2/tracker.py
Virtual portfolio tracker with minimum hold period,
trailing stop, max hold duration, and score-ranked buys.

Features:
  - MOMENTUM/BETA-TILTED BUY RANKING: among BUY candidates, ranks by a
    blended score that favors high-momentum, high-vol names.
  - VARIABLE POSITION SIZING: _buy() accepts per-ticker weight from pipeline.
  - Dynamic sizing uses current NAV (not frozen initial_capital).
  - force_exits() tags exit_type for attribution.
  - Trailing stop fires at open price (gap-down protection).
  - MIN BETA FILTER: optionally skips low-vol names entirely.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import logging
import numpy as np

log = logging.getLogger(__name__)


@dataclass
class Position:
    ticker: str
    entry_date: object
    entry_price: float
    shares: int
    cost_basis: float
    peak_price: float = 0.0
    latest_score: float = 0.0
    current_price: float = 0.0
    target_weight: float = 0.0
    entry_momentum_rank: float = 0.0  # diagnostic: the blended rank at entry

    def __post_init__(self):
        if self.peak_price == 0.0:
            self.peak_price = self.entry_price
        if self.current_price == 0.0:
            self.current_price = self.entry_price

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price


class PortfolioTracker:

    def __init__(
        self,
        initial_capital: float = 1_000_000.0,
        max_positions: int = 12,
        commission_rate: float = 0.0010,
        slippage_rate: float = 0.0010,
        min_hold_days: int = 5,
        min_profit_early_exit_pct: float = 0.05,
        trailing_stop_pct: float = 0.18,
        max_hold_days: int = 120,
        upgrade_min_score_gap: float = 999,
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_positions = max_positions
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.min_hold_days = min_hold_days
        self.min_profit_early_exit_pct = min_profit_early_exit_pct

        self.trailing_stop_pct = trailing_stop_pct
        self.max_hold_days = max_hold_days
        self.upgrade_min_score_gap = upgrade_min_score_gap

        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Dict] = []

        # Diagnostic counters
        self._total_buys: int = 0
        self._total_sells: int = 0
        self._blocked_sells: int = 0
        self._filtered_low_vol: int = 0
        self._ranking_logged: bool = False

    # ──────────────────────────────────────────────────────────
    #  NAV
    # ──────────────────────────────────────────────────────────
    @property
    def nav(self) -> float:
        return self.cash + sum(
            pos.market_value for pos in self.positions.values()
        )

    # ──────────────────────────────────────────────────────────
    #  Min-hold check
    # ──────────────────────────────────────────────────────────
    def _can_sell(self, pos: Position, date, current_price: float) -> bool:
        held_days = (date - pos.entry_date).days
        if held_days >= self.min_hold_days:
            return True
        unrealised_pct = (current_price / pos.entry_price) - 1.0
        if unrealised_pct >= self.min_profit_early_exit_pct:
            log.debug(
                "  early exit allowed %s: +%.1f%% after %dd",
                pos.ticker, unrealised_pct * 100, held_days,
            )
            return True
        return False

    # ──────────────────────────────────────────────────────────
    #  Force exits: trailing stop + max hold
    # ──────────────────────────────────────────────────────────
    def force_exits(self, date, prices, **kw) -> List[str]:
        force_sold = []
        for ticker in list(self.positions):
            pos = self.positions[ticker]
            price = prices.get(ticker)

            if price is None:
                held = (date - pos.entry_date).days
                if held > 5:
                    log.warning(
                        "  ⚠ NO PRICE for %-10s  held %dd  "
                        "entry=$%.2f  peak=$%.2f  — STOP CANNOT FIRE",
                        ticker, held, pos.entry_price, pos.peak_price,
                    )
                continue

            held_days = (date - pos.entry_date).days
            drawdown_from_peak = (price - pos.peak_price) / pos.peak_price
            drawdown_from_entry = (price - pos.entry_price) / pos.entry_price

            if drawdown_from_entry < -0.25:
                log.warning(
                    "  ⚠ BIG LOSS %-10s  from_entry=%.1f%%  from_peak=%.1f%%  "
                    "price=$%.2f  peak=$%.2f  entry=$%.2f  held=%dd",
                    ticker,
                    drawdown_from_entry * 100,
                    drawdown_from_peak * 100,
                    price, pos.peak_price, pos.entry_price, held_days,
                )

            reason = None
            exit_type = None

            if held_days >= self.max_hold_days:
                reason = f"max_hold ({held_days}d >= {self.max_hold_days}d)"
                exit_type = "force_exit_max_hold"
            elif drawdown_from_peak <= -self.trailing_stop_pct:
                reason = (
                    f"trailing_stop ({drawdown_from_peak:+.1%} from "
                    f"peak ${pos.peak_price:.2f})"
                )
                exit_type = "force_exit_trailing"

            if reason:
                log.info("  ✖ FORCE-EXIT %-10s  %s", ticker, reason)
                self._sell(date, ticker, price, exit_type=exit_type)
                force_sold.append(ticker)

        return force_sold

    # ──────────────────────────────────────────────────────────
    #  Upgrade
    # ──────────────────────────────────────────────────────────
    def try_upgrades(
        self,
        date,
        candidate_scores: Dict[str, float],
        prices: Dict[str, float],
        max_upgrades: int = 2,
    ) -> List[Tuple[str, str]]:
        if not self.positions or not candidate_scores:
            return []

        held_scored = [
            (t, pos.latest_score)
            for t, pos in self.positions.items()
            if self._can_sell(pos, date, prices.get(t, pos.entry_price))
        ]
        held_scored.sort(key=lambda x: x[1])

        candidates = [
            (t, s)
            for t, s in candidate_scores.items()
            if t not in self.positions and t in prices
        ]
        candidates.sort(key=lambda x: x[1], reverse=True)

        swaps = []
        used_held = set()
        used_cand = set()

        for cand_ticker, cand_score in candidates:
            if len(swaps) >= max_upgrades:
                break
            if cand_ticker in used_cand:
                continue

            for held_ticker, held_score in held_scored:
                if held_ticker in used_held:
                    continue
                gap = cand_score - held_score
                if gap >= self.upgrade_min_score_gap:
                    log.info(
                        "  ⇄ UPGRADE  sell %-10s (score %.3f) → "
                        "buy %-10s (score %.3f, gap +%.3f)",
                        held_ticker, held_score,
                        cand_ticker, cand_score, gap,
                    )
                    self._sell(
                        date, held_ticker, prices[held_ticker],
                        exit_type="upgrade_out",
                    )
                    self._buy(
                        date, cand_ticker, prices[cand_ticker],
                        current_nav=self.nav,
                    )
                    used_held.add(held_ticker)
                    used_cand.add(cand_ticker)
                    swaps.append((held_ticker, cand_ticker))
                    break

        return swaps

    # ──────────────────────────────────────────────────────────
    #  BLENDED BUY RANKING — momentum/beta tilt
    #
    #  The ranking formula:
    #    blended_rank = composite_rank * (1 - tilt) + momentum_rank * tilt
    #
    #  Where momentum_rank is derived from:
    #    - rszscore (relative strength z-score vs benchmark)
    #    - trailing realized volatility (higher vol = higher beta proxy)
    #    - rsi14 (current momentum)
    #
    #  This ensures that among all BUY candidates with similar scores,
    #  we pick the ones with higher beta / stronger momentum.
    # ──────────────────────────────────────────────────────────
    def _rank_buy_candidates(
        self,
        buy_tickers: List[str],
        scores: Dict[str, float],
        momentum_metrics: Dict[str, Dict[str, float]],
        trailing_vols: Dict[str, float],
        params: Dict,
    ) -> List[str]:
        """
        Re-rank buy candidates using blended composite + momentum score.

        params keys:
            momentum_tilt:     0.0-1.0, how much to favor momentum (default 0.4)
            vol_preference:    0.0-1.0, weight of vol in momentum score (default 0.3)
            min_trailing_vol:  minimum annualized vol to pass filter (default 0.0)
            min_rszscore:      minimum RS z-score to pass filter (default -99)
            prefer_strong_buy: bonus multiplier for STRONG_BUY (default 1.0, no bonus)

        Returns: buy_tickers sorted best-first (highest blended rank).
        """
        if not buy_tickers:
            return []

        tilt = params.get("momentum_tilt", 0.4)
        vol_weight = params.get("vol_preference", 0.3)
        min_vol = params.get("min_trailing_vol", 0.0)
        min_rs = params.get("min_rszscore", -99.0)

        # ── Step 1: Filter out low-vol / low-RS names ────────────
        filtered = []
        for t in buy_tickers:
            vol = trailing_vols.get(t, 0.0)
            mm = momentum_metrics.get(t, {})
            rs = mm.get("rszscore", mm.get("rs_zscore", 0.0))

            # Filter: skip low-vol names (likely bonds, utilities, stable large caps)
            if min_vol > 0 and vol < min_vol:
                self._filtered_low_vol += 1
                log.debug(
                    "  FILTER %-10s: trailing_vol=%.3f < min_vol=%.3f",
                    t, vol, min_vol,
                )
                continue

            # Filter: skip negative RS z-score (underperforming benchmark)
            if rs < min_rs:
                log.debug(
                    "  FILTER %-10s: rszscore=%.3f < min_rszscore=%.3f",
                    t, rs, min_rs,
                )
                continue

            filtered.append(t)

        if not filtered:
            # If all filtered out, fall back to original list (safety)
            log.debug(
                "  RANKING: all %d candidates filtered out, using unfiltered",
                len(buy_tickers),
            )
            filtered = buy_tickers

        # ── Step 2: Compute composite rank (percentile) ──────────
        n = len(filtered)
        if n == 1:
            return filtered

        # Sort by composite score → assign percentile rank (0=worst, 1=best)
        score_sorted = sorted(filtered, key=lambda t: scores.get(t, 0.0))
        composite_rank = {t: i / (n - 1) for i, t in enumerate(score_sorted)}

        # ── Step 3: Compute momentum rank ────────────────────────
        # Momentum signal = weighted blend of rszscore + trailing_vol + rsi
        momentum_raw = {}
        for t in filtered:
            mm = momentum_metrics.get(t, {})
            rs = mm.get("rszscore", mm.get("rs_zscore", 0.0))
            rsi = mm.get("rsi14", mm.get("rsi_14", 50.0))
            vol = trailing_vols.get(t, 0.30)  # default 30% annualized

            # Normalize RSI to 0-1 scale (30-70 range → 0-1)
            rsi_norm = max(0.0, min(1.0, (rsi - 30) / 40.0))

            # Normalize RS z-score (typically -3 to +3 → 0-1)
            rs_norm = max(0.0, min(1.0, (rs + 2.0) / 4.0))

            # Normalize vol (0.15 to 0.80 typical range → 0-1)
            vol_norm = max(0.0, min(1.0, (vol - 0.15) / 0.65))

            # Blend: RS is primary, vol preference configurable, RSI secondary
            rs_weight = 1.0 - vol_weight - 0.2  # remainder after vol + rsi
            momentum_raw[t] = (
                rs_norm * max(rs_weight, 0.3) +
                vol_norm * vol_weight +
                rsi_norm * 0.2
            )

        # Rank momentum (percentile)
        mom_sorted = sorted(filtered, key=lambda t: momentum_raw.get(t, 0.0))
        momentum_rank = {t: i / (n - 1) for i, t in enumerate(mom_sorted)}

        # ── Step 4: Blend ────────────────────────────────────────
        blended = {}
        for t in filtered:
            blended[t] = (
                composite_rank[t] * (1.0 - tilt) +
                momentum_rank[t] * tilt
            )

        # Sort descending (highest blended rank = best candidate)
        result = sorted(filtered, key=lambda t: blended[t], reverse=True)

        # ── Log ranking details (first time only) ────────────────
        if not self._ranking_logged and len(result) >= 3:
            log.info(
                "  RANKING (tilt=%.2f, vol_pref=%.2f, min_vol=%.3f, min_rs=%.1f):",
                tilt, vol_weight, min_vol, min_rs,
            )
            for t in result[:5]:
                mm = momentum_metrics.get(t, {})
                log.info(
                    "    %-12s  composite_rank=%.2f  mom_rank=%.2f  "
                    "blended=%.3f  score=%.3f  rs=%.2f  vol=%.3f",
                    t,
                    composite_rank.get(t, 0),
                    momentum_rank.get(t, 0),
                    blended.get(t, 0),
                    scores.get(t, 0),
                    mm.get("rszscore", mm.get("rs_zscore", 0)),
                    trailing_vols.get(t, 0),
                )
            if len(buy_tickers) > len(filtered):
                log.info(
                    "    (filtered %d → %d candidates)",
                    len(buy_tickers), len(filtered),
                )
            self._ranking_logged = True

        return result

    # ──────────────────────────────────────────────────────────
    #  Process signals — with momentum-tilted ranking
    # ──────────────────────────────────────────────────────────
    def process_signals(
        self,
        date,
        actions: Dict[str, str],
        prices: Dict[str, float],
        scores: Optional[Dict[str, float]] = None,
        current_nav: Optional[float] = None,
        position_sizes: Optional[Dict[str, float]] = None,
        momentum_metrics: Optional[Dict[str, Dict[str, float]]] = None,
        trailing_vols: Optional[Dict[str, float]] = None,
        buy_ranking_params: Optional[Dict] = None,
    ) -> None:
        scores = scores or {}
        position_sizes = position_sizes or {}
        momentum_metrics = momentum_metrics or {}
        trailing_vols = trailing_vols or {}
        buy_ranking_params = buy_ranking_params or {}

        # ── sells first (respect min hold) ────────────────────
        blocked_sells = []
        for ticker, action in actions.items():
            if action == "SELL" and ticker in self.positions:
                if ticker not in prices:
                    continue
                pos = self.positions[ticker]
                if self._can_sell(pos, date, prices[ticker]):
                    self._sell(date, ticker, prices[ticker], exit_type="signal_exit")
                else:
                    held = (date - pos.entry_date).days
                    blocked_sells.append((ticker, held))

        if blocked_sells:
            self._blocked_sells += len(blocked_sells)
            log.debug(
                "  min-hold blocked %d sells: %s",
                len(blocked_sells),
                [(t, f"{d}d") for t, d in blocked_sells[:5]],
            )

        # ── then buys — MOMENTUM-TILTED RANKING ──────────────
        buy_tickers = [
            t
            for t, a in actions.items()
            if a in ("BUY", "STRONG_BUY")
            and t not in self.positions
            and t in prices
        ]

        # Apply blended ranking (composite + momentum/beta tilt)
        if buy_tickers and (momentum_metrics or trailing_vols):
            buy_tickers = self._rank_buy_candidates(
                buy_tickers,
                scores=scores,
                momentum_metrics=momentum_metrics,
                trailing_vols=trailing_vols,
                params=buy_ranking_params,
            )
        else:
            # Fallback: pure composite score ranking
            buy_tickers.sort(key=lambda t: scores.get(t, 0.0), reverse=True)

        slots = self.max_positions - len(self.positions)
        if slots <= 0 or not buy_tickers:
            return

        for ticker in buy_tickers[:slots]:
            ticker_weight = position_sizes.get(ticker)
            self._buy(
                date, ticker, prices[ticker],
                current_nav=current_nav,
                target_weight=ticker_weight,
            )

    # ──────────────────────────────────────────────────────────
    #  Update held positions' scores
    # ──────────────────────────────────────────────────────────
    def update_scores(self, scores: Dict[str, float]) -> None:
        for ticker, pos in self.positions.items():
            if ticker in scores:
                pos.latest_score = scores[ticker]

    # ──────────────────────────────────────────────────────────
    #  Buy — VARIABLE SIZING
    # ──────────────────────────────────────────────────────────
    def _buy(
        self,
        date,
        ticker: str,
        raw_price: float,
        current_nav: Optional[float] = None,
        target_weight: Optional[float] = None,
    ) -> None:
        exec_price = raw_price * (1 + self.slippage_rate)

        sizing_nav = current_nav if current_nav else self.nav

        if target_weight is not None and target_weight > 0:
            weight = min(target_weight, 0.25)
            slot_target = sizing_nav * weight
            sizing_source = f"pipeline_weight={target_weight:.3f}"
        else:
            weight = 1.0 / self.max_positions
            slot_target = sizing_nav * weight
            sizing_source = f"equal_weight=1/{self.max_positions}"

        target_value = min(slot_target, self.cash * 0.95)

        if target_value < 1_000:
            log.debug(
                "  SKIP BUY %-10s: target_value=$%.0f too small "
                "(nav=$%s, cash=$%s, weight=%.3f, source=%s)",
                ticker, target_value,
                f"{sizing_nav:,.0f}", f"{self.cash:,.0f}",
                weight, sizing_source,
            )
            return

        shares = int(target_value / exec_price)
        if shares <= 0:
            return

        cost = shares * exec_price
        commission = cost * self.commission_rate
        total = cost + commission
        if total > self.cash:
            max_cost = self.cash / (1 + self.commission_rate)
            shares = int(max_cost / exec_price)
            if shares <= 0:
                return
            cost = shares * exec_price
            commission = cost * self.commission_rate
            total = cost + commission
            if total > self.cash:
                return

        self.cash -= total
        self.positions[ticker] = Position(
            ticker=ticker,
            entry_date=date,
            entry_price=exec_price,
            shares=shares,
            cost_basis=total,
            peak_price=exec_price,
            current_price=exec_price,
            target_weight=weight,
        )
        self._total_buys += 1

        log.info(
            "  ▲ BUY  %-10s  %d shares @ %.2f  cost $%s  "
            "(weight=%.1f%%, slot=$%s, nav=$%s, %s)",
            ticker, shares, exec_price,
            f"{total:,.0f}",
            weight * 100,
            f"{slot_target:,.0f}",
            f"{sizing_nav:,.0f}",
            sizing_source,
        )

    # ──────────────────────────────────────────────────────────
    #  Sell
    # ──────────────────────────────────────────────────────────
    def _sell(
        self,
        date,
        ticker: str,
        raw_price: float,
        exit_type: Optional[str] = None,
    ) -> None:
        pos = self.positions.get(ticker)
        if pos is None:
            return

        exec_price = raw_price * (1 - self.slippage_rate)
        proceeds = pos.shares * exec_price
        commission = proceeds * self.commission_rate
        net = proceeds - commission

        pnl = net - pos.cost_basis
        pnl_pct = pnl / pos.cost_basis
        held = (date - pos.entry_date).days

        self.cash += net
        del self.positions[ticker]
        self._total_sells += 1

        trade_record = {
            "ticker": ticker,
            "entry_date": pos.entry_date,
            "exit_date": date,
            "entry_price": pos.entry_price,
            "exit_price": exec_price,
            "shares": pos.shares,
            "cost_basis": pos.cost_basis,
            "net_proceeds": net,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "holding_days": held,
            "peak_price": pos.peak_price,
            "max_favorable": (pos.peak_price / pos.entry_price) - 1.0,
            "latest_score": pos.latest_score,
            "entry_weight": pos.target_weight,
        }
        if exit_type:
            trade_record["exit_type"] = exit_type

        self.closed_trades.append(trade_record)

        log.info(
            "  ▼ SELL %-10s  %d shares @ %.2f  PnL $%s (%+.1f%%)  "
            "held %dd  [%s]",
            ticker, pos.shares, exec_price,
            f"{pnl:,.0f}", pnl_pct * 100, held,
            exit_type or "unknown",
        )

    # ──────────────────────────────────────────────────────────
    #  Mark-to-market
    # ──────────────────────────────────────────────────────────
    def mark_to_market(self, date, close_prices: Dict[str, float]) -> float:
        pos_val = 0.0
        for t, p in self.positions.items():
            price = close_prices.get(t, p.current_price)
            p.current_price = price
            pos_val += price * p.shares
            if price > p.peak_price:
                p.peak_price = price
        return self.cash + pos_val