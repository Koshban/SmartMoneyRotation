"""
reports/portfolio_view.py
Portfolio view and rebalance-delta generator.

Compares the CASH recommendation report against your actual current
holdings and produces:
  1. A rebalance plan (what to buy, sell, trim, add)
  2. A current-portfolio health check (per-position diagnostics)
  3. Drift analysis (target vs actual allocation by bucket)

This is the "what do I actually need to DO" layer.
"""

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
from datetime import datetime


# ═════════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═════════════════════════════════════════════════════════════════

@dataclass
class Position:
    """A single current holding."""
    ticker: str
    shares: int
    avg_cost: float
    current_price: float
    category: str = ""
    bucket: str = ""

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def cost_basis(self) -> float:
        return self.shares * self.avg_cost

    @property
    def unrealised_pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def unrealised_pct(self) -> float:
        if self.cost_basis == 0:
            return 0.0
        return self.unrealised_pnl / self.cost_basis

    def to_dict(self) -> dict:
        return {
            "ticker":         self.ticker,
            "shares":         self.shares,
            "avg_cost":       self.avg_cost,
            "current_price":  self.current_price,
            "market_value":   self.market_value,
            "cost_basis":     self.cost_basis,
            "unrealised_pnl": self.unrealised_pnl,
            "unrealised_pct": self.unrealised_pct,
            "category":       self.category,
            "bucket":         self.bucket,
        }


@dataclass
class TradeAction:
    """A single trade needed to reach the target portfolio."""
    ticker: str
    action: str          # "BUY_NEW", "ADD", "TRIM", "CLOSE", "NO_CHANGE"
    current_shares: int
    target_shares: int
    delta_shares: int    # positive = buy, negative = sell
    current_price: float
    delta_dollars: float
    reason: str
    priority: int = 0    # lower = more urgent (1 = highest)
    stop_price: Optional[float] = None
    composite: Optional[float] = None
    confidence: Optional[float] = None

    @property
    def abs_delta_dollars(self) -> float:
        return abs(self.delta_dollars)

    def to_dict(self) -> dict:
        return {
            "ticker":          self.ticker,
            "action":          self.action,
            "current_shares":  self.current_shares,
            "target_shares":   self.target_shares,
            "delta_shares":    self.delta_shares,
            "current_price":   self.current_price,
            "delta_dollars":   self.delta_dollars,
            "reason":          self.reason,
            "priority":        self.priority,
            "stop_price":      self.stop_price,
            "composite":       self.composite,
            "confidence":      self.confidence,
        }


@dataclass
class RebalancePlan:
    """Complete rebalance output."""
    date: str
    portfolio_value: float
    cash_balance: float
    positions: list          # list of Position
    trades: list             # list of TradeAction
    bucket_drift: dict       # bucket -> {target_pct, actual_pct, drift_pct, drift_dollars}
    health_checks: list      # list of per-position health dicts
    warnings: list = field(default_factory=list)

    @property
    def buy_trades(self) -> list:
        return [t for t in self.trades if t.delta_shares > 0]

    @property
    def sell_trades(self) -> list:
        return [t for t in self.trades if t.delta_shares < 0]

    @property
    def net_cash_impact(self) -> float:
        """Negative means net outflow (buying), positive means net inflow (selling)."""
        return sum(t.delta_dollars for t in self.trades)

    @property
    def trade_count(self) -> int:
        return len([t for t in self.trades if t.action != "NO_CHANGE"])

    def to_dict(self) -> dict:
        return {
            "date":            self.date,
            "portfolio_value": self.portfolio_value,
            "cash_balance":    self.cash_balance,
            "trade_count":     self.trade_count,
            "net_cash_impact": self.net_cash_impact,
            "trades":          [t.to_dict() for t in self.trades],
            "bucket_drift":    self.bucket_drift,
            "health_checks":   self.health_checks,
            "warnings":        self.warnings,
        }


# ═════════════════════════════════════════════════════════════════
#  1.  REBALANCE PLAN BUILDER
# ═════════════════════════════════════════════════════════════════

def build_rebalance_plan(
    report: dict,
    current_positions: list[dict],
    cash_balance: float,
    portfolio_value: float,
    *,
    min_trade_dollars: float = 100.0,
    trim_threshold_pct: float = 0.02,
) -> RebalancePlan:
    """
    Compare CASH recommendation report against current holdings
    and produce a complete rebalance plan.

    Parameters
    ----------
    report : dict
        Output of recommendations.build_report().
    current_positions : list of dict
        Each dict needs: ticker, shares, avg_cost, current_price.
        Optional: category, bucket.
    cash_balance : float
        Current cash available.
    portfolio_value : float
        Total portfolio value (positions + cash).
    min_trade_dollars : float
        Trades smaller than this are filtered out as noise.
    trim_threshold_pct : float
        Minimum position weight drift to trigger a trim (fraction of portfolio).

    Returns
    -------
    RebalancePlan
    """
    # ── parse current positions ─────────────────────────────────
    positions = []
    current_map = {}  # ticker -> Position
    for p in current_positions:
        pos = Position(
            ticker=p["ticker"].upper(),
            shares=int(p["shares"]),
            avg_cost=float(p["avg_cost"]),
            current_price=float(p["current_price"]),
            category=p.get("category", ""),
            bucket=p.get("bucket", ""),
        )
        positions.append(pos)
        current_map[pos.ticker] = pos

    # ── parse recommendation targets ───────────────────────────
    buy_targets = {}   # ticker -> buy dict
    for b in report["buy_list"]:
        buy_targets[b["ticker"].upper()] = b

    sell_tickers = set()
    for s in report["sell_list"]:
        sell_tickers.add(s["ticker"].upper())

    hold_tickers = set()
    for h in report["hold_list"]:
        hold_tickers.add(h["ticker"].upper())

    # ── generate trade actions ──────────────────────────────────
    trades = []
    warnings = []
    processed = set()

    # --- SELLS: close positions that CASH says to sell ----------
    for ticker in sell_tickers:
        processed.add(ticker)
        if ticker in current_map:
            pos = current_map[ticker]
            delta = -pos.shares
            sell_record = _find_in_list(report["sell_list"], ticker)
            trades.append(TradeAction(
                ticker=ticker,
                action="CLOSE",
                current_shares=pos.shares,
                target_shares=0,
                delta_shares=delta,
                current_price=pos.current_price,
                delta_dollars=delta * pos.current_price,
                reason=f"SELL signal — composite {sell_record['composite']:.2f}, "
                       f"confidence {sell_record['confidence']:.0%}",
                priority=1,
                composite=sell_record["composite"],
                confidence=sell_record["confidence"],
            ))
        # if we don't hold it, no action needed

    # --- BUYS: new positions or add to existing ----------------
    for ticker, target in buy_targets.items():
        processed.add(ticker)
        target_shares = target["shares"]
        target_price = target["close"]

        if ticker in current_map:
            # already hold — compute delta
            pos = current_map[ticker]
            current_shares = pos.shares
            delta = target_shares - current_shares

            if delta > 0:
                # need to add
                delta_dollars = delta * target_price
                if abs(delta_dollars) >= min_trade_dollars:
                    trades.append(TradeAction(
                        ticker=ticker,
                        action="ADD",
                        current_shares=current_shares,
                        target_shares=target_shares,
                        delta_shares=delta,
                        current_price=target_price,
                        delta_dollars=delta_dollars,
                        reason=f"BUY signal — add {delta} shares to reach "
                               f"target {target_shares} "
                               f"(composite {target['composite']:.2f})",
                        priority=2,
                        stop_price=target.get("stop_price"),
                        composite=target["composite"],
                        confidence=target["confidence"],
                    ))
            elif delta < 0:
                # overweight vs target — suggest trim
                delta_dollars = delta * target_price
                weight_drift = abs(delta_dollars) / portfolio_value
                if weight_drift >= trim_threshold_pct:
                    trades.append(TradeAction(
                        ticker=ticker,
                        action="TRIM",
                        current_shares=current_shares,
                        target_shares=target_shares,
                        delta_shares=delta,
                        current_price=target_price,
                        delta_dollars=delta_dollars,
                        reason=f"Overweight by {abs(delta)} shares — trim to "
                               f"target {target_shares}",
                        priority=3,
                        stop_price=target.get("stop_price"),
                        composite=target["composite"],
                        confidence=target["confidence"],
                    ))
            # else: exactly on target, no trade needed
        else:
            # new position
            delta_dollars = target_shares * target_price
            if delta_dollars >= min_trade_dollars:
                trades.append(TradeAction(
                    ticker=ticker,
                    action="BUY_NEW",
                    current_shares=0,
                    target_shares=target_shares,
                    delta_shares=target_shares,
                    current_price=target_price,
                    delta_dollars=delta_dollars,
                    reason=f"New BUY — rank #{target['rank']}, "
                           f"composite {target['composite']:.2f}, "
                           f"confidence {target['confidence']:.0%}",
                    priority=2,
                    stop_price=target.get("stop_price"),
                    composite=target["composite"],
                    confidence=target["confidence"],
                ))

    # --- ORPHANS: positions we hold that CASH didn't mention ---
    for ticker, pos in current_map.items():
        if ticker not in processed:
            if ticker in hold_tickers:
                # CASH says hold — no action
                trades.append(TradeAction(
                    ticker=ticker,
                    action="NO_CHANGE",
                    current_shares=pos.shares,
                    target_shares=pos.shares,
                    delta_shares=0,
                    current_price=pos.current_price,
                    delta_dollars=0,
                    reason="HOLD signal — maintain current position",
                    priority=9,
                ))
            else:
                # not in buy, sell, or hold — CASH has no opinion
                # flag as orphan for manual review
                warnings.append(
                    f"ORPHAN: {ticker} ({pos.shares} shares, "
                    f"${pos.market_value:,.0f}) — not in CASH universe. "
                    f"Review manually."
                )
                trades.append(TradeAction(
                    ticker=ticker,
                    action="NO_CHANGE",
                    current_shares=pos.shares,
                    target_shares=pos.shares,
                    delta_shares=0,
                    current_price=pos.current_price,
                    delta_dollars=0,
                    reason="ORPHAN — not in CASH universe, review manually",
                    priority=5,
                ))

    # ── sort trades by priority then abs dollar size ────────────
    trades.sort(key=lambda t: (t.priority, -t.abs_delta_dollars))

    # ── cash feasibility check ──────────────────────────────────
    total_buy_cost = sum(t.delta_dollars for t in trades if t.delta_shares > 0)
    total_sell_proceeds = abs(sum(t.delta_dollars for t in trades if t.delta_shares < 0))
    net_outflow = total_buy_cost - total_sell_proceeds

    if net_outflow > cash_balance:
        warnings.append(
            f"CASH SHORTFALL: Net buy cost ${net_outflow:,.0f} exceeds "
            f"cash ${cash_balance:,.0f} by ${net_outflow - cash_balance:,.0f}. "
            f"Sells should execute before buys, or reduce buy sizes."
        )

    # ── bucket drift analysis ───────────────────────────────────
    bucket_drift = _compute_bucket_drift(
        report=report,
        current_map=current_map,
        buy_targets=buy_targets,
        portfolio_value=portfolio_value,
    )

    # ── per-position health checks ──────────────────────────────
    health_checks = _build_health_checks(
        positions=positions,
        report=report,
        portfolio_value=portfolio_value,
    )

    return RebalancePlan(
        date=report["header"]["date"],
        portfolio_value=portfolio_value,
        cash_balance=cash_balance,
        positions=positions,
        trades=trades,
        bucket_drift=bucket_drift,
        health_checks=health_checks,
        warnings=warnings,
    )


# ═════════════════════════════════════════════════════════════════
#  2.  DRIFT ANALYSIS
# ═════════════════════════════════════════════════════════════════

def _compute_bucket_drift(
    report: dict,
    current_map: dict,
    buy_targets: dict,
    portfolio_value: float,
) -> dict:
    """
    Compare target bucket allocation against current actual allocation.

    Returns dict of bucket -> {target_pct, actual_pct, drift_pct, drift_dollars,
                                actual_dollars, target_dollars}
    """
    bucket_weights = report["allocation"]["bucket_weights"]
    drift = {}

    # actual dollars per bucket from current holdings
    actual_by_bucket = {}
    for ticker, pos in current_map.items():
        bkt = pos.bucket or _infer_bucket(ticker, buy_targets, report)
        actual_by_bucket[bkt] = actual_by_bucket.get(bkt, 0) + pos.market_value

    # also count tickers in buy_targets that we don't hold yet
    # (they represent target allocation, not actual)

    all_buckets = set(bucket_weights.keys()) | set(actual_by_bucket.keys())

    for bucket in sorted(all_buckets):
        target_pct = bucket_weights.get(bucket, 0.0)
        target_dollars = target_pct * portfolio_value
        actual_dollars = actual_by_bucket.get(bucket, 0.0)
        actual_pct = actual_dollars / portfolio_value if portfolio_value > 0 else 0.0
        drift_pct = actual_pct - target_pct
        drift_dollars = actual_dollars - target_dollars

        drift[bucket] = {
            "target_pct":     target_pct,
            "actual_pct":     actual_pct,
            "drift_pct":      drift_pct,
            "drift_dollars":  drift_dollars,
            "actual_dollars":  actual_dollars,
            "target_dollars":  target_dollars,
        }

    return drift


def _infer_bucket(ticker: str, buy_targets: dict, report: dict) -> str:
    """Try to find the bucket for a ticker from buy targets or hold list."""
    if ticker in buy_targets:
        return buy_targets[ticker].get("bucket", "unknown")
    for h in report["hold_list"]:
        if h["ticker"].upper() == ticker:
            return h.get("bucket", "unknown")
    return "unknown"


# ═════════════════════════════════════════════════════════════════
#  3.  PER-POSITION HEALTH CHECKS
# ═════════════════════════════════════════════════════════════════

def _build_health_checks(
    positions: list,
    report: dict,
    portfolio_value: float,
) -> list:
    """
    Generate a health diagnostic for each current position.

    Flags: concentration risk, underwater positions, stop proximity,
    signal disagreement.
    """
    buy_map = {b["ticker"].upper(): b for b in report["buy_list"]}
    sell_set = {s["ticker"].upper() for s in report["sell_list"]}
    checks = []

    max_position_pct = 0.08  # flag if any single position > 8% of portfolio

    for pos in positions:
        ticker = pos.ticker
        weight = pos.market_value / portfolio_value if portfolio_value > 0 else 0
        flags = []

        # concentration
        if weight > max_position_pct:
            flags.append(
                f"CONCENTRATION: {weight:.1%} of portfolio "
                f"(threshold {max_position_pct:.0%})"
            )

        # underwater
        if pos.unrealised_pct < -0.10:
            flags.append(
                f"UNDERWATER: {pos.unrealised_pct:.1%} unrealised loss "
                f"(${pos.unrealised_pnl:,.0f})"
            )

        # deep underwater
        if pos.unrealised_pct < -0.25:
            flags.append(
                f"DEEP LOSS: {pos.unrealised_pct:.1%} — consider tax-loss "
                f"harvest or forced exit"
            )

        # stop proximity
        if ticker in buy_map:
            rec = buy_map[ticker]
            stop = rec.get("stop_price")
            if stop and pos.current_price > 0:
                stop_distance = (pos.current_price - stop) / pos.current_price
                if stop_distance < 0.02:
                    flags.append(
                        f"STOP PROXIMITY: price ${pos.current_price:.2f} is "
                        f"only {stop_distance:.1%} above stop ${stop:.2f}"
                    )
                if stop_distance < 0:
                    flags.append(
                        f"STOP BREACHED: price ${pos.current_price:.2f} is "
                        f"BELOW stop ${stop:.2f} — exit immediately"
                    )

        # signal disagreement: we hold it but CASH says SELL
        if ticker in sell_set:
            flags.append("SIGNAL CONFLICT: CASH recommends SELL but position is held")

        # signal check: we hold it and it's not in buy or hold
        if (ticker not in buy_map and
            ticker not in sell_set and
            ticker not in {h["ticker"].upper() for h in report["hold_list"]}):
            flags.append("ORPHAN: ticker not in CASH universe — no signal available")

        checks.append({
            "ticker":          ticker,
            "shares":          pos.shares,
            "current_price":   pos.current_price,
            "market_value":    pos.market_value,
            "weight_pct":      weight,
            "avg_cost":        pos.avg_cost,
            "unrealised_pnl":  pos.unrealised_pnl,
            "unrealised_pct":  pos.unrealised_pct,
            "flags":           flags,
            "flag_count":      len(flags),
            "healthy":         len(flags) == 0,
        })

    # sort: most-flagged first
    checks.sort(key=lambda c: (-c["flag_count"], -c["market_value"]))
    return checks


# ═════════════════════════════════════════════════════════════════
#  4.  PLAIN-TEXT REBALANCE REPORT
# ═════════════════════════════════════════════════════════════════

def rebalance_to_text(plan: RebalancePlan) -> str:
    """Render the rebalance plan as a plain-text report."""
    lines = []

    lines.append("=" * 72)
    lines.append("  CASH — REBALANCE PLAN")
    lines.append(f"  Date: {plan.date}    Portfolio: ${plan.portfolio_value:,.0f}    "
                 f"Cash: ${plan.cash_balance:,.0f}")
    lines.append("=" * 72)

    # ── warnings ────────────────────────────────────────────────
    if plan.warnings:
        lines.append("")
        lines.append("─── ⚠  WARNINGS ────────────────────────────────────────────────")
        for w in plan.warnings:
            lines.append(f"  ▸ {w}")

    # ── trade actions ───────────────────────────────────────────
    active_trades = [t for t in plan.trades if t.action != "NO_CHANGE"]

    lines.append("")
    lines.append(f"─── TRADES REQUIRED ({len(active_trades)}) "
                 "─────────────────────────────────────")

    if active_trades:
        lines.append(
            f"  {'Action':<10s} {'Ticker':<8s} {'Current':>8s} {'Target':>8s} "
            f"{'Delta':>8s} {'$Delta':>10s} {'Reason'}"
        )
        lines.append("  " + "-" * 68)

        for t in active_trades:
            sign = "+" if t.delta_shares > 0 else ""
            lines.append(
                f"  {t.action:<10s} {t.ticker:<8s} "
                f"{t.current_shares:>8d} {t.target_shares:>8d} "
                f"{sign}{t.delta_shares:>7d} "
                f"${t.delta_dollars:>+9,.0f}  "
                f"{t.reason}"
            )
            if t.stop_price:
                lines.append(f"{'':>51s} stop: ${t.stop_price:.2f}")
    else:
        lines.append("  No trades required — portfolio is aligned with recommendations.")

    # ── execution summary ───────────────────────────────────────
    lines.append("")
    lines.append("─── EXECUTION SUMMARY ──────────────────────────────────────────")
    buy_trades = plan.buy_trades
    sell_trades = plan.sell_trades
    total_buy = sum(t.delta_dollars for t in buy_trades)
    total_sell = sum(t.delta_dollars for t in sell_trades)  # negative
    lines.append(f"  Total to BUY:    ${total_buy:>12,.0f}  ({len(buy_trades)} trades)")
    lines.append(f"  Total to SELL:   ${abs(total_sell):>12,.0f}  ({len(sell_trades)} trades)")
    lines.append(f"  Net cash impact: ${plan.net_cash_impact:>+12,.0f}")
    lines.append(f"  Cash after:      ${plan.cash_balance + plan.net_cash_impact:>12,.0f}")

    # ── suggested execution order ───────────────────────────────
    if sell_trades and buy_trades:
        lines.append("")
        lines.append("─── SUGGESTED EXECUTION ORDER ──────────────────────────────────")
        lines.append("  1. Execute SELLS first to free cash:")
        for t in sell_trades:
            lines.append(f"     • {t.action} {t.ticker}  ({abs(t.delta_shares)} shares, "
                         f"~${abs(t.delta_dollars):,.0f})")
        lines.append("  2. Then execute BUYS:")
        for t in buy_trades:
            lines.append(f"     • {t.action} {t.ticker}  ({t.delta_shares} shares, "
                         f"~${t.delta_dollars:,.0f})")

    # ── bucket drift ────────────────────────────────────────────
    lines.append("")
    lines.append("─── BUCKET DRIFT ANALYSIS ─────────────────────────────────────")
    lines.append(
        f"  {'Bucket':<22s} {'Target':>7s} {'Actual':>7s} "
        f"{'Drift':>7s} {'$Drift':>12s}"
    )
    lines.append("  " + "-" * 58)
    for bucket, d in sorted(plan.bucket_drift.items()):
        lines.append(
            f"  {bucket:<22s} {d['target_pct']:>6.1%} {d['actual_pct']:>6.1%} "
            f"{d['drift_pct']:>+6.1%} ${d['drift_dollars']:>+11,.0f}"
        )

    # ── health checks ──────────────────────────────────────────
    flagged = [c for c in plan.health_checks if not c["healthy"]]
    if flagged:
        lines.append("")
        lines.append(f"─── POSITION HEALTH FLAGS ({len(flagged)} positions) "
                     "──────────────────────")
        for c in flagged:
            lines.append(
                f"  {c['ticker']:<8s}  ${c['market_value']:>10,.0f}  "
                f"({c['weight_pct']:.1%})  P&L {c['unrealised_pct']:>+6.1%}"
            )
            for flag in c["flags"]:
                lines.append(f"    ▸ {flag}")
            lines.append("")
    else:
        lines.append("")
        lines.append("─── POSITION HEALTH: All positions healthy ─────────────────────")

    # ── no-change positions ─────────────────────────────────────
    no_change = [t for t in plan.trades if t.action == "NO_CHANGE"]
    if no_change:
        lines.append("")
        lines.append(f"─── NO CHANGE ({len(no_change)} positions) "
                     "─────────────────────────────────")
        for t in no_change:
            lines.append(
                f"  {t.ticker:<8s}  {t.current_shares:>6d} shares  "
                f"${t.current_shares * t.current_price:>10,.0f}  "
                f"— {t.reason}"
            )

    lines.append("")
    lines.append("=" * 72)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════
#  5.  HTML REBALANCE REPORT
# ═════════════════════════════════════════════════════════════════

def rebalance_to_html(plan: RebalancePlan) -> str:
    """Render the rebalance plan as a self-contained HTML page."""

    active_trades = [t for t in plan.trades if t.action != "NO_CHANGE"]

    # ── trade rows ──────────────────────────────────────────────
    trade_rows = ""
    for t in active_trades:
        action_class = {
            "BUY_NEW": "buy", "ADD": "buy", "TRIM": "sell", "CLOSE": "sell",
        }.get(t.action, "neutral")
        sign = "+" if t.delta_shares > 0 else ""
        stop_str = f"${t.stop_price:.2f}" if t.stop_price else "—"
        trade_rows += f"""
        <tr class="{action_class}">
            <td class="action-badge {action_class}">{t.action}</td>
            <td class="ticker">{t.ticker}</td>
            <td class="num">{t.current_shares}</td>
            <td class="num">{t.target_shares}</td>
            <td class="num delta">{sign}{t.delta_shares}</td>
            <td class="num">${t.delta_dollars:+,.0f}</td>
            <td class="num">{stop_str}</td>
            <td class="reason">{t.reason}</td>
        </tr>"""

    # ── drift rows ──────────────────────────────────────────────
    drift_rows = ""
    for bucket, d in sorted(plan.bucket_drift.items()):
        drift_class = "over" if d["drift_pct"] > 0.01 else "under" if d["drift_pct"] < -0.01 else ""
        drift_rows += f"""
        <tr class="{drift_class}">
            <td>{bucket.replace('_', ' ').title()}</td>
            <td class="num">{d['target_pct']:.1%}</td>
            <td class="num">{d['actual_pct']:.1%}</td>
            <td class="num drift">{d['drift_pct']:+.1%}</td>
            <td class="num">${d['drift_dollars']:+,.0f}</td>
        </tr>"""

    # ── health rows ─────────────────────────────────────────────
    flagged = [c for c in plan.health_checks if not c["healthy"]]
    health_rows = ""
    for c in flagged:
        flag_html = "<br>".join(f"⚠ {f}" for f in c["flags"])
        health_rows += f"""
        <tr>
            <td class="ticker">{c['ticker']}</td>
            <td class="num">${c['market_value']:,.0f}</td>
            <td class="num">{c['weight_pct']:.1%}</td>
            <td class="num pnl">{c['unrealised_pct']:+.1%}</td>
            <td class="flags">{flag_html}</td>
        </tr>"""

    # ── warnings ────────────────────────────────────────────────
    warning_html = ""
    if plan.warnings:
        items = "".join(f"<li>{w}</li>" for w in plan.warnings)
        warning_html = f"""
        <div class="warning-box">
            <h2>⚠ Warnings</h2>
            <ul>{items}</ul>
        </div>"""

    # ── execution summary ───────────────────────────────────────
    total_buy = sum(t.delta_dollars for t in plan.buy_trades)
    total_sell_abs = abs(sum(t.delta_dollars for t in plan.sell_trades))
    cash_after = plan.cash_balance + plan.net_cash_impact

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CASH Rebalance — {plan.date}</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
                     Roboto, Helvetica, Arial, sans-serif;
        background: #0d1117; color: #c9d1d9;
        padding: 24px; max-width: 1100px; margin: 0 auto;
        font-size: 14px; line-height: 1.5;
    }}
    h1 {{ color: #58a6ff; font-size: 22px; }}
    h2 {{ color: #8b949e; font-size: 16px; margin: 24px 0 12px;
          border-bottom: 1px solid #30363d; padding-bottom: 6px; }}
    .subtitle {{ color: #8b949e; font-size: 13px; margin-bottom: 16px; }}
    .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
                   gap: 10px; margin: 16px 0; }}
    .stat-card {{ background: #161b22; padding: 12px; border-radius: 8px;
                   border: 1px solid #30363d; }}
    .stat-card .label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; }}
    .stat-card .value {{ font-size: 18px; font-weight: 700; color: #e6edf3; }}
    table {{ width: 100%; border-collapse: collapse; margin: 8px 0; font-size: 13px; }}
    th {{ text-align: left; font-size: 11px; color: #8b949e; text-transform: uppercase;
          padding: 8px 6px; border-bottom: 2px solid #30363d; }}
    td {{ padding: 7px 6px; border-bottom: 1px solid #21262d; }}
    tr:hover {{ background: #161b22; }}
    .ticker {{ font-weight: 700; color: #58a6ff; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .action-badge {{
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 700; text-transform: uppercase;
    }}
    .action-badge.buy {{ background: #23863622; color: #3fb950; }}
    .action-badge.sell {{ background: #f8514922; color: #f85149; }}
    .delta {{ font-weight: 700; }}
    tr.buy .delta {{ color: #3fb950; }}
    tr.sell .delta {{ color: #f85149; }}
    .reason {{ font-size: 11px; color: #8b949e; max-width: 260px; }}
    .drift {{ font-weight: 700; }}
    tr.over .drift {{ color: #d29922; }}
    tr.under .drift {{ color: #58a6ff; }}
    .pnl {{ font-weight: 700; }}
    .flags {{ font-size: 11px; color: #d29922; line-height: 1.4; }}
    .warning-box {{ background: #2d1b1b; border: 1px solid #f85149;
                     border-radius: 8px; padding: 16px; margin: 16px 0; }}
    .warning-box h2 {{ color: #f85149; border: none; margin-top: 0; }}
    .warning-box li {{ margin: 4px 0 4px 20px; font-size: 13px; }}
    .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #30363d;
               font-size: 11px; color: #484f58; text-align: center; }}
    @media (max-width: 700px) {{
        body {{ padding: 12px; }}
        table {{ font-size: 11px; }}
        .stat-grid {{ grid-template-columns: repeat(2, 1fr); }}
    }}
</style>
</head>
<body>

<h1>CASH — Rebalance Plan</h1>
<div class="subtitle">{plan.date} &nbsp;|&nbsp; Portfolio ${plan.portfolio_value:,.0f}</div>

{warning_html}

<div class="stat-grid">
    <div class="stat-card">
        <div class="label">Trades</div>
        <div class="value">{len(active_trades)}</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Buy</div>
        <div class="value">${total_buy:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Total Sell</div>
        <div class="value">${total_sell_abs:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Net Impact</div>
        <div class="value">${plan.net_cash_impact:+,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash Before</div>
        <div class="value">${plan.cash_balance:,.0f}</div>
    </div>
    <div class="stat-card">
        <div class="label">Cash After</div>
        <div class="value">${cash_after:,.0f}</div>
    </div>
</div>

<h2>Trades Required</h2>
<table>
    <thead>
        <tr><th>Action</th><th>Ticker</th><th>Current</th><th>Target</th>
            <th>Delta</th><th>$ Impact</th><th>Stop</th><th>Reason</th></tr>
    </thead>
    <tbody>
        {trade_rows if trade_rows else '<tr><td colspan="8">No trades required.</td></tr>'}
    </tbody>
</table>

<h2>Bucket Drift</h2>
<table>
    <thead>
        <tr><th>Bucket</th><th>Target</th><th>Actual</th><th>Drift</th><th>$ Drift</th></tr>
    </thead>
    <tbody>{drift_rows}</tbody>
</table>

{"<h2>Position Health Flags</h2>" if flagged else ""}
{"<table><thead><tr><th>Ticker</th><th>Value</th><th>Weight</th><th>P&L</th><th>Flags</th></tr></thead><tbody>" + health_rows + "</tbody></table>" if flagged else '<p style="color:#3fb950;margin-top:16px;">✓ All positions healthy — no flags.</p>'}

<div class="footer">
    CASH Rebalance Plan &nbsp;|&nbsp; {plan.date} &nbsp;|&nbsp;
    {len(active_trades)} trades &nbsp;|&nbsp;
    {len(flagged)} health flags
</div>

</body>
</html>"""

    return html


# ═════════════════════════════════════════════════════════════════
#  6.  FILE OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def save_rebalance_text(plan: RebalancePlan, filepath: str) -> str:
    text = rebalance_to_text(plan)
    with open(filepath, "w") as f:
        f.write(text)
    return filepath


def save_rebalance_html(plan: RebalancePlan, filepath: str) -> str:
    html = rebalance_to_html(plan)
    with open(filepath, "w") as f:
        f.write(html)
    return filepath


def print_rebalance(plan: RebalancePlan):
    print(rebalance_to_text(plan))


# ═════════════════════════════════════════════════════════════════
#  7.  CONVENIENCE: QUICK DIFF
# ═════════════════════════════════════════════════════════════════

def quick_diff(report: dict, current_positions: list[dict]) -> dict:
    """
    Fast summary of what changes are needed, without building a
    full RebalancePlan. Good for dashboards or quick checks.

    Returns dict with: new_buys, additions, trims, closes, holds, orphans.
    Each is a list of ticker strings.
    """
    current_tickers = {p["ticker"].upper() for p in current_positions}
    buy_tickers = {b["ticker"].upper() for b in report["buy_list"]}
    sell_tickers = {s["ticker"].upper() for s in report["sell_list"]}
    hold_tickers = {h["ticker"].upper() for h in report["hold_list"]}

    new_buys = buy_tickers - current_tickers
    additions = buy_tickers & current_tickers
    closes = sell_tickers & current_tickers
    holds = hold_tickers & current_tickers
    orphans = current_tickers - buy_tickers - sell_tickers - hold_tickers

    return {
        "new_buys":   sorted(new_buys),
        "additions":  sorted(additions),
        "trims":      [],  # would need share counts to determine
        "closes":     sorted(closes),
        "holds":      sorted(holds),
        "orphans":    sorted(orphans),
    }


# ═════════════════════════════════════════════════════════════════
#  PRIVATE HELPERS
# ═════════════════════════════════════════════════════════════════

def _find_in_list(lst: list, ticker: str) -> dict:
    """Find a ticker dict in a report list."""
    ticker = ticker.upper()
    for item in lst:
        if item["ticker"].upper() == ticker:
            return item
    return {"composite": 0, "confidence": 0}