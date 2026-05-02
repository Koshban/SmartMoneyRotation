"""backtest/phase2/diagnostics.py

Drop-in signal diagnostics. Wire into your engine's day loop
and signal generator to capture *why* BUY/SELL/HOLD decisions
are made.

Usage:
    from backtest.phase2.diagnostics import SignalDiagnostics
    diag = SignalDiagnostics("Loose", enabled=True)
    # ... wire calls at each decision point (see integration notes below)
    diag.print_summary()
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ── reason tags (use these as constants so summaries group cleanly) ─────
R_SCORE_BELOW_EXIT    = "score<exit"
R_SCORE_ABOVE_ENTRY   = "score>=entry"
R_RANK_BELOW_FLOOR    = "rank<exit_floor"
R_RANK_ABOVE_MIN      = "rank>=min_rank"
R_RANK_BELOW_MIN      = "rank<min_rank"
R_RS_BLOCKED          = "rs_regime_blocked"
R_RS_FAIL_PENALTY     = "rs_fail_penalty_applied"
R_SECTOR_BLOCKED      = "sector_regime_blocked"
R_BREADTH_HARD_BLOCK  = "breadth_hard_block"
R_VOL_HARD_BLOCK      = "vol_hard_block"
R_COOLDOWN            = "cooldown_active"
R_MAX_POSITIONS       = "max_positions_reached"
R_CONTINUATION        = "continuation_pass"
R_CONTINUATION_FAIL   = "continuation_fail"
R_PULLBACK            = "pullback_pass"
R_PULLBACK_FAIL       = "pullback_fail"
R_CHAOTIC_EXIT_BUMP   = "chaotic_exit_bump"
R_NOT_HELD            = "not_held"
R_HELD_OK             = "held_score_ok"


class SignalDiagnostics:
    """Captures per-day, per-ticker decision data and emits summaries."""

    def __init__(self, name: str = "", enabled: bool = True,
                 verbose_top_n: int = 5, verbose_sells: int = 3):
        self.name = name
        self.enabled = enabled
        self.verbose_top_n = verbose_top_n
        self.verbose_sells = verbose_sells

        # accumulation across entire backtest
        self.daily_records: list[dict] = []
        self._rec: dict | None = None

    # ── per-day lifecycle ───────────────────────────────────────────────

    def begin_day(
        self,
        date,
        vol_regime: str,
        breadth_regime: str,
        base_entry: float,
        base_exit: float,
        regime_entry_adj: float,
        breadth_entry_adj: float,
        adjusted_entry: float,
        adjusted_exit: float,
        size_multiplier: float = 1.0,
        n_held: int = 0,
        max_positions: int = 0,
    ):
        """Call once at the start of each trading day, after computing
        regime adjustments but before iterating over tickers."""
        if not self.enabled:
            return
        self._rec = {
            "date": date,
            "vol_regime": vol_regime,
            "breadth_regime": breadth_regime,
            "base_entry": base_entry,
            "base_exit": base_exit,
            "regime_entry_adj": regime_entry_adj,
            "breadth_entry_adj": breadth_entry_adj,
            "adjusted_entry": adjusted_entry,
            "adjusted_exit": adjusted_exit,
            "size_multiplier": size_multiplier,
            "n_held": n_held,
            "max_positions": max_positions,
            # populated by log_score / log_decision
            "composites": {},          # ticker -> float
            "sub_scores": {},          # ticker -> dict
            "ranks": {},               # ticker -> float
            "rs_regimes": {},          # ticker -> str
            "sector_regimes": {},      # ticker -> str
            "decisions": {},           # ticker -> (action, [reasons])
            "sell_triggers": defaultdict(list),
        }

    def log_score(
        self,
        ticker: str,
        composite: float,
        rank_pct: float | None = None,
        sub_scores: dict | None = None,
        rs_regime: str | None = None,
        sector_regime: str | None = None,
    ):
        """Call for every ticker after the scoring pipeline runs."""
        if not self.enabled or self._rec is None:
            return
        self._rec["composites"][ticker] = composite
        if rank_pct is not None:
            self._rec["ranks"][ticker] = rank_pct
        if sub_scores:
            self._rec["sub_scores"][ticker] = sub_scores
        if rs_regime:
            self._rec["rs_regimes"][ticker] = rs_regime
        if sector_regime:
            self._rec["sector_regimes"][ticker] = sector_regime

    def log_decision(self, ticker: str, action: str, reasons: list[str]):
        """Call when a final BUY / SELL / HOLD / BLOCKED decision is made.

        action: one of 'BUY', 'SELL', 'HOLD', 'BLOCKED'
        reasons: list of R_* tags (or free-form strings)
        """
        if not self.enabled or self._rec is None:
            return
        self._rec["decisions"][ticker] = (action, reasons)
        if action == "SELL":
            for r in reasons:
                self._rec["sell_triggers"][r].append(ticker)

    def end_day(self):
        """Call at end of day. Logs summary lines and archives the record."""
        if not self.enabled or self._rec is None:
            return
        rec = self._rec
        scores = np.array(list(rec["composites"].values())) if rec["composites"] else np.array([])
        ranks = np.array(list(rec["ranks"].values())) if rec["ranks"] else np.array([])

        # ── 1. score distribution vs thresholds ────────────────────────
        if len(scores) > 0:
            p = np.percentile(scores, [5, 10, 25, 50, 75, 90, 95])
            above_entry = int(np.sum(scores >= rec["adjusted_entry"]))
            in_band = int(np.sum(
                (scores >= rec["adjusted_exit"]) & (scores < rec["adjusted_entry"])
            ))
            below_exit = int(np.sum(scores < rec["adjusted_exit"]))

            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SCORES  "
                f"n={len(scores)}  "
                f"p5={p[0]:.3f} p10={p[1]:.3f} p25={p[2]:.3f} "
                f"p50={p[3]:.3f} p75={p[4]:.3f} p90={p[5]:.3f} p95={p[6]:.3f}  "
                f"above_entry={above_entry}  in_band={in_band}  below_exit={below_exit}"
            )

        # ── 2. threshold computation trace ─────────────────────────────
        logger.info(
            f"[{self.name}] {rec['date']}  DIAG-THRESH  "
            f"vol={rec['vol_regime']}  breadth={rec['breadth_regime']}  "
            f"base_entry={rec['base_entry']:.3f}  "
            f"+regime_adj={rec['regime_entry_adj']:+.3f}  "
            f"+breadth_adj={rec['breadth_entry_adj']:+.3f}  "
            f"= adjusted_entry={rec['adjusted_entry']:.3f}  "
            f"adjusted_exit={rec['adjusted_exit']:.3f}  "
            f"size_mult={rec['size_multiplier']:.2f}  "
            f"held={rec['n_held']}/{rec['max_positions']}"
        )

        # ── 3. rank distribution (if populated) ────────────────────────
        if len(ranks) > 0:
            rp = np.percentile(ranks, [10, 50, 90])
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"n={len(ranks)}  p10={rp[0]:.3f}  p50={rp[1]:.3f}  p90={rp[2]:.3f}"
            )
        else:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-RANKS   "
                f"** NO RANK DATA — rank keys may not be wired **"
            )

        # ── 4. top N scores with full breakdown ────────────────────────
        if rec["composites"]:
            top = sorted(rec["composites"].items(), key=lambda x: x[1], reverse=True)
            for ticker, comp in top[: self.verbose_top_n]:
                sub = rec["sub_scores"].get(ticker, {})
                rank = rec["ranks"].get(ticker)
                rs = rec["rs_regimes"].get(ticker, "?")
                sec = rec["sector_regimes"].get(ticker, "?")
                dec_action, dec_reasons = rec["decisions"].get(ticker, ("?", []))

                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                rank_str = f"rank={rank:.3f}" if rank is not None else "rank=N/A"

                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-TOP     "
                    f"{ticker:20s}  comp={comp:.4f}  {rank_str}  "
                    f"rs={rs}  sec={sec}  {sub_str}  "
                    f"→ {dec_action}  {dec_reasons}"
                )

        # ── 5. sell trigger breakdown ──────────────────────────────────
        if rec["sell_triggers"]:
            counts = {k: len(v) for k, v in rec["sell_triggers"].items()}
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-SELLS   {counts}"
            )
            # show a few examples of score-below-exit sells
            score_sells = rec["sell_triggers"].get(R_SCORE_BELOW_EXIT, [])
            for ticker in score_sells[: self.verbose_sells]:
                comp = rec["composites"].get(ticker, 0)
                sub = rec["sub_scores"].get(ticker, {})
                sub_str = "  ".join(f"{k}={v:.3f}" for k, v in sub.items())
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-SELL-EX "
                    f"{ticker:20s}  comp={comp:.4f}  {sub_str}"
                )

        # ── 6. blocked buys (score above entry but blocked by filter) ──
        blocked = [
            (t, d) for t, d in rec["decisions"].items() if d[0] == "BLOCKED"
        ]
        if blocked:
            logger.info(
                f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                f"{len(blocked)} tickers passed score threshold but were blocked"
            )
            for ticker, (_, reasons) in blocked[: self.verbose_top_n]:
                comp = rec["composites"].get(ticker, 0)
                logger.info(
                    f"[{self.name}] {rec['date']}  DIAG-BLOCKED "
                    f"{ticker:20s}  comp={comp:.4f}  {reasons}"
                )

        self.daily_records.append(rec)
        self._rec = None

    # ── end-of-backtest summary ─────────────────────────────────────────

    def print_summary(self):
        """Call once after the backtest loop finishes."""
        if not self.daily_records:
            logger.info(f"[{self.name}] DIAG-SUMMARY  no data recorded")
            return

        n_days = len(self.daily_records)
        all_scores = []
        all_entries = []
        all_exits = []
        action_counts = defaultdict(int)
        trigger_counts = defaultdict(int)
        regime_day_counts = defaultdict(int)
        breadth_day_counts = defaultdict(int)
        days_with_rank = 0

        for rec in self.daily_records:
            all_scores.extend(rec["composites"].values())
            all_entries.append(rec["adjusted_entry"])
            all_exits.append(rec["adjusted_exit"])
            regime_day_counts[rec["vol_regime"]] += 1
            breadth_day_counts[rec["breadth_regime"]] += 1
            if rec["ranks"]:
                days_with_rank += 1
            for _, (action, reasons) in rec["decisions"].items():
                action_counts[action] += 1
                if action == "SELL":
                    for r in reasons:
                        trigger_counts[r] += 1

        scores_arr = np.array(all_scores) if all_scores else np.array([0])
        entries_arr = np.array(all_entries)
        exits_arr = np.array(all_exits)

        logger.info(f"[{self.name}] {'=' * 60}")
        logger.info(f"[{self.name}] DIAGNOSTIC SUMMARY  ({n_days} trading days)")
        logger.info(f"[{self.name}] {'=' * 60}")

        logger.info(
            f"[{self.name}]   Score distribution (all ticker-days):  "
            f"mean={scores_arr.mean():.4f}  std={scores_arr.std():.4f}  "
            f"min={scores_arr.min():.4f}  max={scores_arr.max():.4f}"
        )
        p = np.percentile(scores_arr, [5, 25, 50, 75, 90, 95, 99])
        logger.info(
            f"[{self.name}]   Score percentiles:  "
            f"p5={p[0]:.4f}  p25={p[1]:.4f}  p50={p[2]:.4f}  "
            f"p75={p[3]:.4f}  p90={p[4]:.4f}  p95={p[5]:.4f}  p99={p[6]:.4f}"
        )

        pct_above_entry = 100.0 * np.mean(scores_arr >= entries_arr.mean())
        pct_below_exit = 100.0 * np.mean(scores_arr < exits_arr.mean())
        logger.info(
            f"[{self.name}]   Avg entry threshold: {entries_arr.mean():.4f}  "
            f"Avg exit threshold: {exits_arr.mean():.4f}"
        )
        logger.info(
            f"[{self.name}]   %% ticker-days above avg entry: {pct_above_entry:.1f}%%  "
            f"below avg exit: {pct_below_exit:.1f}%%"
        )

        logger.info(f"[{self.name}]   Vol regime days:     {dict(regime_day_counts)}")
        logger.info(f"[{self.name}]   Breadth regime days:  {dict(breadth_day_counts)}")
        logger.info(f"[{self.name}]   Days with rank data:  {days_with_rank}/{n_days}")

        logger.info(f"[{self.name}]   Decision totals:      {dict(action_counts)}")
        if trigger_counts:
            logger.info(f"[{self.name}]   Sell trigger totals:  {dict(trigger_counts)}")

        # gap analysis: how far is p90 score from entry threshold?
        daily_gaps = []
        for rec in self.daily_records:
            s = list(rec["composites"].values())
            if s:
                daily_gaps.append(np.percentile(s, 90) - rec["adjusted_entry"])
        if daily_gaps:
            gaps = np.array(daily_gaps)
            logger.info(
                f"[{self.name}]   Daily (p90_score - entry_thresh):  "
                f"mean={gaps.mean():+.4f}  min={gaps.min():+.4f}  max={gaps.max():+.4f}"
            )
            if gaps.mean() < 0:
                logger.warning(
                    f"[{self.name}]   ⚠ p90 score is BELOW entry threshold on average. "
                    f"Scoring pipeline may be systematically too low, or thresholds too high."
                )

        logger.info(f"[{self.name}] {'=' * 60}")