"""
strategy/convergence.py
-----------------------
Dual-list signal convergence for US; scoring-only passthrough
for HK and India.

Architecture
────────────
  US:  scoring engine (bottom-up) + rotation engine (top-down)
       → merge_convergence() → ConvergedSignal list

  HK:  scoring engine only (vs 2800.HK benchmark)
       → scoring_passthrough() → ConvergedSignal list

  IN:  scoring engine only (vs NIFTYBEES.NS benchmark)
       → scoring_passthrough() → ConvergedSignal list

The value of the dual list isn't either list in isolation — it's
the convergence.

  STRONG_BUY:  BUY on BOTH rotation + scoring.  The stock is in
               a leading sector AND scores well on its own merits.
               This is the highest conviction signal.

  BUY_SCORING: BUY on scoring only.  Good individual profile but
               sector isn't leading.  Still tradeable — just lower
               conviction than STRONG_BUY.

  BUY_ROTATION: BUY on rotation only.  In a leading sector but
                individual metrics don't confirm yet.  Watch for
                scoring improvement.

  CONFLICT:    One says BUY, the other says SELL.  A strong name
               swimming against the sector tide, or a weak name
               dragged up by its sector.  Flag for manual review.

  STRONG_SELL: SELL on BOTH.  Highest conviction exit.

Convergence scoring adjustment:
  STRONG_BUY  → composite + convergence_boost  (default +0.10)
  CONFLICT    → composite + conflict_penalty   (default −0.05)

Parameters live in common/config.py → US_CONVERGENCE.

Pipeline
────────
  Scoring snapshots         Rotation result
  (pipeline/runner.py)      (strategy/rotation.py)
        │                         │
        └──────────┬──────────────┘
                   ↓
         merge_convergence()        ← US only
                   ↓
         list[ConvergedSignal]
                   ↓
         MarketSignalResult
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from common.config import MARKET_CONFIG

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONVERGENCE LABELS
# ═══════════════════════════════════════════════════════════════

STRONG_BUY    = "STRONG_BUY"
BUY_SCORING   = "BUY_SCORING"
BUY_ROTATION  = "BUY_ROTATION"
CONFLICT      = "CONFLICT"
STRONG_SELL   = "STRONG_SELL"
SELL_SCORING  = "SELL_SCORING"
SELL_ROTATION = "SELL_ROTATION"
HOLD          = "HOLD"
NEUTRAL       = "NEUTRAL"

_BUY_LABELS  = {STRONG_BUY, BUY_SCORING, BUY_ROTATION}
_SELL_LABELS = {STRONG_SELL, SELL_SCORING, SELL_ROTATION}


# ═══════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class ConvergedSignal:
    """One ticker's merged signal from both engines."""

    ticker: str
    convergence_label: str

    # ── Scoring engine data ────────────────────────────────────
    scoring_signal: str | None       # BUY / HOLD / SELL / None
    composite_score: float           # raw composite from scoring
    adjusted_score: float            # after convergence boost/penalty
    scoring_regime: str              # rs_regime from scoring engine
    scoring_confirmed: bool          # sig_confirmed == 1

    # ── Rotation engine data ───────────────────────────────────
    rotation_signal: str | None      # BUY / SELL / HOLD / REDUCE / None
    rotation_rs: float               # composite RS from rotation
    rotation_sector: str | None      # sector name
    rotation_sector_rank: int        # 1 = strongest, 99 = unknown
    rotation_sector_tier: str        # Leading / Neutral / Lagging / n/a
    rotation_reason: str             # human-readable reason

    # ── Assigned after sorting ─────────────────────────────────
    rank: int = 0

    @property
    def is_buy(self) -> bool:
        return self.convergence_label in _BUY_LABELS

    @property
    def is_sell(self) -> bool:
        return self.convergence_label in _SELL_LABELS

    @property
    def is_conflict(self) -> bool:
        return self.convergence_label == CONFLICT

    def to_dict(self) -> dict:
        return {
            "ticker":               self.ticker,
            "convergence_label":    self.convergence_label,
            "scoring_signal":       self.scoring_signal,
            "composite_score":      round(self.composite_score, 4),
            "adjusted_score":       round(self.adjusted_score, 4),
            "scoring_regime":       self.scoring_regime,
            "scoring_confirmed":    self.scoring_confirmed,
            "rotation_signal":      self.rotation_signal,
            "rotation_rs":          round(self.rotation_rs, 4),
            "rotation_sector":      self.rotation_sector,
            "rotation_sector_rank": self.rotation_sector_rank,
            "rotation_sector_tier": self.rotation_sector_tier,
            "rotation_reason":      self.rotation_reason,
            "rank":                 self.rank,
        }


@dataclass
class MarketSignalResult:
    """Complete convergence output for one market."""

    market: str
    signals: list[ConvergedSignal] = field(default_factory=list)

    @property
    def buys(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_buy]

    @property
    def sells(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_sell]

    @property
    def conflicts(self) -> list[ConvergedSignal]:
        return [s for s in self.signals if s.is_conflict]

    @property
    def holds(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == HOLD]

    @property
    def strong_buys(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == STRONG_BUY]

    @property
    def strong_sells(self) -> list[ConvergedSignal]:
        return [s for s in self.signals
                if s.convergence_label == STRONG_SELL]

    @property
    def n_tickers(self) -> int:
        return len(self.signals)

    def get_signal(self, ticker: str) -> ConvergedSignal | None:
        for s in self.signals:
            if s.ticker == ticker:
                return s
        return None

    def to_dataframe(self) -> pd.DataFrame:
        if not self.signals:
            return pd.DataFrame()
        return pd.DataFrame([s.to_dict() for s in self.signals])

    def summary(self) -> str:
        parts = [f"[{self.market}] {self.n_tickers} tickers"]
        label_counts: dict[str, int] = {}
        for s in self.signals:
            label_counts[s.convergence_label] = (
                label_counts.get(s.convergence_label, 0) + 1
            )
        for label in [STRONG_BUY, BUY_SCORING, BUY_ROTATION,
                      HOLD, NEUTRAL, CONFLICT,
                      SELL_SCORING, SELL_ROTATION, STRONG_SELL]:
            cnt = label_counts.get(label, 0)
            if cnt > 0:
                parts.append(f"{label}={cnt}")
        return "  ".join(parts)


# ═══════════════════════════════════════════════════════════════
#  MERGE CONVERGENCE  (US dual-list)
# ═══════════════════════════════════════════════════════════════

def merge_convergence(
    scoring_snapshots: list[dict],
    rotation_result,
    convergence_config: dict | None = None,
) -> list[ConvergedSignal]:
    """
    Merge scoring engine snapshots with rotation engine result
    into a unified signal set with convergence labels.

    Parameters
    ----------
    scoring_snapshots : list[dict]
        Output of ``results_to_snapshots()`` — per-ticker dicts
        with keys: ticker, composite, signal, sig_confirmed,
        rs_regime, sig_exit, etc.
    rotation_result : RotationResult
        Output of ``strategy.rotation.run_rotation()``.
    convergence_config : dict, optional
        Convergence parameters.  Defaults to
        ``common.config.US_CONVERGENCE``.

    Returns
    -------
    list[ConvergedSignal]
        One per ticker, sorted by adjusted_score descending,
        with rank assigned.
    """
    if convergence_config is None:
        convergence_config = MARKET_CONFIG["US"].get("convergence", {})
    if convergence_config is None:
        convergence_config = {}

    boost   = convergence_config.get("convergence_boost", 0.10)
    penalty = convergence_config.get("conflict_penalty", -0.05)

    # ── Index rotation recommendations by ticker ──────────────
    rot_map: dict[str, dict] = {}
    if rotation_result is not None:
        for rec in rotation_result.recommendations:
            rot_map[rec.ticker] = {
                "action":       rec.action.value,
                "rs":           rec.rs_composite,
                "rs_vs_sector": rec.rs_vs_sector_etf,
                "sector":       rec.sector,
                "sector_rank":  rec.sector_rank,
                "sector_tier":  rec.sector_tier,
                "reason":       rec.reason,
            }

    # ── Walk every scored ticker ──────────────────────────────
    signals: list[ConvergedSignal] = []
    processed: set[str] = set()

    for snap in scoring_snapshots:
        ticker = snap.get("ticker", "???")
        processed.add(ticker)

        score     = snap.get("composite", 0.0)
        scr_sig   = snap.get("signal", "HOLD")
        confirmed = snap.get("sig_confirmed", 0) == 1
        regime    = snap.get("rs_regime", "unknown")
        sig_exit  = snap.get("sig_exit", 0) == 1

        # Normalise scoring engine's opinion
        s_buy  = scr_sig in ("BUY", "STRONG_BUY") or confirmed
        s_sell = scr_sig in ("SELL", "STRONG_SELL") or sig_exit

        # Look up rotation engine's opinion
        rot = rot_map.get(ticker, {})
        r_action = rot.get("action")   # BUY / SELL / HOLD / REDUCE / None

        r_buy  = r_action == "BUY"
        r_sell = r_action in ("SELL", "REDUCE")

        # ── Convergence logic ─────────────────────────────────
        label, adj = _classify(
            s_buy, s_sell, r_buy, r_sell,
            r_action, scr_sig, score, boost, penalty,
        )

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=scr_sig,
            composite_score=score,
            adjusted_score=adj,
            scoring_regime=regime,
            scoring_confirmed=confirmed,
            rotation_signal=r_action,
            rotation_rs=rot.get("rs", 0.0),
            rotation_sector=rot.get("sector"),
            rotation_sector_rank=rot.get("sector_rank", 99),
            rotation_sector_tier=rot.get("sector_tier", "n/a"),
            rotation_reason=rot.get("reason", ""),
        ))

    # ── Rotation-only tickers (not in scoring universe) ───────
    for ticker, rot in rot_map.items():
        if ticker in processed:
            continue

        r_action = rot["action"]
        if r_action == "BUY":
            label = BUY_ROTATION
        elif r_action in ("SELL", "REDUCE"):
            label = SELL_ROTATION
        else:
            label = HOLD

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=None,
            composite_score=0.0,
            adjusted_score=0.0,
            scoring_regime="unknown",
            scoring_confirmed=False,
            rotation_signal=r_action,
            rotation_rs=rot.get("rs", 0.0),
            rotation_sector=rot.get("sector"),
            rotation_sector_rank=rot.get("sector_rank", 99),
            rotation_sector_tier=rot.get("sector_tier", "n/a"),
            rotation_reason=rot.get("reason", ""),
        ))

    # ── Rank by adjusted score ────────────────────────────────
    signals.sort(key=lambda s: s.adjusted_score, reverse=True)
    for i, sig in enumerate(signals, 1):
        sig.rank = i

    return signals


def _classify(
    s_buy: bool, s_sell: bool,
    r_buy: bool, r_sell: bool,
    r_action: str | None,
    scr_sig: str,
    score: float,
    boost: float,
    penalty: float,
) -> tuple[str, float]:
    """
    Apply convergence classification logic.

    Returns (label, adjusted_score).
    """
    if s_buy and r_buy:
        return STRONG_BUY, min(1.0, score + boost)

    if s_buy and r_sell:
        return CONFLICT, max(0.0, score + penalty)

    if s_sell and r_buy:
        return CONFLICT, max(0.0, score + penalty)

    if s_sell and r_sell:
        return STRONG_SELL, max(0.0, score + penalty)

    if s_buy and not r_sell:
        return BUY_SCORING, score

    if r_buy and not s_sell:
        return BUY_ROTATION, score

    if s_sell and not r_buy:
        return SELL_SCORING, score

    if r_sell and not s_buy:
        return SELL_ROTATION, score

    if r_action == "HOLD" or scr_sig == "HOLD":
        return HOLD, score

    return NEUTRAL, score


# ═══════════════════════════════════════════════════════════════
#  SCORING-ONLY PASSTHROUGH  (HK, IN)
# ═══════════════════════════════════════════════════════════════

def scoring_passthrough(
    scoring_snapshots: list[dict],
    market: str = "HK",
) -> list[ConvergedSignal]:
    """
    Convert scoring-only snapshots into ConvergedSignal objects.

    No rotation engine → no dual-list merge.  The convergence
    label simply reflects the scoring engine's assessment.

    Parameters
    ----------
    scoring_snapshots : list[dict]
        Same format as ``merge_convergence`` input.
    market : str
        Market label for logging.

    Returns
    -------
    list[ConvergedSignal]
        Sorted by composite_score descending, ranked.
    """
    signals: list[ConvergedSignal] = []

    for snap in scoring_snapshots:
        ticker    = snap.get("ticker", "???")
        score     = snap.get("composite", 0.0)
        scr_sig   = snap.get("signal", "HOLD")
        confirmed = snap.get("sig_confirmed", 0) == 1
        regime    = snap.get("rs_regime", "unknown")
        sig_exit  = snap.get("sig_exit", 0) == 1

        # Map scoring signal → convergence label
        if scr_sig in ("BUY", "STRONG_BUY") or confirmed:
            label = BUY_SCORING
        elif scr_sig in ("SELL", "STRONG_SELL") or sig_exit:
            label = SELL_SCORING
        elif scr_sig == "HOLD":
            label = HOLD
        else:
            label = NEUTRAL

        signals.append(ConvergedSignal(
            ticker=ticker,
            convergence_label=label,
            scoring_signal=scr_sig,
            composite_score=score,
            adjusted_score=score,
            scoring_regime=regime,
            scoring_confirmed=confirmed,
            rotation_signal=None,
            rotation_rs=0.0,
            rotation_sector=None,
            rotation_sector_rank=99,
            rotation_sector_tier="n/a",
            rotation_reason="",
        ))

    signals.sort(key=lambda s: s.adjusted_score, reverse=True)
    for i, sig in enumerate(signals, 1):
        sig.rank = i

    return signals


# ═══════════════════════════════════════════════════════════════
#  MARKET DISPATCHER
# ═══════════════════════════════════════════════════════════════

def run_convergence(
    market: str,
    scoring_snapshots: list[dict],
    rotation_result=None,
) -> MarketSignalResult:
    """
    Route to the correct merge strategy based on market.

    Parameters
    ----------
    market : str
        "US", "HK", or "IN".
    scoring_snapshots : list[dict]
        Per-ticker snapshot dicts from the scoring pipeline.
    rotation_result : RotationResult or None
        Output of ``strategy.rotation.run_rotation()``.
        Required for US, ignored for HK/IN.

    Returns
    -------
    MarketSignalResult
    """
    cfg = MARKET_CONFIG.get(market, {})
    engines = cfg.get("engines", ["scoring"])

    if "rotation" in engines and rotation_result is not None:
        convergence_cfg = cfg.get("convergence", {})
        signals = merge_convergence(
            scoring_snapshots, rotation_result, convergence_cfg
        )
        logger.info(
            f"[{market}] Convergence merge: "
            f"{sum(1 for s in signals if s.convergence_label == STRONG_BUY)} "
            f"STRONG_BUY, "
            f"{sum(1 for s in signals if s.is_conflict)} CONFLICT, "
            f"{len(signals)} total"
        )
    else:
        signals = scoring_passthrough(scoring_snapshots, market)
        logger.info(
            f"[{market}] Scoring only: "
            f"{sum(1 for s in signals if s.is_buy)} BUY, "
            f"{sum(1 for s in signals if s.is_sell)} SELL, "
            f"{len(signals)} total"
        )

    result = MarketSignalResult(market=market, signals=signals)
    logger.info(result.summary())
    return result


# ═══════════════════════════════════════════════════════════════
#  HELPERS — enrich snapshots with convergence data
# ═══════════════════════════════════════════════════════════════

def enrich_snapshots(
    snapshots: list[dict],
    convergence: MarketSignalResult,
) -> list[dict]:
    """
    Write convergence labels and adjusted scores back into
    the snapshot dicts.

    Modifies snapshots in-place.  Downstream report generators
    can then pick up ``convergence_label`` and ``adjusted_score``
    from the snapshot without knowing about the convergence
    module directly.
    """
    sig_map = {s.ticker: s for s in convergence.signals}

    for snap in snapshots:
        ticker = snap.get("ticker")
        cs = sig_map.get(ticker)
        if cs is None:
            snap["convergence_label"] = NEUTRAL
            snap["convergence_rank"]  = 999
            continue

        snap["convergence_label"]    = cs.convergence_label
        snap["convergence_rank"]     = cs.rank
        snap["adjusted_score"]       = cs.adjusted_score
        snap["rotation_signal"]      = cs.rotation_signal
        snap["rotation_rs"]          = cs.rotation_rs
        snap["rotation_sector"]      = cs.rotation_sector
        snap["rotation_sector_rank"] = cs.rotation_sector_rank
        snap["rotation_sector_tier"] = cs.rotation_sector_tier
        snap["rotation_reason"]      = cs.rotation_reason

    return snapshots


# ═══════════════════════════════════════════════════════════════
#  PRICE MATRIX BUILDER
# ═══════════════════════════════════════════════════════════════

def build_price_matrix(
    ohlcv: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Build a wide-format adjusted-close matrix from per-ticker
    OHLCV DataFrames.

    The rotation engine expects:
      DatetimeIndex × ticker columns → close prices.

    Forward-fills gaps so the matrix is dense.

    Parameters
    ----------
    ohlcv : dict[str, pd.DataFrame]
        {ticker: OHLCV DataFrame with 'close' column}

    Returns
    -------
    pd.DataFrame
        Wide matrix, or empty DataFrame if no valid data.
    """
    frames: dict[str, pd.Series] = {}
    for ticker, df in ohlcv.items():
        if df is None or df.empty:
            continue
        if "close" in df.columns:
            frames[ticker] = df["close"]

    if not frames:
        return pd.DataFrame()

    matrix = pd.DataFrame(frames)
    matrix = matrix.sort_index().ffill()
    return matrix


# ═══════════════════════════════════════════════════════════════
#  PRETTY PRINT
# ═══════════════════════════════════════════════════════════════

def convergence_report(result: MarketSignalResult) -> str:
    """
    Format a MarketSignalResult as a human-readable text report.
    """
    ln: list[str] = []
    w = 76
    div = "=" * w
    sub = "-" * w
    market = result.market

    ln.append(div)
    ln.append(f"  {market} CONVERGENCE REPORT")
    ln.append(div)
    ln.append(f"  {result.summary()}")

    # ── Strong buys ───────────────────────────────────────────
    if result.strong_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢🟢  STRONG BUY  ({len(result.strong_buys)})")
        ln.append(f"  Both scoring + rotation agree — highest conviction")
        ln.append(sub)
        for s in result.strong_buys:
            ln.append(_fmt_signal(s))

    # ── Buy (scoring only) ────────────────────────────────────
    scr_buys = [s for s in result.signals
                if s.convergence_label == BUY_SCORING]
    if scr_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢  BUY — Scoring Only  ({len(scr_buys)})")
        ln.append(f"  Strong individual profile, sector not leading")
        ln.append(sub)
        for s in scr_buys:
            ln.append(_fmt_signal(s))

    # ── Buy (rotation only) ───────────────────────────────────
    rot_buys = [s for s in result.signals
                if s.convergence_label == BUY_ROTATION]
    if rot_buys:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🟢  BUY — Rotation Only  ({len(rot_buys)})")
        ln.append(f"  In leading sector, individual metrics not confirmed")
        ln.append(sub)
        for s in rot_buys:
            ln.append(_fmt_signal(s))

    # ── Conflicts ─────────────────────────────────────────────
    if result.conflicts:
        ln.append("")
        ln.append(sub)
        ln.append(f"  ⚠️  CONFLICT  ({len(result.conflicts)})")
        ln.append(f"  Engines disagree — review manually")
        ln.append(sub)
        for s in result.conflicts:
            ln.append(_fmt_signal(s))

    # ── Sells ─────────────────────────────────────────────────
    if result.sells:
        ln.append("")
        ln.append(sub)
        ln.append(f"  🔴  SELL  ({len(result.sells)})")
        ln.append(sub)
        for s in result.sells:
            ln.append(_fmt_signal(s))

    # ── Holds (abbreviated) ───────────────────────────────────
    holds = result.holds
    if holds:
        ln.append("")
        ln.append(sub)
        ln.append(f"  ⚪  HOLD  ({len(holds)})")
        ln.append(sub)
        for s in holds[:10]:
            ln.append(
                f"    #{s.rank:<3d}  {s.ticker:<8s}  "
                f"score={s.composite_score:.3f}"
            )
        if len(holds) > 10:
            ln.append(f"    ... and {len(holds) - 10} more")

    ln.append("")
    ln.append(div)
    return "\n".join(ln)


def _fmt_signal(s: ConvergedSignal) -> str:
    """Format one ConvergedSignal for text display."""
    parts = [
        f"    #{s.rank:<3d}  {s.ticker:<8s}  "
        f"score={s.composite_score:.3f}  "
        f"adj={s.adjusted_score:.3f}"
    ]

    if s.scoring_signal:
        confirmed = "✓" if s.scoring_confirmed else "✗"
        parts.append(
            f"  scr={s.scoring_signal}[{confirmed}] "
            f"regime={s.scoring_regime}"
        )

    if s.rotation_signal:
        parts.append(
            f"  rot={s.rotation_signal} "
            f"RS={s.rotation_rs:+.3f}"
        )
        if s.rotation_sector:
            parts.append(
                f"  sector={s.rotation_sector} "
                f"(#{s.rotation_sector_rank} {s.rotation_sector_tier})"
            )

    return "".join(parts)