"""
STRATEGY:
-----------------------------
"""
"""
strategy/portfolio.py
---------------------
Multi-stock portfolio construction.

Takes signal DataFrames from multiple tickers (output of
``generate_signals``), ranks candidates cross-sectionally,
enforces concentration limits, and outputs a target portfolio.

Pipeline
────────
  {ticker: signaled_df}  for N stocks
       ↓
  extract_snapshots()     — latest-day data per ticker
       ↓
  filter_candidates()     — keep sig_confirmed == 1
       ↓
  rank_candidates()       — sort by score + incumbent bonus
       ↓
  select_positions()      — top N, enforce sector caps
       ↓
  compute_weights()       — normalise to target allocation
       ↓
  build_portfolio()       — master orchestrator
       ↓
  compute_rebalance()     — diff vs current → trade list

Breadth Integration
───────────────────
  When breadth data is passed to ``build_portfolio()``, the
  target invested percentage is scaled by the breadth regime:
    strong  → 100 % of target_invested_pct
    neutral →  80 %
    weak    →  50 %

  This is a portfolio-level risk dial that sits on top of the
  per-stock breadth gate already applied in signals.py.

Output
──────
  dict with target_weights, holdings DataFrame, sector exposure,
  rebalance trades, and metadata.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import (
    PORTFOLIO_PARAMS,
    SIGNAL_PARAMS,
    BREADTH_PORTFOLIO,
)


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _pp(key: str):
    """Fetch from PORTFOLIO_PARAMS."""
    return PORTFOLIO_PARAMS[key]


def _bpp(key: str):
    """Fetch from BREADTH_PORTFOLIO."""
    return BREADTH_PORTFOLIO[key]


def _get_score(row) -> float:
    """Pick best available score from a snapshot row."""
    for col in ("score_adjusted", "score_composite"):
        val = row.get(col, np.nan)
        if pd.notna(val):
            return float(val)
    return 0.0


# ═══════════════════════════════════════════════════════════════
#  BREADTH EXPOSURE ADJUSTMENT
# ═══════════════════════════════════════════════════════════════

def _breadth_exposure_multiplier(
    breadth: pd.DataFrame | None,
) -> tuple[float, str]:
    """
    Determine portfolio exposure multiplier from latest breadth.

    Returns
    -------
    multiplier : float — scale factor for target_invested_pct
    regime     : str   — breadth regime label
    """
    if breadth is None or breadth.empty:
        return 1.0, "unknown"

    if "breadth_regime" not in breadth.columns:
        return 1.0, "unknown"

    regime = str(breadth["breadth_regime"].iloc[-1])

    mapping = {
        "strong":  _bpp("strong_exposure"),
        "neutral": _bpp("neutral_exposure"),
        "weak":    _bpp("weak_exposure"),
    }

    multiplier = mapping.get(regime, 1.0)
    return multiplier, regime


# ═══════════════════════════════════════════════════════════════
#  SNAPSHOT EXTRACTION
# ═══════════════════════════════════════════════════════════════

_SNAPSHOT_COLS = [
    "score_adjusted", "score_composite",
    "score_momentum", "score_trend", "score_volume", "score_volatility",
    "sig_confirmed", "sig_position_pct", "sig_reason",
    "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok", "sig_momentum_ok",
    "sig_in_cooldown", "sig_effective_entry_min",
    "rs_regime", "rs_zscore",
    "sect_rs_regime", "sect_rs_rank", "sect_rs_pctrank",
    "sector_name", "sector_tailwind",
    "breadth_regime", "breadth_score",
]


def extract_snapshots(
    universe: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Extract latest-day signal data for every ticker.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} where each DataFrame is the output
        of ``generate_signals()``.

    Returns
    -------
    pd.DataFrame
        One row per ticker with key signal / score columns and a
        unified ``score`` column (adjusted → composite fallback).
    """
    rows: list[dict] = []
    for ticker, df in universe.items():
        if df is None or df.empty:
            continue
        last = df.iloc[-1]
        row: dict = {"ticker": ticker, "date": df.index[-1]}
        for col in _SNAPSHOT_COLS:
            row[col] = last.get(col, np.nan)
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result["score"] = result.apply(_get_score, axis=1)
    return result


# ═══════════════════════════════════════════════════════════════
#  CANDIDATE FILTERING
# ═══════════════════════════════════════════════════════════════

def filter_candidates(
    snapshots: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split snapshots into candidates (sig_confirmed == 1) and
    rejected.

    Returns
    -------
    candidates : pd.DataFrame — stocks eligible for inclusion
    rejected   : pd.DataFrame — stocks without signal + reason
    """
    if snapshots.empty:
        empty = pd.DataFrame()
        return empty, empty

    is_long = snapshots["sig_confirmed"].fillna(0).astype(int) == 1

    candidates = snapshots[is_long].copy().reset_index(drop=True)
    rejected   = snapshots[~is_long].copy().reset_index(drop=True)

    # Annotate rejection reasons
    reasons: list[str] = []
    for _, row in rejected.iterrows():
        if row.get("sig_in_cooldown", False):
            reasons.append("cooldown")
        elif not row.get("sig_regime_ok", True):
            reasons.append(
                f"stock regime: {row.get('rs_regime', '?')}"
            )
        elif not row.get("sig_sector_ok", True):
            reasons.append(
                f"sector blocked: {row.get('sect_rs_regime', '?')}"
            )
        elif not row.get("sig_breadth_ok", True):
            reasons.append(
                f"breadth weak: {row.get('breadth_regime', '?')}"
            )
        elif not row.get("sig_momentum_ok", True):
            reasons.append("momentum unconfirmed")
        elif row.get("score", 0) < SIGNAL_PARAMS.get(
            "entry_score_min", 0.60
        ):
            reasons.append(
                f"score {row.get('score', 0):.2f} below entry"
            )
        else:
            reasons.append(str(row.get("sig_reason", "no signal")))
    rejected["rejection_reason"] = reasons

    return candidates, rejected


# ═══════════════════════════════════════════════════════════════
#  RANKING
# ═══════════════════════════════════════════════════════════════

def rank_candidates(
    candidates: pd.DataFrame,
    current_holdings: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Rank candidates by score.

    Incumbent positions receive a small bonus
    (``incumbent_bonus``) so the system doesn't churn in and
    out of names that hover near the threshold.
    """
    if candidates.empty:
        return candidates

    ranked = candidates.copy()
    bonus  = _pp("incumbent_bonus")

    if current_holdings and bonus > 0:
        ranked["incumbent"] = ranked["ticker"].isin(current_holdings)
        ranked["ranking_score"] = (
            ranked["score"]
            + ranked["incumbent"].astype(float) * bonus
        )
    else:
        ranked["incumbent"]     = False
        ranked["ranking_score"] = ranked["score"]

    ranked = (
        ranked
        .sort_values("ranking_score", ascending=False)
        .reset_index(drop=True)
    )
    ranked["rank"] = range(1, len(ranked) + 1)
    return ranked


# ═══════════════════════════════════════════════════════════════
#  SELECTION  +  SECTOR CAPS
# ═══════════════════════════════════════════════════════════════

def select_positions(
    ranked: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Walk the ranked list top→bottom and accept positions subject
    to **max_positions** and **max_sector_pct** constraints.

    Returns
    -------
    selected : pd.DataFrame — accepted (with ``raw_weight`` col)
    excluded : pd.DataFrame — dropped candidates + reason
    """
    if ranked.empty:
        return pd.DataFrame(), pd.DataFrame()

    max_pos    = _pp("max_positions")
    max_sector = _pp("max_sector_pct")
    min_w      = _pp("min_single_pct")

    selected_rows: list[pd.Series] = []
    excluded_rows: list[pd.Series] = []
    sector_weight: dict[str, float] = {}

    for _, row in ranked.iterrows():
        # ── Position limit ────────────────────────────────────
        if len(selected_rows) >= max_pos:
            r = row.copy()
            r["exclusion_reason"] = f"position limit ({max_pos})"
            excluded_rows.append(r)
            continue

        sector = row.get("sector_name", "Unknown")
        if pd.isna(sector):
            sector = "Unknown"

        raw_w = row.get("sig_position_pct", 0.05)
        if raw_w <= 0:
            raw_w = 0.05

        current_total = sector_weight.get(sector, 0.0)
        remaining     = max_sector - current_total

        # ── Sector cap ────────────────────────────────────────
        if remaining < min_w:
            r = row.copy()
            r["exclusion_reason"] = (
                f"sector cap ({sector} at {current_total:.0%})"
            )
            excluded_rows.append(r)
            continue

        actual_w = min(raw_w, remaining)

        r = row.copy()
        r["raw_weight"] = actual_w
        selected_rows.append(r)
        sector_weight[sector] = current_total + actual_w

    selected = (
        pd.DataFrame(selected_rows) if selected_rows
        else pd.DataFrame()
    )
    excluded = (
        pd.DataFrame(excluded_rows) if excluded_rows
        else pd.DataFrame()
    )
    return selected, excluded


# ═══════════════════════════════════════════════════════════════
#  WEIGHT NORMALISATION  (water-fill)
# ═══════════════════════════════════════════════════════════════

def _waterfill(
    tickers: list[str],
    raw: list[float],
    max_w: float,
    min_w: float,
    target: float,
) -> dict[str, float]:
    """
    Iterative weight normalisation with clamping.

    1. Scale so weights sum to *target*.
    2. Cap any weight above *max_w*.
    3. Remove any weight below *min_w*.
    4. Redistribute freed capital to unclamped names.
    5. Repeat until stable (usually 2–3 rounds).
    """
    weights = dict(zip(tickers, raw))

    for _ in range(10):
        weights = {k: v for k, v in weights.items() if v >= min_w * 0.5}
        if not weights:
            break

        total = sum(weights.values())
        if total <= 0:
            break

        scale   = target / total
        weights = {k: v * scale for k, v in weights.items()}

        changed = False
        for k in list(weights):
            if weights[k] > max_w:
                weights[k] = max_w
                changed = True
            elif weights[k] < min_w:
                del weights[k]
                changed = True

        if not changed:
            break

    return weights


def _enforce_sector_caps(
    weights: dict[str, float],
    sectors: dict[str, str],
    max_sector: float,
) -> dict[str, float]:
    """
    Post-normalisation sector-cap enforcement.

    If scaling pushed any sector above the limit, proportionally
    reduce its positions.  Freed capital becomes cash (not
    redistributed) to avoid infinite loops.
    """
    for _ in range(5):
        totals: dict[str, float] = {}
        for tk, w in weights.items():
            s = sectors.get(tk, "Unknown")
            totals[s] = totals.get(s, 0.0) + w

        any_over = False
        for s, t in totals.items():
            if t > max_sector + 0.001:
                scale = max_sector / t
                for tk in weights:
                    if sectors.get(tk, "Unknown") == s:
                        weights[tk] *= scale
                any_over = True

        if not any_over:
            break
    return weights


def compute_weights(
    selected: pd.DataFrame,
    target_override: float | None = None,
) -> pd.DataFrame:
    """
    Normalise ``raw_weight`` into final portfolio weights via
    water-fill, then enforce sector caps a second time (scaling
    can push totals above the cap set during selection).

    Parameters
    ----------
    selected : pd.DataFrame
        Rows from ``select_positions`` with ``raw_weight`` column.
    target_override : float, optional
        When provided, overrides ``target_invested_pct`` from
        config.  Used by breadth integration to scale exposure.
    """
    if selected.empty or "raw_weight" not in selected.columns:
        return selected

    target = (
        target_override
        if target_override is not None
        else _pp("target_invested_pct")
    )
    max_w = _pp("max_single_pct")
    min_w = _pp("min_single_pct")

    tickers = selected["ticker"].tolist()
    raw     = selected["raw_weight"].tolist()

    normalised = _waterfill(tickers, raw, max_w, min_w, target)

    # Build sector map for post-norm cap check
    sectors: dict[str, str] = {}
    for _, row in selected.iterrows():
        s = row.get("sector_name", "Unknown")
        sectors[row["ticker"]] = s if pd.notna(s) else "Unknown"

    normalised = _enforce_sector_caps(
        normalised, sectors, _pp("max_sector_pct")
    )

    kept = selected[selected["ticker"].isin(normalised)].copy()
    kept["weight"] = kept["ticker"].map(normalised)
    kept = (
        kept
        .sort_values("weight", ascending=False)
        .reset_index(drop=True)
    )
    return kept


# ═══════════════════════════════════════════════════════════════
#  REBALANCE
# ═══════════════════════════════════════════════════════════════

def compute_rebalance(
    target_weights: dict[str, float],
    current_weights: dict[str, float],
    threshold: float | None = None,
) -> pd.DataFrame:
    """
    Compare target vs current portfolio and produce a trade list.

    Actions: BUY (new), SELL (close), ADD (increase),
    TRIM (decrease), HOLD (change below threshold).
    """
    if threshold is None:
        threshold = _pp("rebalance_threshold")

    all_tickers = sorted(
        set(list(target_weights) + list(current_weights))
    )

    rows: list[dict] = []
    for tk in all_tickers:
        curr = current_weights.get(tk, 0.0)
        tgt  = target_weights.get(tk, 0.0)
        diff = tgt - curr

        if abs(diff) < threshold:
            action = "HOLD"
        elif diff > 0 and curr == 0:
            action = "BUY"
        elif diff < 0 and tgt == 0:
            action = "SELL"
        elif diff > 0:
            action = "ADD"
        else:
            action = "TRIM"

        rows.append({
            "ticker":         tk,
            "current_weight": round(curr, 4),
            "target_weight":  round(tgt, 4),
            "delta":          round(diff, 4),
            "action":         action,
        })

    result = pd.DataFrame(rows)
    order  = {"SELL": 0, "TRIM": 1, "HOLD": 2, "ADD": 3, "BUY": 4}
    result["_s"] = result["action"].map(order)
    result = (
        result
        .sort_values(["_s", "delta"], ascending=[True, True])
        .drop(columns="_s")
        .reset_index(drop=True)
    )
    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def build_portfolio(
    universe: dict[str, pd.DataFrame],
    current_holdings: dict[str, float] | None = None,
    breadth: pd.DataFrame | None = None,
) -> dict:
    """
    Full portfolio construction pipeline.

    Parameters
    ----------
    universe : dict
        {ticker: DataFrame} — each DataFrame is the output
        of ``generate_signals()``.
    current_holdings : dict, optional
        {ticker: weight} of the current portfolio.  When
        provided, incumbents get a ranking bonus and a rebalance
        trade list is generated.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()``.  When provided,
        the target invested percentage is scaled by the breadth
        regime (strong / neutral / weak).

    Returns
    -------
    dict
        ===============  ==========================================
        snapshots        latest-day row for every ticker
        candidates       tickers with active long signal
        rejected         tickers without signal + reason
        holdings         final portfolio with weights
        excluded         candidates that didn't make the cut
        target_weights   {ticker: weight}
        sector_exposure  {sector: total_weight}
        trades           rebalance trades (None if no current)
        metadata         summary statistics
        ===============  ==========================================
    """
    snapshots = extract_snapshots(universe)

    if snapshots.empty:
        return _empty_result(current_holdings)

    # ── Breadth exposure adjustment ───────────────────────────
    breadth_mult, breadth_regime = _breadth_exposure_multiplier(breadth)
    base_target    = _pp("target_invested_pct")
    adjusted_target = base_target * breadth_mult

    # ── Filter ────────────────────────────────────────────────
    candidates, rejected = filter_candidates(snapshots)

    if candidates.empty:
        return _no_candidate_result(
            snapshots, rejected, current_holdings,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        )

    # ── Rank ──────────────────────────────────────────────────
    ranked = rank_candidates(candidates, current_holdings)

    # ── Select ────────────────────────────────────────────────
    selected, excluded = select_positions(ranked)

    if selected.empty:
        return _no_candidate_result(
            snapshots, rejected, current_holdings, excluded,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        )

    # ── Weight (breadth-adjusted target) ──────────────────────
    holdings = compute_weights(selected, target_override=adjusted_target)

    target_weights = (
        dict(zip(holdings["ticker"], holdings["weight"]))
        if not holdings.empty else {}
    )

    # ── Sector exposure ───────────────────────────────────────
    sector_exposure = _calc_sector_exposure(holdings)

    # ── Rebalance ─────────────────────────────────────────────
    trades = None
    if current_holdings is not None:
        trades = compute_rebalance(target_weights, current_holdings)

    return {
        "snapshots":       snapshots,
        "candidates":      candidates,
        "rejected":        rejected,
        "holdings":        holdings,
        "excluded":        excluded,
        "target_weights":  target_weights,
        "sector_exposure": sector_exposure,
        "trades":          trades,
        "metadata":        _build_metadata(
            holdings, snapshots,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        ),
    }


# ═══════════════════════════════════════════════════════════════
#  INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════

def _calc_sector_exposure(holdings: pd.DataFrame) -> dict[str, float]:
    if holdings.empty or "sector_name" not in holdings.columns:
        return {}
    exp: dict[str, float] = {}
    for sec in holdings["sector_name"].dropna().unique():
        mask = holdings["sector_name"] == sec
        exp[str(sec)] = round(
            float(holdings.loc[mask, "weight"].sum()), 4
        )
    return dict(sorted(exp.items(), key=lambda x: -x[1]))


def _build_metadata(
    holdings: pd.DataFrame,
    snapshots: pd.DataFrame,
    breadth_regime: str = "unknown",
    breadth_mult: float = 1.0,
) -> dict:
    invested = (
        float(holdings["weight"].sum())
        if "weight" in holdings.columns else 0.0
    )
    n_sect = (
        int(holdings["sector_name"].nunique())
        if "sector_name" in holdings.columns and not holdings.empty
        else 0
    )
    n_cand = (
        int(snapshots["sig_confirmed"].fillna(0).eq(1).sum())
        if not snapshots.empty else 0
    )
    return {
        "num_universe":     len(snapshots),
        "num_candidates":   n_cand,
        "num_holdings":     len(holdings),
        "total_invested":   round(invested, 4),
        "cash_pct":         round(1.0 - invested, 4),
        "num_sectors":      n_sect,
        "breadth_regime":   breadth_regime,
        "breadth_exposure": round(breadth_mult, 2),
    }


def _empty_result(current_holdings=None) -> dict:
    trades = None
    if current_holdings:
        trades = compute_rebalance({}, current_holdings)
    return {
        "snapshots":       pd.DataFrame(),
        "candidates":      pd.DataFrame(),
        "rejected":        pd.DataFrame(),
        "holdings":        pd.DataFrame(),
        "excluded":        pd.DataFrame(),
        "target_weights":  {},
        "sector_exposure": {},
        "trades":          trades,
        "metadata": {
            "num_universe": 0, "num_candidates": 0,
            "num_holdings": 0, "total_invested": 0.0,
            "cash_pct": 1.0, "num_sectors": 0,
            "breadth_regime": "unknown", "breadth_exposure": 1.0,
        },
    }


def _no_candidate_result(
    snapshots, rejected,
    current_holdings=None, excluded=None,
    breadth_regime: str = "unknown",
    breadth_mult: float = 1.0,
) -> dict:
    trades = None
    if current_holdings:
        trades = compute_rebalance({}, current_holdings)
    return {
        "snapshots":       snapshots,
        "candidates":      pd.DataFrame(),
        "rejected":        rejected,
        "holdings":        pd.DataFrame(),
        "excluded":        excluded if excluded is not None
                           else pd.DataFrame(),
        "target_weights":  {},
        "sector_exposure": {},
        "trades":          trades,
        "metadata":        _build_metadata(
            pd.DataFrame(), snapshots,
            breadth_regime=breadth_regime,
            breadth_mult=breadth_mult,
        ),
    }


# ═══════════════════════════════════════════════════════════════
#  REPORT
# ═══════════════════════════════════════════════════════════════

def portfolio_report(result: dict) -> str:
    """
    Format a ``build_portfolio`` result as a human-readable
    report string.
    """
    ln: list[str] = []
    meta = result.get("metadata", {})
    div  = "=" * 60
    sub  = "-" * 60

    # ── Header ────────────────────────────────────────────────
    ln.append(div)
    ln.append("PORTFOLIO SUMMARY")
    ln.append(div)
    ln.append(f"  Universe:       {meta.get('num_universe', 0)} stocks")
    ln.append(
        f"  Candidates:     "
        f"{meta.get('num_candidates', 0)} with active signal"
    )
    ln.append(
        f"  Holdings:       {meta.get('num_holdings', 0)} positions"
    )
    ln.append(
        f"  Invested:       {meta.get('total_invested', 0):.1%}"
    )
    ln.append(f"  Cash:           {meta.get('cash_pct', 1):.1%}")
    ln.append(f"  Sectors:        {meta.get('num_sectors', 0)}")

    # ── Breadth context ───────────────────────────────────────
    b_regime = meta.get("breadth_regime", "unknown")
    b_mult   = meta.get("breadth_exposure", 1.0)
    ln.append(
        f"  Breadth:        {b_regime}  "
        f"(exposure scale: {b_mult:.0%})"
    )

    # ── Sector exposure ───────────────────────────────────────
    se = result.get("sector_exposure", {})
    if se:
        ln.append("")
        ln.append(sub)
        ln.append("SECTOR EXPOSURE")
        ln.append(sub)
        for sec, w in se.items():
            bar = "█" * int(w * 50)
            ln.append(f"  {sec:<20} {w:>6.1%}  {bar}")

    # ── Holdings ──────────────────────────────────────────────
    h = result.get("holdings", pd.DataFrame())
    if not h.empty:
        ln.append("")
        ln.append(sub)
        ln.append("HOLDINGS")
        ln.append(sub)
        header = (
            f"  {'Ticker':<7} {'Weight':>7} {'Score':>6} "
            f"{'Sector':<16} {'Regime':<12} {'Sect Rgm':<10}"
        )
        ln.append(header)
        ln.append(
            f"  {'──────':<7} {'──────':>7} {'─────':>6} "
            f"{'───────────────':<16} {'──────────':<12} "
            f"{'────────':<10}"
        )
        for _, row in h.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"{row.get('weight', 0):>6.1%} "
                f"{row.get('score', 0):>6.2f} "
                f"{str(row.get('sector_name','?')):<16} "
                f"{str(row.get('rs_regime','?')):<12} "
                f"{str(row.get('sect_rs_regime','?')):<10}"
            )

    # ── Excluded candidates ───────────────────────────────────
    ex = result.get("excluded", pd.DataFrame())
    if not ex.empty:
        ln.append("")
        ln.append(sub)
        ln.append(f"EXCLUDED CANDIDATES ({len(ex)})")
        ln.append(sub)
        for _, row in ex.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"score={row.get('score', 0):.2f}  "
                f"→ {row.get('exclusion_reason', '?')}"
            )

    # ── Rejected ──────────────────────────────────────────────
    rej = result.get("rejected", pd.DataFrame())
    if not rej.empty:
        ln.append("")
        ln.append(sub)
        ln.append(f"REJECTED ({len(rej)} stocks)")
        ln.append(sub)
        show = rej.sort_values("score", ascending=False).head(10)
        for _, row in show.iterrows():
            ln.append(
                f"  {str(row.get('ticker','?')):<7} "
                f"score={row.get('score', 0):.2f}  "
                f"→ {row.get('rejection_reason', '?')}"
            )
        if len(rej) > 10:
            ln.append(f"  ... and {len(rej) - 10} more")

    # ── Trades ────────────────────────────────────────────────
    trades = result.get("trades")
    if trades is not None and not trades.empty:
        ln.append("")
        ln.append(sub)
        ln.append("REBALANCE TRADES")
        ln.append(sub)
        for _, row in trades.iterrows():
            act  = row["action"]
            tick = row["ticker"]
            tgt  = row["target_weight"]
            d    = row["delta"]
            if act == "HOLD":
                ln.append(
                    f"  {act:<5} {tick:<7} @ {tgt:>5.1%}"
                )
            else:
                sign = "+" if d > 0 else ""
                ln.append(
                    f"  {act:<5} {tick:<7} → {tgt:>5.1%} "
                    f"({sign}{d:.1%})"
                )

    return "\n".join(ln)

    
####################################################################################   

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

#################################################################
"""
strategy/rotation.py
------------------
Core Smart Money Rotation engine.

Flow
====
  1. Compute composite relative strength (RS) for each sector ETF vs SPY
  2. Rank the 11 GICS sectors → Leading (top 3), Neutral, Lagging (bottom 3)
  3. Within each Leading sector, rank US single names by composite RS
     **blended with a technical quality score** (SMA/EMA, RSI, MACD,
     ADX, volume, volatility)
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

Quality Filter
==============
  When indicator data from the bottom-up pipeline is available,
  each candidate stock is also evaluated on six technical
  dimensions (MA structure, RSI zone, volume profile, MACD
  state, ADX trend strength, volatility regime).

  Candidates must pass a hard quality gate (price > 50 SMA,
  EMA > SMA, RSI 30-75, ADX ≥ 18) before being ranked.

  The final stock rank is a blend of normalised RS (60 %)
  and quality score (40 %).  Weights are configurable via
  RotationConfig.quality.

  When indicator data is unavailable, the engine falls back
  to RS-only ranking (original behaviour).

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
from typing import Optional

import pandas as pd

from common.sector_map import (
    SECTOR_ETFS,
    get_sector,
    get_sector_or_class,
    get_us_tickers_for_sector,
)
from strategy.rotation_filters import (
    QualityConfig,
    GateResult,
    quality_gate,
    quality_score,
    blend_rs_quality,
    quality_diagnostics,
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

    # ── Technical quality filter ───────────────────────────────
    quality: QualityConfig = field(default_factory=QualityConfig)

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

    # ── Quality filter results ─────────────────────────────────
    quality_score: float = 0.0              # 0–1 quality from filters
    quality_gate_passed: bool = True        # did the hard gate pass?
    quality_gates: dict[str, bool] = field(default_factory=dict)
    blended_score: float = 0.0              # final RS+quality blend

    def __repr__(self) -> str:
        q_str = f"  Q {self.quality_score:.2f}" if self.quality_score > 0 else ""
        return (f"  {self.action.value:6s}  {self.ticker:8s}  │  "
                f"{self.sector:24s} (rank {self.sector_rank:2d}, {self.sector_tier:8s})  │  "
                f"RS {self.rs_composite:+.4f}  vs-sector {self.rs_vs_sector_etf:+.4f}"
                f"{q_str}  │  "
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

    @property
    def quality_stats(self) -> dict:
        """
        Aggregate quality statistics across all recommendations.

        Returns a dict suitable for summary printing and JSON export.
        ``enabled`` is False when no recommendation carries a non-zero
        quality score (i.e. the quality filter was not active).
        """
        scored = [r for r in self.recommendations if r.quality_score > 0]
        if not scored:
            return {"enabled": False}

        scores = [r.quality_score for r in scored]
        gate_passed = sum(1 for r in scored if r.quality_gate_passed)
        buy_scores = [r.quality_score for r in scored if r.action == Action.BUY]

        stats: dict = {
            "enabled": True,
            "n_scored": len(scored),
            "n_gate_passed": gate_passed,
            "n_gate_failed": len(scored) - gate_passed,
            "avg_quality": round(sum(scores) / len(scores), 3),
            "min_quality": round(min(scores), 3),
            "max_quality": round(max(scores), 3),
        }

        if buy_scores:
            stats["avg_buy_quality"] = round(
                sum(buy_scores) / len(buy_scores), 3
            )

        return stats


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
    indicator_data: dict[str, pd.DataFrame] | None = None,
) -> list[Recommendation]:
    """
    For each leading sector, rank US single names by a blend of
    RS and technical quality, and return the top N as BUY
    recommendations.

    When ``indicator_data`` is provided and ``config.quality.enabled``
    is True, each candidate must pass the quality gate (SMA, EMA,
    RSI, ADX checks) and the final ranking uses a weighted blend
    of normalised RS (default 60 %) and quality score (default 40 %).

    When indicator data is unavailable or quality is disabled,
    ranking falls back to raw RS (original behaviour).

    Falls back to the sector ETF itself when no single names pass
    both RS and quality filters.
    """
    buys: list[Recommendation] = []
    qcfg = config.quality

    use_quality = (
        qcfg.enabled
        and indicator_data is not None
        and len(indicator_data) > 0
    )

    for ss in leading:
        sector = ss.sector
        etf = ss.etf
        candidates = get_us_tickers_for_sector(sector)

        # keep only those present in the price matrix
        available = [t for t in candidates if t in prices.columns]

        if not available:
            log.info("No single-name data for %s — falling back to %s", sector, etf)
            buys.append(_etf_fallback(
                etf, sector, ss, rs_all,
                reason=f"Sector ETF fallback (no single-name data for {sector})",
            ))
            continue

        # ── Score each candidate ──────────────────────────
        scored: list[tuple] = []
        #   (ticker, sort_key, rs, rs_vs_etf, q_score, gate_passed, gates)

        gate_failures: list[str] = []

        for t in available:
            rs = rs_all.get(t, 0.0)
            rs_vs_etf = _rs_vs(prices, t, etf, config)

            # ── RS eligibility (unchanged) ────────────────
            if rs < config.min_rs_score:
                continue
            if config.prefer_positive_rs_vs_sector and rs_vs_etf < 0:
                continue

            # ── Quality filter ────────────────────────────
            if use_quality:
                idf = indicator_data.get(t)

                if idf is not None and not idf.empty:
                    gate = quality_gate(idf, qcfg)
                    q_score = quality_score(idf, qcfg)

                    if qcfg.gate_required and not gate.passed:
                        gate_failures.append(t)
                        log.debug(
                            "  %s: failed quality gate — %s",
                            t, gate.failed_gates,
                        )
                        continue

                    blended = blend_rs_quality(rs, q_score, qcfg)
                    scored.append((
                        t, blended, rs, rs_vs_etf,
                        q_score, gate.passed, gate.gates,
                    ))
                else:
                    # No indicator data for this ticker —
                    # include with neutral quality
                    scored.append((
                        t, blend_rs_quality(rs, 0.5, qcfg),
                        rs, rs_vs_etf, 0.5, True, {},
                    ))
            else:
                # Quality disabled — rank by raw RS
                scored.append((
                    t, rs, rs, rs_vs_etf,
                    0.0, True, {},
                ))

        if gate_failures:
            log.info(
                "  %s: %d/%d candidates failed quality gate: %s",
                sector, len(gate_failures),
                len(gate_failures) + len(scored),
                gate_failures[:5],
            )

        # Sort by blended score (or RS) descending
        scored.sort(key=lambda x: x[1], reverse=True)
        picks = scored[: config.stocks_per_sector]

        if not picks:
            # All candidates filtered out → fall back to ETF
            reason = (
                "Sector ETF fallback (no candidates passed"
                f" RS + quality filters)"
                if use_quality
                else "Sector ETF fallback (no candidates above RS threshold)"
            )
            log.info(
                "All candidates in %s filtered out — using %s",
                sector, etf,
            )
            buys.append(_etf_fallback(
                etf, sector, ss, rs_all, reason=reason,
            ))
            continue

        for (ticker, sort_key, rs, rs_vs_etf,
             q_score, gate_passed, gates) in picks:

            quality_note = ""
            if use_quality and q_score > 0:
                quality_note = f", quality {q_score:.2f}"

            buys.append(Recommendation(
                ticker=ticker,
                action=Action.BUY,
                sector=sector,
                sector_rank=ss.rank,
                sector_tier=ss.tier,
                rs_composite=rs,
                rs_vs_sector_etf=rs_vs_etf,
                reason=(
                    f"Top ranked in leading sector {sector} "
                    f"(rank {ss.rank}){quality_note}"
                ),
                quality_score=q_score,
                quality_gate_passed=gate_passed,
                quality_gates=gates,
                blended_score=sort_key,
            ))

    return buys


def _etf_fallback(
    etf: str,
    sector: str,
    ss: SectorScore,
    rs_all: pd.Series,
    reason: str,
) -> Recommendation:
    """Build a Recommendation for the sector ETF as fallback."""
    return Recommendation(
        ticker=etf,
        action=Action.BUY,
        sector=sector,
        sector_rank=ss.rank,
        sector_tier=ss.tier,
        rs_composite=rs_all.get(etf, 0.0),
        rs_vs_sector_etf=0.0,
        reason=reason,
    )


# ═══════════════════════════════════════════════════════════════
#  STEP 3 — EVALUATE CURRENT HOLDINGS
# ═══════════════════════════════════════════════════════════════

def _evaluate_holdings(
    holdings: list[str],
    sector_scores: list[SectorScore],
    prices: pd.DataFrame,
    config: RotationConfig,
    rs_all: pd.Series,
    indicator_data: dict[str, pd.DataFrame] | None = None,
) -> list[Recommendation]:
    """
    Walk every current holding through the sell-rule waterfall.

    Priority
    --------
    1.  Sector → Lagging         → SELL
    2.  Sector → Neutral         → REDUCE
    3.  Individual RS too weak   → SELL
    4.  Otherwise                → HOLD

    Quality diagnostics are attached to every Recommendation for
    reporting, but quality does NOT currently trigger sells.  That
    can be added as a follow-up rule (e.g. quality collapse while
    sector is only Neutral → SELL instead of REDUCE).
    """
    tier_map = {s.sector: s.tier for s in sector_scores}
    rank_map = {s.sector: s.rank for s in sector_scores}
    leading_set = {s.sector for s in sector_scores if s.tier == "Leading"}

    use_quality = (
        config.quality.enabled
        and indicator_data is not None
    )

    recs: list[Recommendation] = []

    for ticker in holdings:
        sector = get_sector(ticker)
        label = get_sector_or_class(ticker)

        sector_etf = (
            SECTOR_ETFS.get(sector, config.benchmark)
            if sector else config.benchmark
        )
        rs = rs_all.get(ticker, 0.0)
        rs_vs_etf = _rs_vs(prices, ticker, sector_etf, config)
        s_rank = rank_map.get(sector, 99) if sector else 99
        s_tier = tier_map.get(sector, "n/a") if sector else "n/a"

        # ── Quality diagnostics (informational) ───────────
        q_score = 0.0
        q_passed = True
        q_gates: dict[str, bool] = {}

        if use_quality:
            idf = indicator_data.get(ticker)
            if idf is not None and not idf.empty:
                gate = quality_gate(idf, config.quality)
                q_score = quality_score(idf, config.quality)
                q_passed = gate.passed
                q_gates = gate.gates

        blended = (
            blend_rs_quality(rs, q_score, config.quality)
            if use_quality else rs
        )

        # ── Rule 1 & 2: sector drift ──────────────────────
        if config.sell_if_sector_not_leading and sector and sector not in leading_set:
            if s_tier == "Lagging":
                recs.append(Recommendation(
                    ticker=ticker, action=Action.SELL,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} is Lagging (rank {s_rank}/11)",
                    quality_score=q_score,
                    quality_gate_passed=q_passed,
                    quality_gates=q_gates,
                    blended_score=blended,
                ))
            else:
                recs.append(Recommendation(
                    ticker=ticker, action=Action.REDUCE,
                    sector=label, sector_rank=s_rank, sector_tier=s_tier,
                    rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                    reason=f"Sector {sector} drifted to Neutral (rank {s_rank}/11)",
                    quality_score=q_score,
                    quality_gate_passed=q_passed,
                    quality_gates=q_gates,
                    blended_score=blended,
                ))
            continue

        # ── Rule 3: individual RS collapse ─────────────────
        if rs < config.sell_individual_rs_below:
            recs.append(Recommendation(
                ticker=ticker, action=Action.SELL,
                sector=label, sector_rank=s_rank, sector_tier=s_tier,
                rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
                reason=(f"Individual RS {rs:+.3f} below floor "
                        f"({config.sell_individual_rs_below:+.3f})"),
                quality_score=q_score,
                quality_gate_passed=q_passed,
                quality_gates=q_gates,
                blended_score=blended,
            ))
            continue

        # ── Rule 4: everything fine ────────────────────────
        recs.append(Recommendation(
            ticker=ticker, action=Action.HOLD,
            sector=label, sector_rank=s_rank, sector_tier=s_tier,
            rs_composite=rs, rs_vs_sector_etf=rs_vs_etf,
            reason=f"Sector {sector} still Leading (rank {s_rank}), RS OK",
            quality_score=q_score,
            quality_gate_passed=q_passed,
            quality_gates=q_gates,
            blended_score=blended,
        ))

    return recs


# ═══════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════

def run_rotation(
    prices: pd.DataFrame,
    current_holdings: list[str] | None = None,
    config: RotationConfig | None = None,
    indicator_data: dict[str, pd.DataFrame] | None = None,
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

    indicator_data : dict[str, pd.DataFrame] or None
        ``{ticker: DataFrame}`` where each DataFrame is the output
        of ``compute_all_indicators()`` (and optionally
        ``compute_all_rs()`` / ``compute_composite_score()``).

        When provided and ``config.quality.enabled`` is True, the
        quality filter enhances stock selection within leading
        sectors by:
          1. Gating on SMA/EMA trend structure, RSI, and ADX
          2. Scoring MA positioning, momentum, volume, MACD,
             directional strength, and volatility
          3. Blending normalised RS with the quality score

        When None, the engine uses RS-only ranking (original
        behaviour — fully backward compatible).

        The orchestrator provides this by passing
        ``{ticker: result.df for ticker, result in ticker_results.items()
           if result.ok}`` from the bottom-up pipeline.

    Returns
    -------
    RotationResult
        .sector_rankings  — ordered list of SectorScore
        .recommendations  — ordered list of Recommendation
        .buys / .sells / .reduces / .holds — convenience accessors
        .quality_stats    — aggregate quality metrics (dict)
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

    n_indicators = len(indicator_data) if indicator_data else 0
    quality_status = (
        f"quality ON ({n_indicators} tickers)"
        if config.quality.enabled and n_indicators > 0
        else "quality OFF (RS only)"
    )
    log.info(
        "Rotation as-of %s  |  %d days × %d tickers  |  %s",
        as_of, prices.shape[0], prices.shape[1], quality_status,
    )

    # ── Step 1: rank sectors ───────────────────────────────────
    sector_rankings = _rank_sectors(prices, config)
    leading = [s for s in sector_rankings if s.tier == "Leading"]

    log.info("Leading : %s", [s.sector for s in leading])
    log.info("Lagging : %s", [s.sector for s in sector_rankings if s.tier == "Lagging"])

    # Pre-compute RS for all tickers once (reused everywhere)
    rs_all, _ = composite_rs_all(prices, config)

    # ── Step 2: pick stocks in leading sectors ─────────────────
    raw_buys = _pick_stocks(
        leading, prices, config, rs_all, indicator_data,
    )

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
        indicator_data=indicator_data,
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

    w = 80
    q_stats = result.quality_stats
    has_quality = q_stats.get("enabled", False)

    print(f"\n{'═' * w}")
    print(f"  SMART MONEY ROTATION  —  {result.as_of_date}")
    if has_quality:
        qcfg = result.config.quality
        print(f"  Quality filter: ON  "
              f"(RS {qcfg.w_rs:.0%} / Quality {qcfg.w_quality:.0%})")
    else:
        print(f"  Quality filter: OFF  (RS-only ranking)")
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
            _print_rec(r, has_quality)

    # ── Reduce ─────────────────────────────────────────────────
    if result.reduces:
        print(f"\n  🟡 REDUCE  ({len(result.reduces)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.reduces:
            _print_rec(r, has_quality)

    # ── Buy ────────────────────────────────────────────────────
    if result.buys:
        print(f"\n  🟢 BUY  ({len(result.buys)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.buys:
            _print_rec(r, has_quality)

    # ── Hold ───────────────────────────────────────────────────
    if result.holds:
        print(f"\n  ⚪ HOLD  ({len(result.holds)})")
        print(f"  {'─' * (w - 4)}")
        for r in result.holds:
            _print_rec(r, has_quality)

    # ── Summary ────────────────────────────────────────────────
    print(f"\n  {'─' * (w - 4)}")
    total = len(result.recommendations)
    print(f"  Summary: {len(result.sells)} sell  {len(result.reduces)} reduce  "
          f"{len(result.buys)} buy  {len(result.holds)} hold  "
          f"({total} total)")
    print(f"  Leading sectors : {', '.join(result.leading_sectors)}")
    print(f"  Lagging sectors : {', '.join(result.lagging_sectors)}")

    if has_quality:
        print(f"  Quality scored  : {q_stats['n_scored']} tickers  "
              f"(gate pass {q_stats['n_gate_passed']}, "
              f"fail {q_stats['n_gate_failed']})")
        print(f"  Quality range   : {q_stats['min_quality']:.2f} – "
              f"{q_stats['max_quality']:.2f}  "
              f"(avg {q_stats['avg_quality']:.2f})")
        if "avg_buy_quality" in q_stats:
            print(f"  Avg BUY quality : {q_stats['avg_buy_quality']:.2f}")

    print(f"{'═' * w}\n")


def _print_rec(r: Recommendation, show_quality: bool) -> None:
    """Print a single Recommendation line."""
    line = f"    {r.ticker:8s}  RS {r.rs_composite:+.4f}"

    if show_quality and r.quality_score > 0:
        gate_icon = "✓" if r.quality_gate_passed else "✗"
        line += (
            f"  Q={r.quality_score:.2f}[{gate_icon}]"
            f"  blend={r.blended_score:.3f}"
        )
        # Show failed gates inline if any
        if not r.quality_gate_passed and r.quality_gates:
            failed = [k for k, v in r.quality_gates.items() if not v]
            if failed:
                line += f"  ⚠{','.join(failed)}"

    line += f"  │  {r.reason}"
    print(line)


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

##############################################################

"""
strategy/rotation_filters.py
-----------------------------
Technical quality filters for the rotation engine's stock selection.

The rotation engine identifies which sectors are leading and which
stocks within those sectors have the strongest relative strength.
This module adds a second dimension: technical quality confirmation.

A stock with strong RS but exhausted technicals (RSI 85, extended
above all MAs, declining volume) is likely to mean-revert.  A stock
with strong RS AND confirmed technicals (trending MAs, RSI in the
sweet spot, rising volume) is the high-conviction pick.

Architecture
────────────
  rotation.py::_pick_stocks()
       ↓  candidate ticker + its indicator DataFrame
  quality_gate()        — hard pass/fail checks (all must pass)
       ↓
  quality_score()       — 0–1 weighted quality metric
       ↓
  blend_rs_quality()    — combine RS + quality for final ranking

All thresholds live in QualityConfig and are set via
RotationConfig.quality in rotation.py.

Column Dependencies (from compute/indicators.py)
─────────────────────────────────────────────────
  close, ema_30, sma_30, sma_50
  close_vs_ema_30_pct, close_vs_sma_50_pct
  rsi_14, adx_14, plus_di, minus_di
  macd_hist, macd_line, macd_signal
  obv_slope_10d, relative_volume
  atr_14_pct

All of these are produced by compute_all_indicators() which runs
during the standard bottom-up pipeline (Phase 2).  The rotation
engine accesses them via the optional ``indicator_data`` parameter
on run_rotation().

When indicator data is unavailable (e.g. the bottom-up pipeline
hasn't run, or a ticker wasn't in the scoring universe), all
filters degrade gracefully — the gate passes by default and quality
returns 0.5 (neutral), so ranking falls back to RS-only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════

@dataclass
class QualityConfig:
    """
    Quality filter configuration for rotation stock selection.

    Split into three sections:
      1. Gate thresholds   — hard pass/fail, all must be True
      2. Blending weights  — how much RS vs quality matters
      3. Sub-component wts — within the quality score itself
    """

    # ── Master switch ──────────────────────────────────────
    enabled: bool = True

    # ── Gate thresholds (hard pass / fail) ─────────────────
    gate_price_above_sma: bool = True     # close > sma_50
    gate_ema_above_sma: bool = True       # ema_30 > sma_50
    gate_rsi_min: float = 30.0
    gate_rsi_max: float = 75.0
    gate_adx_min: float = 18.0

    # When True, a ticker that fails the gate is excluded.
    # When False, the gate result is recorded but the ticker
    # still participates in ranking (quality score penalises).
    gate_required: bool = True

    # ── Blending weights (RS vs quality) ───────────────────
    # These control the final ranked score:
    #   blended = w_rs × sigmoid(RS) + w_quality × quality
    w_rs: float = 0.60
    w_quality: float = 0.40

    # Sigmoid scale factor for RS normalisation.
    # Higher = more spread.  With 10.0:
    #   RS +0.10 → 0.73,  RS 0 → 0.50,  RS -0.10 → 0.27
    rs_sigmoid_scale: float = 10.0

    # ── Quality sub-component weights (sum to 1.0) ─────────
    w_ma_position: float = 0.25       # MA alignment & distance
    w_rsi_zone: float = 0.20          # RSI sweet-spot
    w_volume: float = 0.20            # Volume + OBV confirmation
    w_macd: float = 0.15              # MACD histogram state
    w_adx_direction: float = 0.10     # ADX strength + DI direction
    w_volatility: float = 0.10        # ATR% regime (moderate best)


# ═══════════════════════════════════════════════════════════════
#  GATE RESULT
# ═══════════════════════════════════════════════════════════════

@dataclass
class GateResult:
    """Outcome of the quality gate check for one ticker."""

    passed: bool
    gates: dict[str, bool] = field(default_factory=dict)

    @property
    def failed_gates(self) -> list[str]:
        """Names of gates that did not pass."""
        return [k for k, v in self.gates.items() if not v]

    @property
    def n_passed(self) -> int:
        return sum(1 for v in self.gates.values() if v)

    @property
    def n_total(self) -> int:
        return len(self.gates)

    def summary(self) -> str:
        if self.passed:
            return f"PASS ({self.n_passed}/{self.n_total})"
        return (
            f"FAIL ({self.n_passed}/{self.n_total}): "
            f"{', '.join(self.failed_gates)}"
        )


# ═══════════════════════════════════════════════════════════════
#  VALUE EXTRACTION HELPER
# ═══════════════════════════════════════════════════════════════

def _safe(row: pd.Series, col: str, default: float = 0.0) -> float:
    """
    Extract a float from a Series row, returning *default* on
    any failure (missing key, None, NaN, non-numeric).
    """
    val = row.get(col)
    if val is None:
        return default
    try:
        fval = float(val)
        return default if np.isnan(fval) else fval
    except (TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════
#  QUALITY GATE
# ═══════════════════════════════════════════════════════════════

def quality_gate(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> GateResult:
    """
    Hard pass/fail checks on the latest row of an indicator
    DataFrame.

    All enabled gates must pass for the ticker to be eligible
    as a rotation BUY (when ``config.gate_required`` is True).

    Gates
    ─────
    1. price_above_sma50  — close > 50-day SMA
       Why: confirms the stock is in a structural uptrend.
       A stock below its 50 SMA is in a downtrend regardless
       of sector strength.

    2. ema_above_sma      — 30 EMA > 50 SMA
       Why: confirms bullish moving-average alignment.  When
       the fast MA is below the slow MA, momentum has already
       broken down even if RS is still positive from earlier
       performance.

    3. rsi_in_range       — RSI between 30 and 75
       Why: RSI < 30 means collapse in progress (not momentum).
       RSI > 75 means overbought and at risk of mean reversion.
       The quality score handles fine-grained RSI positioning.

    4. adx_above_min      — ADX ≥ 18
       Why: ADX below ~18 means no trend — the market is
       choppy and RS readings are noise, not signal.

    Parameters
    ----------
    df : pd.DataFrame
        Indicator-enriched DataFrame for one ticker (output of
        ``compute_all_indicators()``).  Uses the latest row.
    config : QualityConfig, optional

    Returns
    -------
    GateResult
        .passed  — True if all enabled gates are True
        .gates   — dict of {gate_name: bool} for diagnostics
    """
    if config is None:
        config = QualityConfig()

    if df is None or df.empty:
        return GateResult(passed=False, gates={"data_available": False})

    last = df.iloc[-1]
    gates: dict[str, bool] = {}

    # ── Gate 1: Price above 50 SMA ────────────────────────
    if config.gate_price_above_sma:
        close = _safe(last, "close", 0.0)
        sma50 = _safe(last, "sma_50", np.nan)
        if np.isnan(sma50) or sma50 == 0:
            # No SMA data — pass by default (warmup period)
            gates["price_above_sma50"] = True
        else:
            gates["price_above_sma50"] = close > sma50

    # ── Gate 2: Short EMA > Long SMA (bullish alignment) ──
    if config.gate_ema_above_sma:
        ema = _safe(last, "ema_30", np.nan)
        sma = _safe(last, "sma_50", np.nan)
        if np.isnan(ema) or np.isnan(sma):
            gates["ema_above_sma"] = True
        else:
            gates["ema_above_sma"] = ema > sma

    # ── Gate 3: RSI in range ──────────────────────────────
    rsi = _safe(last, "rsi_14", 50.0)
    gates["rsi_in_range"] = config.gate_rsi_min <= rsi <= config.gate_rsi_max

    # ── Gate 4: ADX minimum trend strength ────────────────
    adx = _safe(last, "adx_14", 0.0)
    gates["adx_above_min"] = adx >= config.gate_adx_min

    passed = all(gates.values())
    return GateResult(passed=passed, gates=gates)


# ═══════════════════════════════════════════════════════════════
#  QUALITY SCORE
# ═══════════════════════════════════════════════════════════════

def quality_score(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> float:
    """
    Compute a 0–1 quality score from the latest row of a
    ticker's indicator DataFrame.

    Six sub-components are weighted and summed:

      1. MA positioning   (0.25) — trend structure health
      2. RSI zone         (0.20) — momentum sweet-spot
      3. Volume profile   (0.20) — institutional participation
      4. MACD state       (0.15) — momentum direction & strength
      5. ADX / direction  (0.10) — trend strength + bullish bias
      6. Volatility       (0.10) — risk regime (moderate best)

    Returns 0.5 (neutral) when input data is unavailable.
    """
    if config is None:
        config = QualityConfig()

    if df is None or df.empty:
        return 0.5

    last = df.iloc[-1]

    ma_sc   = _score_ma_position(last)
    rsi_sc  = _score_rsi_zone(last)
    vol_sc  = _score_volume(last)
    macd_sc = _score_macd(last)
    adx_sc  = _score_adx_direction(last)
    atr_sc  = _score_volatility(last)

    composite = (
        config.w_ma_position    * ma_sc
        + config.w_rsi_zone     * rsi_sc
        + config.w_volume       * vol_sc
        + config.w_macd         * macd_sc
        + config.w_adx_direction * adx_sc
        + config.w_volatility   * atr_sc
    )

    return float(np.clip(composite, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  BLENDING
# ═══════════════════════════════════════════════════════════════

def blend_rs_quality(
    rs_score: float,
    quality: float,
    config: QualityConfig | None = None,
) -> float:
    """
    Blend composite RS with quality score for final ranking.

    RS is typically in the range [-0.2, +0.2] while quality
    is [0, 1].  We normalise RS to [0, 1] via a sigmoid so
    the two scales are comparable before weighting.

    The sigmoid scale factor controls discrimination:
      scale=10  →  RS ±0.10 maps to [0.27, 0.73]
      scale=15  →  RS ±0.10 maps to [0.18, 0.82]
      scale=5   →  RS ±0.10 maps to [0.38, 0.62]

    When all candidates have similar RS the sigmoid compresses
    them and quality differentiates — which is exactly what we
    want (the sector tide lifts all boats equally, so pick the
    mechanically soundest boat).

    Parameters
    ----------
    rs_score : float
        Raw composite RS vs benchmark (e.g. +0.05).
    quality : float
        Quality score from ``quality_score()`` (0–1).
    config : QualityConfig

    Returns
    -------
    float — blended ranking score (higher = better).
    """
    if config is None:
        config = QualityConfig()

    scale = config.rs_sigmoid_scale
    rs_norm = 1.0 / (1.0 + np.exp(-scale * rs_score))

    blended = config.w_rs * rs_norm + config.w_quality * quality
    return float(blended)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: MA POSITIONING
# ═══════════════════════════════════════════════════════════════

def _score_ma_position(last: pd.Series) -> float:
    """
    Score trend structure from moving-average positioning.

    Best:  Price 0–5 % above EMA — near dynamic support, ideal
           pullback entry within a trend.
    Good:  5–10 % above — actively trending, not yet extended.
    Weak:  >15 % above — extended, risk of snapback.
    Weak:  Below EMA — trend weakening.

    MA alignment bonus: EMA_30 > SMA_50 adds +0.15 because
    bullish MA crossover confirms structural momentum.
    """
    close_vs_ema = _safe(last, "close_vs_ema_30_pct", 0.0)
    close_vs_sma = _safe(last, "close_vs_sma_50_pct", 0.0)

    # Distance from 30 EMA (% terms)
    ema_dist = float(np.interp(
        close_vs_ema,
        [-10, -3, 0, 3, 8, 15, 25],
        [0.05, 0.20, 0.70, 1.00, 0.80, 0.40, 0.10],
    ))

    # Distance from 50 SMA (% terms)
    sma_dist = float(np.interp(
        close_vs_sma,
        [-10, -2, 0, 5, 12, 20, 30],
        [0.05, 0.15, 0.60, 1.00, 0.70, 0.30, 0.10],
    ))

    # Bullish MA alignment bonus
    ema = _safe(last, "ema_30", 0.0)
    sma = _safe(last, "sma_50", 0.0)
    alignment = 0.15 if (ema > 0 and sma > 0 and ema > sma) else 0.0

    score = 0.50 * ema_dist + 0.35 * sma_dist + alignment
    return float(np.clip(score, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: RSI ZONE
# ═══════════════════════════════════════════════════════════════

def _score_rsi_zone(last: pd.Series) -> float:
    """
    RSI sweet-spot scoring.

    The rotation strategy follows momentum, so the ideal RSI
    zone is 45–60: the stock is trending but not overbought.

      0–25   : 0.05  — collapsing, no momentum
      25–35  : ramp up to 0.50  — oversold, building
      35–45  : ramp to 0.90  — momentum starting
      45–55  : 1.00  — ideal trending zone
      55–65  : 0.85  — still strong, getting warm
      65–75  : ramp down to 0.50  — overbought risk
      75–100 : 0.05–0.15  — extreme, likely to revert
    """
    rsi = _safe(last, "rsi_14", 50.0)

    return float(np.interp(
        rsi,
        [0, 25, 35, 45, 55, 65, 75, 85, 100],
        [0.05, 0.10, 0.50, 0.90, 1.00, 0.85, 0.50, 0.15, 0.05],
    ))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: VOLUME PROFILE
# ═══════════════════════════════════════════════════════════════

def _score_volume(last: pd.Series) -> float:
    """
    Volume profile scoring.

    Combines relative volume (vs 20-day average) and OBV slope.

    Relative volume 1.2–2.5 with positive OBV slope =
    institutional buying — the highest score.

    Rel vol < 0.5 = no interest (low-liquidity drift trap).
    Rel vol > 5.0 = panic / event-driven (unstable).
    Negative OBV slope = distribution (selling pressure).
    """
    rel_vol = _safe(last, "relative_volume", 1.0)
    obv_slope = _safe(last, "obv_slope_10d", 0.0)

    # Relative volume score (piecewise)
    vol_sc = float(np.interp(
        rel_vol,
        [0.0, 0.4, 0.8, 1.2, 2.0, 3.5, 6.0, 10.0],
        [0.05, 0.15, 0.40, 0.80, 1.00, 0.70, 0.40, 0.20],
    ))

    # OBV slope direction — positive = accumulation
    if obv_slope > 0:
        obv_sc = 0.80
    elif obv_slope == 0:
        obv_sc = 0.50
    else:
        obv_sc = 0.20

    return float(0.65 * vol_sc + 0.35 * obv_sc)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: MACD STATE
# ═══════════════════════════════════════════════════════════════

def _score_macd(last: pd.Series) -> float:
    """
    MACD state scoring.

    Three signals are checked and combined:

    1. Histogram positive   (+0.35) — current momentum is bullish
    2. Line above signal    (+0.30) — bullish crossover intact
    3. Histogram strength   (+0.20) — magnitude relative to line

    Base score is 0.15 so even a fully bearish MACD doesn't
    score zero (other components may still justify the pick).
    """
    hist = _safe(last, "macd_hist", 0.0)
    line = _safe(last, "macd_line", 0.0)
    signal = _safe(last, "macd_signal", 0.0)

    score = 0.15  # base

    if hist > 0:
        score += 0.35

    if line > signal:
        score += 0.30

    # Histogram strength (relative to MACD line magnitude)
    if line != 0:
        hist_strength = abs(hist / line)
        score += 0.20 * min(hist_strength, 1.0)
    elif hist > 0:
        score += 0.10

    return float(np.clip(score, 0.0, 1.0))


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: ADX + DIRECTIONAL
# ═══════════════════════════════════════════════════════════════

def _score_adx_direction(last: pd.Series) -> float:
    """
    ADX trend strength + directional bias.

    ADX 25–40 with +DI > −DI = strong bullish trend (peak score).

    ADX < 15: no trend (RS is noise).
    ADX > 50: extreme trend (possible exhaustion or blow-off).

    +DI / −DI ratio determines bullish vs bearish directional
    bias within the trend.
    """
    adx = _safe(last, "adx_14", 15.0)
    plus_di = _safe(last, "plus_di", 0.0)
    minus_di = _safe(last, "minus_di", 0.0)

    # ADX value score (piecewise)
    adx_sc = float(np.interp(
        adx,
        [0, 12, 18, 25, 35, 50, 70],
        [0.05, 0.15, 0.50, 0.90, 1.00, 0.70, 0.40],
    ))

    # Directional bias: +DI share of total DI
    di_total = plus_di + minus_di
    if di_total > 0:
        di_ratio = plus_di / di_total  # 0.5 = neutral, >0.5 = bullish
        dir_sc = float(np.interp(
            di_ratio,
            [0.0, 0.30, 0.50, 0.65, 0.80, 1.0],
            [0.00, 0.15, 0.50, 0.80, 1.00, 1.00],
        ))
    else:
        dir_sc = 0.50

    return float(0.55 * adx_sc + 0.45 * dir_sc)


# ═══════════════════════════════════════════════════════════════
#  SUB-SCORE: VOLATILITY
# ═══════════════════════════════════════════════════════════════

def _score_volatility(last: pd.Series) -> float:
    """
    Volatility regime scoring (tent function — moderate is best).

    Uses ATR as a percentage of price for cross-asset comparison.

    ATR% 1.5–3.0 %: healthy trending volatility.
    ATR% < 0.5 %:   dead money / no movement.
    ATR% > 8 %:     whipsaw territory, stops get hit.
    """
    atr_pct = _safe(last, "atr_14_pct", 2.0)

    return float(np.interp(
        atr_pct,
        [0.0, 0.3, 0.8, 1.5, 3.0, 5.0, 8.0, 15.0],
        [0.10, 0.25, 0.60, 0.90, 1.00, 0.60, 0.30, 0.10],
    ))


# ═══════════════════════════════════════════════════════════════
#  DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════

def quality_diagnostics(
    df: pd.DataFrame,
    config: QualityConfig | None = None,
) -> dict:
    """
    Full diagnostic breakdown for a single ticker.

    Returns a dict with gate results, sub-scores, the final
    quality score, and the raw indicator values that drove each
    decision.  Useful for debugging and reports.
    """
    if config is None:
        config = QualityConfig()

    gate = quality_gate(df, config)
    score = quality_score(df, config)

    if df is not None and not df.empty:
        last = df.iloc[-1]
        sub = {
            "ma_position":   round(_score_ma_position(last), 4),
            "rsi_zone":      round(_score_rsi_zone(last), 4),
            "volume":        round(_score_volume(last), 4),
            "macd":          round(_score_macd(last), 4),
            "adx_direction": round(_score_adx_direction(last), 4),
            "volatility":    round(_score_volatility(last), 4),
        }
        vals = {
            "close":             _safe(last, "close"),
            "ema_30":            _safe(last, "ema_30"),
            "sma_50":            _safe(last, "sma_50"),
            "close_vs_ema_pct":  _safe(last, "close_vs_ema_30_pct"),
            "close_vs_sma_pct":  _safe(last, "close_vs_sma_50_pct"),
            "rsi_14":            _safe(last, "rsi_14"),
            "adx_14":            _safe(last, "adx_14"),
            "plus_di":           _safe(last, "plus_di"),
            "minus_di":          _safe(last, "minus_di"),
            "macd_hist":         _safe(last, "macd_hist"),
            "macd_line":         _safe(last, "macd_line"),
            "relative_volume":   _safe(last, "relative_volume"),
            "obv_slope_10d":     _safe(last, "obv_slope_10d"),
            "atr_14_pct":        _safe(last, "atr_14_pct"),
        }
    else:
        sub = {}
        vals = {}

    return {
        "gate_passed":   gate.passed,
        "gate_summary":  gate.summary(),
        "gates":         gate.gates,
        "failed_gates":  gate.failed_gates,
        "quality_score": round(score, 4),
        "sub_scores":    sub,
        "key_values":    vals,
    }


##############################################################
"""
strategy/signals.py
-------------------
Entry / exit signal generation.

Takes a scored, RS-enriched DataFrame for a single ticker and
produces a column set that tells the portfolio builder what to do.

Signals are *gated*: every candidate must pass a checklist of
conditions before it earns a ``sig_confirmed == 1`` flag.

Gates
─────
  1.  rs_regime       ∈  allowed_rs_regimes       (stock trend)
  1b. rsi_14          ∈  [rsi_entry_min, rsi_entry_max]  (RSI range)
  2.  sect_rs_regime  ∈  allowed_sector_regimes   (sector tide)
  3.  breadth_regime  ∈  allowed_breadth_regimes  (market gate)
  4.  momentum_streak ≥  N consecutive days > 0.5 (persistence)
  5.  NOT in cooldown after recent exit            (anti-churn)
  6.  score_adjusted  ≥  entry_score_min           (quality bar)

Pipeline
────────
  scored_df  →  _gate_regime()
             →  _gate_rsi()          ← NEW: hard RSI 30-70 gate
             →  _gate_sector()
             →  _gate_breadth()
             →  _gate_momentum()
             →  _gate_cooldown()
             →  _gate_entry()
             →  _compute_exits()
             →  _position_sizing()
             →  generate_signals()   ← master function

Each gate adds its own boolean column so downstream analytics
can diagnose *why* a signal was or wasn't generated.

This is the per-ticker quality filter. It runs on a single
ticker's time series and answers "is this ticker trade-worthy
today?" through seven gates (regime, RSI, sector, breadth,
momentum, cooldown, score). Its output is sig_confirmed,
sig_exit, and all the sig_* diagnostic columns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from common.config import SIGNAL_PARAMS, BREADTH_PORTFOLIO


# ═══════════════════════════════════════════════════════════════
#  CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════

def _sp(key: str):
    return SIGNAL_PARAMS[key]


def _bpp(key: str):
    return BREADTH_PORTFOLIO[key]


# ═══════════════════════════════════════════════════════════════
#  GATE 1 — STOCK RS REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_regime(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_rs_regimes")

    if "rs_regime" in result.columns:
        result["sig_regime_ok"] = result["rs_regime"].isin(allowed)
    else:
        result["sig_regime_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 1b — RSI RANGE
# ═══════════════════════════════════════════════════════════════

def _gate_rsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hard RSI gate: only allow entries when RSI is between 30 and 70.

    RSI < 30  → oversold collapse in progress, not momentum
    RSI > 70  → overbought, high risk of mean reversion

    This is a HARD gate — no BUY signal is generated outside
    this range regardless of how strong other indicators are.

    The thresholds are configured via SIGNAL_PARAMS:
      - rsi_entry_min  (default 30)
      - rsi_entry_max  (default 70)
    """
    result = df.copy()
    rsi_min = _sp("rsi_entry_min")
    rsi_max = _sp("rsi_entry_max")

    # The RSI column name from compute/indicators.py
    rsi_col = "rsi_14"

    if rsi_col in result.columns:
        rsi = result[rsi_col].fillna(50.0)
        result["sig_rsi_ok"] = (rsi >= rsi_min) & (rsi <= rsi_max)
    else:
        # No RSI data — pass by default (degrade gracefully)
        result["sig_rsi_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 2 — SECTOR REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_sector(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    allowed = _sp("allowed_sector_regimes")

    if "sect_rs_regime" in result.columns:
        result["sig_sector_ok"] = (
            result["sect_rs_regime"].isin(allowed)
        )
    else:
        result["sig_sector_ok"] = True

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 3 — BREADTH REGIME
# ═══════════════════════════════════════════════════════════════

def _gate_breadth(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Market-level breadth gate.

    When breadth data is provided, this gate:
      - Merges breadth_regime and breadth_score onto the
        ticker's DataFrame by date.
      - Sets ``sig_breadth_ok`` = True when breadth_regime
        is NOT weak (or when ``weak_block_new`` is False).
      - Adjusts the effective entry threshold upward in
        neutral / weak regimes.

    Without breadth data the gate passes unconditionally.
    """
    result = df.copy()

    if breadth is None or breadth.empty:
        result["sig_breadth_ok"]       = True
        result["breadth_regime"]       = "unknown"
        result["breadth_score"]        = np.nan
        result["entry_score_adj"]      = 0.0
        return result

    # ── Merge breadth onto ticker dates ───────────────────────
    breadth_cols = ["breadth_regime", "breadth_score", "breadth_score_smooth"]
    available    = [c for c in breadth_cols if c in breadth.columns]

    if not available:
        result["sig_breadth_ok"]  = True
        result["breadth_regime"]  = "unknown"
        result["breadth_score"]   = np.nan
        result["entry_score_adj"] = 0.0
        return result

    bdata = breadth[available].copy()

    # Align on date index
    result = result.join(bdata, how="left")

    # Forward-fill breadth regime for any gaps
    if "breadth_regime" in result.columns:
        result["breadth_regime"] = (
            result["breadth_regime"].ffill().fillna("unknown")
        )
    else:
        result["breadth_regime"] = "unknown"

    if "breadth_score" in result.columns:
        result["breadth_score"] = result["breadth_score"].ffill()

    # ── Set gate ──────────────────────────────────────────────
    block_new = _bpp("weak_block_new")

    if block_new:
        result["sig_breadth_ok"] = (
            result["breadth_regime"] != "weak"
        )
    else:
        result["sig_breadth_ok"] = True

    # ── Entry threshold adjustment ────────────────────────────
    weak_raise    = _bpp("weak_raise_entry")
    neutral_raise = _bpp("neutral_raise_entry")

    conditions = [
        result["breadth_regime"] == "weak",
        result["breadth_regime"] == "neutral",
    ]
    choices = [weak_raise, neutral_raise]

    result["entry_score_adj"] = np.select(
        conditions, choices, default=0.0
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 4 — MOMENTUM PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def _gate_momentum(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    streak = _sp("confirmation_streak")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_momentum_ok"] = False
        return result

    above = result[score_col] >= 0.50
    cumsum = above.cumsum()
    reset  = cumsum - cumsum.where(~above).ffill().fillna(0)

    result["sig_momentum_streak"] = reset.astype(int)
    result["sig_momentum_ok"]     = reset >= streak

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 5 — COOLDOWN
# ═══════════════════════════════════════════════════════════════

def _gate_cooldown(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    cooldown = _sp("cooldown_days")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_in_cooldown"] = False
        return result

    exit_thresh = _sp("exit_score_max")

    was_above = (result[score_col].shift(1) >= exit_thresh)
    now_below = (result[score_col] < exit_thresh)
    exit_event = was_above & now_below

    cooldown_remaining = pd.Series(0, index=result.index, dtype=int)
    counter = 0
    for i in range(len(result)):
        if exit_event.iloc[i]:
            counter = cooldown
        elif counter > 0:
            counter -= 1
        cooldown_remaining.iloc[i] = counter

    result["sig_in_cooldown"]       = cooldown_remaining > 0
    result["sig_cooldown_remaining"] = cooldown_remaining

    return result


# ═══════════════════════════════════════════════════════════════
#  GATE 6 — ENTRY CONFIRMATION
# ═══════════════════════════════════════════════════════════════

def _gate_entry(df: pd.DataFrame) -> pd.DataFrame:
    result    = df.copy()
    base_min  = _sp("entry_score_min")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_confirmed"] = 0
        result["sig_reason"]    = "no score"
        return result

    # ── Effective entry threshold (breadth-adjusted) ──────────
    entry_adj = result.get("entry_score_adj", 0.0)
    if isinstance(entry_adj, (int, float)):
        entry_adj = pd.Series(entry_adj, index=result.index)
    effective_min = base_min + entry_adj.fillna(0.0)

    result["sig_effective_entry_min"] = effective_min

    scores = result[score_col]

    # All gates must pass (RSI gate included)
    regime_ok   = result.get("sig_regime_ok", True)
    rsi_ok      = result.get("sig_rsi_ok", True)
    sector_ok   = result.get("sig_sector_ok", True)
    breadth_ok  = result.get("sig_breadth_ok", True)
    momentum_ok = result.get("sig_momentum_ok", False)
    cooldown    = result.get("sig_in_cooldown", False)

    confirmed = (
        (scores >= effective_min)
        & regime_ok
        & rsi_ok
        & sector_ok
        & breadth_ok
        & momentum_ok
        & (~cooldown)
    )

    result["sig_confirmed"] = confirmed.astype(int)

    # ── Reason annotation ─────────────────────────────────────
    # Priority order: RSI → regime → sector → breadth → cooldown
    #                 → momentum → score → fallback
    reasons = pd.Series("", index=result.index)

    reasons = reasons.where(
        confirmed,
        np.where(
            ~(rsi_ok.astype(bool) if not isinstance(
                rsi_ok, bool
            ) else rsi_ok),
            "rsi_out_of_range",
            np.where(
                ~regime_ok.astype(bool) if not isinstance(
                    regime_ok, bool
                ) else ~regime_ok,
                "regime_blocked",
                np.where(
                    ~sector_ok.astype(bool) if not isinstance(
                        sector_ok, bool
                    ) else ~sector_ok,
                    "sector_blocked",
                    np.where(
                        ~breadth_ok.astype(bool) if not isinstance(
                            breadth_ok, bool
                        ) else ~breadth_ok,
                        "breadth_weak",
                        np.where(
                            cooldown.astype(bool) if not isinstance(
                                cooldown, bool
                            ) else cooldown,
                            "cooldown",
                            np.where(
                                ~momentum_ok.astype(bool) if not isinstance(
                                    momentum_ok, bool
                                ) else ~momentum_ok,
                                "momentum_unconfirmed",
                                np.where(
                                    scores < effective_min,
                                    "score_below_entry",
                                    "no_signal",
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )

    reasons = reasons.where(~confirmed, "LONG")
    result["sig_reason"] = reasons

    return result


# ═══════════════════════════════════════════════════════════════
#  EXIT SIGNALS
# ═══════════════════════════════════════════════════════════════

def _compute_exits(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    exit_max = _sp("exit_score_max")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_exit"] = 0
        return result

    was_confirmed = result["sig_confirmed"].shift(1).fillna(0) == 1
    now_weak      = result[score_col] < exit_max

    result["sig_exit"] = (was_confirmed & now_weak).astype(int)

    # Also flag if breadth turned weak on an existing position
    if "breadth_regime" in result.columns:
        breadth_was_ok  = result["sig_breadth_ok"].shift(1, fill_value=True)
        breadth_now_bad = result["breadth_regime"] == "weak"
        result["sig_exit_breadth"] = (
            was_confirmed & breadth_now_bad & breadth_was_ok
        ).astype(int)
    else:
        result["sig_exit_breadth"] = 0

    return result


# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════

def _position_sizing(df: pd.DataFrame) -> pd.DataFrame:
    result   = df.copy()
    base     = _sp("base_position_pct")
    max_pos  = _sp("max_position_pct")

    score_col = (
        "score_adjusted" if "score_adjusted" in result.columns
        else "score_composite"
    )

    if score_col not in result.columns:
        result["sig_position_pct"] = 0.0
        return result

    scores = result[score_col].fillna(0)

    # Scale linearly from base → max as score goes 0.6 → 1.0
    low, high = 0.60, 1.0
    frac = ((scores - low) / (high - low)).clip(0, 1)
    raw  = base + frac * (max_pos - base)

    # ── Breadth-based scaling ─────────────────────────────────
    # In neutral/weak breadth, scale down position sizes
    if "breadth_regime" in result.columns:
        breadth_scale = result["breadth_regime"].map({
            "strong":  1.0,
            "neutral": _bpp("neutral_exposure"),
            "weak":    _bpp("weak_exposure"),
        }).fillna(1.0)
    else:
        breadth_scale = 1.0

    raw = raw * breadth_scale

    result["sig_position_pct"] = np.where(
        result["sig_confirmed"] == 1, raw, 0.0,
    )

    return result


# ═══════════════════════════════════════════════════════════════
#  MASTER FUNCTION
# ═══════════════════════════════════════════════════════════════

def generate_signals(
    df: pd.DataFrame,
    breadth: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Run the full signal pipeline on a single ticker's scored
    DataFrame.

    Gates (all must pass for sig_confirmed = 1):
      1.  RS regime        — stock in allowed regime
      1b. RSI range        — RSI between 30 and 70
      2.  Sector regime    — sector tide favourable
      3.  Breadth regime   — market not weak
      4.  Momentum streak  — N consecutive days above threshold
      5.  Cooldown         — not recently exited
      6.  Entry score      — composite above threshold

    Parameters
    ----------
    df : pd.DataFrame
        Output of the scoring pipeline, with RS and sector RS
        columns already merged.
    breadth : pd.DataFrame, optional
        Output of ``compute_all_breadth()``.  When provided,
        breadth regime gates and position-size scaling are active.

    Returns
    -------
    pd.DataFrame
        Original columns plus all ``sig_*`` columns.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    result = _gate_regime(df)
    result = _gate_rsi(result)
    result = _gate_sector(result)
    result = _gate_breadth(result, breadth)
    result = _gate_momentum(result)
    result = _gate_cooldown(result)
    result = _gate_entry(result)
    result = _compute_exits(result)
    result = _position_sizing(result)

    return result

####################################################################

"""
main.py
CASH — Composite Adaptive Signal Hierarchy
===========================================

Entry point.  One command → full pipeline → reports on disk.

Usage:
    python main.py                                # default run
    python main.py --portfolio 150000             # custom capital
    python main.py --positions positions.json     # with holdings
    python main.py --output-dir reports/          # custom output
    python main.py --text-only                    # skip HTML
    python main.py --tickers AAPL MSFT NVDA       # specific tickers
    python main.py --universe universes/core.json # custom universe
    python main.py --dry-run                      # score only
    python main.py --backtest                     # include backtest
    python main.py --verbose                      # debug logging
"""

import argparse
import json
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── CASH modules ────────────────────────────────────────────────
from common.config import (
    UNIVERSE,
    BENCHMARK_TICKER,
    PORTFOLIO_PARAMS,
    LOGS_DIR,
)
from pipeline.orchestrator import (
    Orchestrator,
    PipelineResult,
    run_full_pipeline,
)
from pipeline.runner import results_errors
from reports.recommendations import (
    build_report,
    to_text,
    to_html,
    save_text,
    save_html,
    print_report,
)

# Optional: portfolio rebalance view
try:
    from reports.portfolio_view import (
        build_rebalance_plan,
        save_rebalance_text,
        save_rebalance_html,
        print_rebalance,
    )
    _HAS_REBALANCE = True
except ImportError:
    _HAS_REBALANCE = False


# ═════════════════════════════════════════════════════════════════
#  LOGGING
# ═════════════════════════════════════════════════════════════════

def setup_logging(
    verbose: bool = False,
    log_file: str | None = None,
) -> str:
    """
    Configure root logger.

    If *log_file* is ``None`` the log is written to ``LOGS_DIR``
    (from ``common.config``) with a timestamped filename.

    Returns the resolved log-file path.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)-24s  %(message)s"
    datefmt = "%H:%M:%S"

    # Ensure LOGS_DIR exists
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    # Default log location: LOGS_DIR/cash_<timestamp>.log
    if log_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_path / f"cash_{ts}.log")
    else:
        # Relative paths resolve inside LOGS_DIR
        lf = Path(log_file)
        if not lf.is_absolute():
            log_file = str(logs_path / lf)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="w"),
    ]

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
        force=True,
    )
    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    return log_file


logger = logging.getLogger("cash.main")


# ═════════════════════════════════════════════════════════════════
#  CLI ARGUMENT PARSER
# ═════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cash",
        description="CASH — Composite Adaptive Signal Hierarchy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py
  python main.py --portfolio 200000 --positions holdings.json
  python main.py --dry-run --verbose
  python main.py --tickers AAPL MSFT NVDA GOOG
  python main.py --output-dir ~/reports --text-only
        """,
    )

    # ── portfolio ───────────────────────────────────────────────
    p.add_argument(
        "--portfolio", "-p", type=float, default=None,
        help="Total portfolio value in dollars (overrides config)",
    )
    p.add_argument(
        "--positions", type=str, default=None,
        help="Path to JSON file with current holdings "
             "(enables rebalance report)",
    )

    # ── universe ────────────────────────────────────────────────
    p.add_argument(
        "--universe", "-u", type=str, default=None,
        help="Path to universe JSON file (list of tickers)",
    )
    p.add_argument(
        "--tickers", "-t", type=str, nargs="+", default=None,
        help="Run on specific tickers only (space-separated)",
    )

    # ── output ──────────────────────────────────────────────────
    p.add_argument(
        "--output-dir", "-o", type=str, default="output",
        help="Directory for report files (default: output/)",
    )
    p.add_argument(
        "--text-only", action="store_true",
        help="Generate text reports only, skip HTML",
    )
    p.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress terminal output (files still saved)",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Also save structured report as JSON",
    )

    # ── run mode ────────────────────────────────────────────────
    p.add_argument(
        "--dry-run", action="store_true",
        help="Score and rank only — no portfolio or signals",
    )
    p.add_argument(
        "--backtest", action="store_true",
        help="Run historical backtest after pipeline",
    )
    p.add_argument(
        "--top-n", type=int, default=None,
        help="Only show top N buy candidates in output",
    )

    # ── debug ───────────────────────────────────────────────────
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )
    p.add_argument(
        "--log-file", type=str, default=None,
        help="Custom log filename (written inside LOGS_DIR)",
    )

    return p


# ═════════════════════════════════════════════════════════════════
#  POSITION LOADING
# ═════════════════════════════════════════════════════════════════

def load_positions(
    filepath: str,
) -> tuple[list[dict], float | None]:
    """
    Load current holdings from a JSON file.

    Supports two formats:

    Plain list::

        [
            {"ticker": "AAPL", "shares": 50,
             "avg_cost": 142.30, "current_price": 178.50},
            ...
        ]

    Wrapper with cash::

        {
            "positions": [ ... ],
            "cash": 25000.0
        }

    Returns ``(positions_list, cash_or_None)``.
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Positions file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        positions = data.get("positions", [])
        cash = data.get("cash", None)
    elif isinstance(data, list):
        positions = data
        cash = None
    else:
        logger.error(
            f"Unexpected positions format in {filepath}"
        )
        sys.exit(1)

    required = {"ticker", "shares", "avg_cost", "current_price"}
    for i, pos in enumerate(positions):
        missing = required - set(pos.keys())
        if missing:
            logger.error(
                f"Position {i} ({pos.get('ticker', '?')}) "
                f"missing fields: {missing}"
            )
            sys.exit(1)

    logger.info(
        f"Loaded {len(positions)} positions from {filepath}"
    )
    if cash is not None:
        logger.info(f"Cash from positions file: ${cash:,.0f}")

    return positions, cash


def load_universe_file(filepath: str) -> list[str]:
    """
    Load a universe JSON file and return ticker strings.

    Accepts a plain list of strings::

        ["AAPL", "MSFT", "NVDA"]

    or a list of objects with a ``"ticker"`` key::

        [{"ticker": "AAPL", "category": "Tech"}, ...]
    """
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Universe file not found: {filepath}")
        sys.exit(1)

    with open(path, "r") as f:
        data = json.load(f)

    if not isinstance(data, list) or len(data) == 0:
        logger.error("Universe file must be a non-empty list")
        sys.exit(1)

    if isinstance(data[0], str):
        return [t.upper() for t in data]
    elif isinstance(data[0], dict) and "ticker" in data[0]:
        return [d["ticker"].upper() for d in data]
    else:
        logger.error(
            "Universe entries must be strings or dicts "
            "with a 'ticker' key"
        )
        sys.exit(1)


# ═════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ═════════════════════════════════════════════════════════════════

def ensure_output_dir(output_dir: str) -> Path:
    """Create the output directory if it doesn't exist."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_filenames(
    output_dir: Path, date_str: str,
) -> dict:
    """Generate timestamped filenames for all output files."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "report_txt":     output_dir / f"cash_report_{date_str}_{ts}.txt",
        "report_html":    output_dir / f"cash_report_{date_str}_{ts}.html",
        "report_json":    output_dir / f"cash_report_{date_str}_{ts}.json",
        "rebalance_txt":  output_dir / f"cash_rebalance_{date_str}_{ts}.txt",
        "rebalance_html": output_dir / f"cash_rebalance_{date_str}_{ts}.html",
        "rebalance_json": output_dir / f"cash_rebalance_{date_str}_{ts}.json",
        "pipeline_json":  output_dir / f"cash_pipeline_{date_str}_{ts}.json",
    }


# ═════════════════════════════════════════════════════════════════
#  DRY-RUN HELPERS
# ═════════════════════════════════════════════════════════════════

def _run_dry(
    tickers: list[str],
    capital: float,
) -> PipelineResult:
    """
    Execute Phases 0–2 only (load → breadth/sector → score).

    Skips portfolio construction, signal generation, and
    report building.  Useful for inspecting raw scores.
    """
    orch = Orchestrator(
        universe=tickers,
        capital=capital,
        enable_breadth=True,
        enable_sectors=True,
        enable_signals=False,
        enable_backtest=False,
    )
    orch.load_data()
    orch.compute_universe_context()
    orch.run_tickers()

    errors = results_errors(orch._ticker_results)

    return PipelineResult(
        ticker_results=orch._ticker_results,
        scored_universe=orch._scored_universe,
        snapshots=orch._snapshots,
        breadth=orch._breadth,
        breadth_scores=orch._breadth_scores,
        sector_rs=orch._sector_rs,
        bench_df=orch._bench_df,                    # ← NEW
        errors=errors,
        timings=orch._timings,
        run_date=pd.Timestamp.now(),
    )


def _print_dry_run_summary(
    result: PipelineResult,
    top_n: int = 20,
) -> None:
    """Print a compact scoring table for dry-run mode."""
    snaps = result.snapshots[:top_n]
    if not snaps:
        print("  No scored tickers.")
        return

    print()
    print("  DRY RUN — Top Scored Tickers")
    print("  " + "─" * 58)
    print(
        f"  {'Rank':<5} {'Ticker':<8} {'Composite':>10} "
        f"{'Signal':<10} {'Close':>10}"
    )
    print("  " + "─" * 58)

    for i, s in enumerate(snaps, 1):
        ticker = s.get("ticker", "???")
        score = s.get("composite", 0)
        signal = s.get("signal", "—")
        close = s.get("close", 0)
        print(
            f"  {i:<5} {ticker:<8} {score:>10.1f} "
            f"{signal:<10} {close:>10.2f}"
        )

    print("  " + "─" * 58)
    print(
        f"  {result.n_tickers} scored, "
        f"{result.n_errors} errors"
    )
    print()


# ═════════════════════════════════════════════════════════════════
#  REBALANCE HANDLER
# ═════════════════════════════════════════════════════════════════

def _handle_rebalance(
    args: argparse.Namespace,
    report: dict,
    current_positions: list[dict],
    positions_cash: float | None,
    capital: float,
    filenames: dict,
) -> None:
    """Build and save the rebalance plan."""
    logger.info("")
    logger.info(
        "─── BUILDING REBALANCE PLAN ───────────────────────"
    )

    cash_for_rebalance = (
        positions_cash
        if positions_cash is not None
        else capital * 0.10
    )

    try:
        plan = build_rebalance_plan(
            report=report,
            current_positions=current_positions,
            cash_balance=cash_for_rebalance,
            portfolio_value=capital,
        )
    except Exception as e:
        logger.warning(f"build_rebalance_plan failed: {e}")
        return

    # Terminal
    if not args.quiet:
        try:
            print()
            print_rebalance(plan)
        except Exception as e:
            logger.warning(f"print_rebalance failed: {e}")

    # Text
    try:
        save_rebalance_text(
            plan, str(filenames["rebalance_txt"])
        )
        logger.info(
            f"Rebalance text → {filenames['rebalance_txt']}"
        )
    except Exception as e:
        logger.warning(f"save_rebalance_text failed: {e}")

    # HTML
    if not args.text_only:
        try:
            save_rebalance_html(
                plan, str(filenames["rebalance_html"])
            )
            logger.info(
                f"Rebalance HTML → {filenames['rebalance_html']}"
            )
        except Exception as e:
            logger.warning(
                f"save_rebalance_html failed: {e}"
            )

    # JSON
    if args.json and hasattr(plan, "to_dict"):
        _save_json(
            plan.to_dict(),
            str(filenames["rebalance_json"]),
        )
        logger.info(
            f"Rebalance JSON → {filenames['rebalance_json']}"
        )

    # Summary log
    if hasattr(plan, "trade_count"):
        logger.info(f"Trades required: {plan.trade_count}")
    if hasattr(plan, "net_cash_impact"):
        logger.info(
            f"Net cash impact: ${plan.net_cash_impact:+,.0f}"
        )
    if hasattr(plan, "warnings") and plan.warnings:
        for w in plan.warnings:
            logger.warning(w)


# ═════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═════════════════════════════════════════════════════════════════

def main():
    parser = build_parser()
    args = parser.parse_args()

    # ── setup ───────────────────────────────────────────────────
    output_dir = ensure_output_dir(args.output_dir)
    date_str = datetime.now().strftime("%Y%m%d")
    filenames = generate_filenames(output_dir, date_str)

    log_file = setup_logging(
        verbose=args.verbose,
        log_file=args.log_file,
    )

    logger.info("=" * 60)
    logger.info("  CASH — Composite Adaptive Signal Hierarchy")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)

    t_start = time.time()

    # ── capital ─────────────────────────────────────────────────
    capital = args.portfolio or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )
    logger.info(f"Portfolio value: ${capital:,.0f}")

    # ── universe ────────────────────────────────────────────────
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
        logger.info(f"CLI tickers: {', '.join(tickers)}")
    elif args.universe:
        tickers = load_universe_file(args.universe)
        logger.info(
            f"Universe from {args.universe}: "
            f"{len(tickers)} tickers"
        )
    else:
        tickers = list(UNIVERSE)
        logger.info(
            f"Default universe: {len(tickers)} tickers"
        )

    # ── load positions (optional) ───────────────────────────────
    current_positions = None
    positions_cash = None
    if args.positions:
        current_positions, positions_cash = load_positions(
            args.positions
        )

    # ── run pipeline ────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── RUNNING PIPELINE ──────────────────────────────"
    )

    if args.dry_run:
        logger.info(
            "DRY RUN — scoring only, no portfolio/signals"
        )
        result = _run_dry(tickers, capital)
    else:
        result = run_full_pipeline(
            universe=tickers,
            capital=capital,
            enable_breadth=True,
            enable_sectors=True,
            enable_signals=True,
            enable_backtest=args.backtest,
        )

    t_pipeline = time.time()
    logger.info(
        f"Pipeline completed in {t_pipeline - t_start:.1f}s"
    )
    logger.info(result.summary())

    # ── terminal summary ────────────────────────────────────────
    if not args.quiet:
        print()
        print(result.summary())

    # ── dry-run: print scores and exit ──────────────────────────
    if args.dry_run:
        if not args.quiet:
            _print_dry_run_summary(
                result, top_n=args.top_n or 20
            )
        _finish(t_start, output_dir, log_file)
        return result

    # ── reports ─────────────────────────────────────────────────
    logger.info("")
    logger.info(
        "─── SAVING REPORTS ────────────────────────────────"
    )

    # The orchestrator already calls build_report() internally
    # and stores the result in PipelineResult.recommendation_report
    report = result.recommendation_report

    if report is None:
        logger.warning(
            "No recommendation report was generated — "
            "check pipeline logs for errors"
        )
    else:
        # Apply --top-n filter to the buy list
        if args.top_n and args.top_n > 0:
            for key in ("ranked_buys", "buy_list"):
                if key in report and isinstance(report[key], list):
                    report[key] = report[key][: args.top_n]
            logger.info(f"Filtered to top {args.top_n} buys")

        # Terminal output
        if not args.quiet:
            try:
                print()
                print_report(report)
            except Exception as e:
                logger.warning(f"print_report failed: {e}")

        # Save text report
        try:
            save_text(report, str(filenames["report_txt"]))
            logger.info(
                f"Text report  → {filenames['report_txt']}"
            )
        except Exception as e:
            logger.warning(f"save_text failed: {e}")

        # Save HTML report
        if not args.text_only:
            try:
                save_html(
                    report, str(filenames["report_html"])
                )
                logger.info(
                    f"HTML report  → {filenames['report_html']}"
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        # Save JSON report
        if args.json:
            _save_json(report, str(filenames["report_json"]))
            logger.info(
                f"JSON report  → {filenames['report_json']}"
            )

    # Pipeline JSON (verbose only — for debugging)
    if args.verbose:
        _save_pipeline_result(
            result, str(filenames["pipeline_json"])
        )
        logger.info(
            f"Pipeline JSON → {filenames['pipeline_json']}"
        )

    # ── rebalance plan (if positions provided) ──────────────────
    if current_positions is not None:
        if _HAS_REBALANCE and report is not None:
            _handle_rebalance(
                args, report, current_positions,
                positions_cash, capital, filenames,
            )
        elif not _HAS_REBALANCE:
            logger.warning(
                "reports.portfolio_view not available — "
                "skipping rebalance plan"
            )
        else:
            logger.warning(
                "No report available — "
                "skipping rebalance plan"
            )
    else:
        logger.info(
            "No --positions file — "
            "skipping rebalance plan. "
            "Use --positions <file.json> to generate one."
        )

    # ── done ────────────────────────────────────────────────────
    _finish(t_start, output_dir, log_file)
    return result


def _finish(
    t_start: float, output_dir: Path, log_file: str,
) -> None:
    """Log the closing banner."""
    elapsed = time.time() - t_start
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  CASH run complete in {elapsed:.1f}s")
    logger.info(f"  Reports: {output_dir}/")
    logger.info(f"  Log:     {log_file}")
    logger.info("=" * 60)


# ═════════════════════════════════════════════════════════════════
#  PROGRAMMATIC ENTRY POINT
# ═════════════════════════════════════════════════════════════════

def run(
    portfolio_value: float | None = None,
    tickers: list[str] | None = None,
    universe_path: str | None = None,
    positions: list[dict] | None = None,
    cash_balance: float | None = None,
    output_dir: str = "output",
    save_files: bool = True,
    verbose: bool = False,
    enable_backtest: bool = False,
) -> dict:
    """
    Programmatic entry point for notebooks and scripts.

    Returns a dict with keys: ``result``, ``report``,
    ``plan``, ``filenames``.
    """
    setup_logging(verbose=verbose)

    capital = portfolio_value or PORTFOLIO_PARAMS.get(
        "total_capital", 100_000
    )

    # Resolve universe
    if tickers:
        uni = [t.upper() for t in tickers]
    elif universe_path:
        uni = load_universe_file(universe_path)
    else:
        uni = list(UNIVERSE)

    # Run
    result = run_full_pipeline(
        universe=uni,
        capital=capital,
        enable_backtest=enable_backtest,
    )

    output = {
        "result":   result,
        "report":   result.recommendation_report,
        "plan":     None,
        "filenames": {},
    }

    # Rebalance
    if (
        positions is not None
        and _HAS_REBALANCE
        and result.recommendation_report
    ):
        cb = (
            cash_balance
            if cash_balance is not None
            else capital * 0.10
        )
        try:
            plan = build_rebalance_plan(
                report=result.recommendation_report,
                current_positions=positions,
                cash_balance=cb,
                portfolio_value=capital,
            )
            output["plan"] = plan
        except Exception as e:
            logger.warning(f"Rebalance plan failed: {e}")

    # Save files
    if save_files:
        out = ensure_output_dir(output_dir)
        date_str = datetime.now().strftime("%Y%m%d")
        fnames = generate_filenames(out, date_str)

        if result.recommendation_report:
            try:
                save_text(
                    result.recommendation_report,
                    str(fnames["report_txt"]),
                )
                output["filenames"]["report_txt"] = str(
                    fnames["report_txt"]
                )
            except Exception as e:
                logger.warning(f"save_text failed: {e}")

            try:
                save_html(
                    result.recommendation_report,
                    str(fnames["report_html"]),
                )
                output["filenames"]["report_html"] = str(
                    fnames["report_html"]
                )
            except Exception as e:
                logger.warning(f"save_html failed: {e}")

        if output["plan"] and _HAS_REBALANCE:
            try:
                save_rebalance_text(
                    output["plan"],
                    str(fnames["rebalance_txt"]),
                )
                save_rebalance_html(
                    output["plan"],
                    str(fnames["rebalance_html"]),
                )
                output["filenames"]["rebalance_txt"] = str(
                    fnames["rebalance_txt"]
                )
                output["filenames"]["rebalance_html"] = str(
                    fnames["rebalance_html"]
                )
            except Exception as e:
                logger.warning(f"Rebalance save failed: {e}")

    return output


# ═════════════════════════════════════════════════════════════════
#  SERIALISATION HELPERS
# ═════════════════════════════════════════════════════════════════

def _save_json(data: dict, filepath: str) -> None:
    """Save a dict to JSON with numpy/pandas fallback."""
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=_json_serialiser)


def _save_pipeline_result(
    result: PipelineResult, filepath: str,
) -> None:
    """
    Serialise a ``PipelineResult`` to JSON for debugging.

    Large DataFrames are summarised (shape + columns) to keep
    the file size manageable.
    """
    out: dict = {}

    out["run_date"] = result.run_date.isoformat()
    out["as_of"] = (
        result.as_of.isoformat() if result.as_of else None
    )
    out["n_tickers"] = result.n_tickers
    out["n_errors"] = result.n_errors
    out["total_time"] = result.total_time
    out["timings"] = result.timings
    out["errors"] = result.errors
    out["snapshots"] = result.snapshots
    out["portfolio"] = result.portfolio

    # Summarise DataFrames instead of dumping full contents
    for name, df in [
        ("rankings", result.rankings),
        ("signals", result.signals),
        ("breadth", result.breadth),
    ]:
        if df is not None and hasattr(df, "shape"):
            out[name] = {
                "shape": list(df.shape),
                "columns": list(df.columns),
            }
        else:
            out[name] = None

    with open(filepath, "w") as f:
        json.dump(out, f, indent=2, default=_json_serialiser)


def _json_serialiser(obj):
    """Fallback JSON serialiser for numpy / pandas types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


# ═════════════════════════════════════════════════════════════════
#  ENTRY
# ═════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()

