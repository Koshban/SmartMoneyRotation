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
    # ── Scores ────────────────────────────────────────────────
    "score_adjusted", "score_composite",
    "score_rotation", "score_momentum", "score_volatility",
    "score_microstructure", "score_breadth",
    "score_percentile", "convergence_label",
    # ── Per-ticker signal gates ───────────────────────────────
    "sig_confirmed", "sig_position_pct", "sig_reason",
    "sig_regime_ok", "sig_sector_ok", "sig_breadth_ok", "sig_momentum_ok",
    "sig_in_cooldown", "sig_effective_entry_min",
    # ── Relative strength ─────────────────────────────────────
    "rs_regime", "rs_zscore",
    # ── Sector context ────────────────────────────────────────
    "sect_rs_regime", "sect_rs_rank", "sect_rs_pctrank",
    "sector_name", "sector_tailwind",
    # ── Breadth context ───────────────────────────────────────
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