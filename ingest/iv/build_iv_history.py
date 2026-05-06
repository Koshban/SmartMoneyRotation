"""Daily IV snapshot for {market}_iv_history (US only in v1).

Monthly contracts only. Strict 3rd-Friday expiries, two nearest with
0 < DTE ≤ 70  →  labelled M1 and M2. No tenor interpolation; the actual
DTE of each chain is recorded in the `dte` column.

Methodology
-----------
  - atm_iv         : mean of nearest-strike call IV and put IV at that expiry
  - atm_iv_call    : nearest-strike call IV
  - atm_iv_put     : nearest-strike put IV
  - skew_25d       : 25Δ-put IV − 25Δ-call IV at the same expiry
  - term_slope_m1_m2 : (iv_M2 − iv_M1) / (dte_M2 − dte_M1) × 30
                       (vol points per 30 days; same value on M1 & M2 rows)

Quote-quality filter (applied per option *before* its IV is used)
-----------------------------------------------------------------
  bid > 0, ask > 0
  (ask − bid)  ≤  max(0.20, 0.70 × mid)      # deliberately wide for EoD data
  mid          ≥  0.05
  openInterest ≥  5                           # only if field is present

Post-solve sanity clips
-----------------------
  atm_iv > 3.0                 → NULL atm_iv (drops that tenor's IV)
  |call_iv − put_iv| > 0.10    → NULL atm_iv (one-sided pollution)
  |skew_25d| > 0.20            → NULL skew_25d only (keep atm_iv)

Example
-------
  python -m ingest.iv.build_iv_history --market us
  python -m ingest.iv.build_iv_history --market us --symbols AAPL,SPY --dry-run
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
from datetime import date, datetime

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

from common.universe import get_universe_for_market
from common.db_writer import upsert_iv_history
from utils.loging_setup import setup_logging
from collections import defaultdict

logger = logging.getLogger("ingest.iv.build_iv_history")

# ---------- config ----------
MAX_DTE          = 80       # ignore expiries beyond this
MIN_IV           = 0.01     # floor: yfinance occasionally returns 0
MAX_IV           = 5.0      # raw-row ceiling
MAX_ATM_IV       = 3.0      # post-solve clip on atm_iv
MAX_CP_DIVERGE   = 0.10     # |call_iv − put_iv| clip
MAX_SKEW         = 0.20     # |skew_25d| clip

DELTA_TARGET     = 0.25
MIN_OI           = 5
MIN_MID          = 0.05
MAX_SPREAD_ABS   = 0.20
MAX_SPREAD_REL   = 0.70


def _atm_iv_parabolic(
    df: pd.DataFrame,
    spot: float,
    n: int = 5,
    side: str = "",
) -> float | None:
    """
    Estimate ATM IV by fitting a quadratic in log-moneyness x=ln(K/S)
    to the n strikes nearest spot, then evaluating at x=0.

    Corrects for strike-grid offset: with $5-wide TSLA strikes around
    spot=388.94, the nearest call (390) and nearest put (385) sit on
    different parts of the smile. Interpolating both to K=spot makes
    them comparable.
    """
    if df is None or df.empty:
        return None

    # drop obviously broken IVs before picking neighbors
    df = df[
        df["impliedVolatility"].between(MIN_IV, MAX_IV)
    ]
    if df.empty:
        return None

    d = df.assign(_dist=(df["strike"] - spot).abs()).nsmallest(n, "_dist")
    if len(d) == 0:
        return None
    
    # reject if even the *nearest* surviving strike is too far from spot.
    # With liquid names the nearest is usually <1% away; >10% means the
    # cleaner gutted the chain and we're fitting to OTM wings.
    nearest_moneyness = float(d["_dist"].min()) / spot
    if nearest_moneyness > 0.10:
        logger.debug(
            "        parabolic[%s]: nearest strike %.2f is %.1f%% from spot %.2f, "
            "chain too sparse for ATM estimate",
            side, float(d.iloc[0]["strike"]), nearest_moneyness * 100, spot,
        )
        return None

    if len(d) < 3:
        val = float(d["impliedVolatility"].mean())
        logger.debug(
            "        parabolic[%s]: only %d strikes near spot, using mean=%.4f",
            side, len(d), val,
        )
        return val if MIN_IV <= val <= MAX_IV else None

    K = d["strike"].to_numpy(dtype=float)
    iv = d["impliedVolatility"].to_numpy(dtype=float)
    x = np.log(K / spot)

    try:
        a, b, c = np.polyfit(x, iv, 2)
        atm = float(c)
    except Exception as e:
        med = float(np.median(iv))
        logger.debug(
            "        parabolic[%s]: polyfit failed (%s), using median=%.4f",
            side, e, med,
        )
        return med if MIN_IV <= med <= MAX_IV else None

    lo, hi = float(iv.min()), float(iv.max())
    if not (lo - 0.02 <= atm <= hi + 0.02):
        med = float(np.median(iv))
        logger.debug(
            "        parabolic[%s]: fit atm=%.4f outside [%.4f, %.4f], "
            "using median=%.4f",
            side, atm, lo, hi, med,
        )
        return med if MIN_IV <= med <= MAX_IV else None

    # final bound check — mirrors old _atm_iv_one_side behavior
    if not (MIN_IV <= atm <= MAX_IV):
        logger.debug(
            "        parabolic[%s]: atm=%.4f outside [%.4f, %.4f], rejecting",
            side, atm, MIN_IV, MAX_IV,
        )
        return None

    logger.debug(
        "        parabolic[%s]: n=%d strikes [%.2f..%.2f] iv=[%.4f..%.4f] → atm=%.4f",
        side, len(d), K.min(), K.max(), lo, hi, atm,
    )
    return atm


# ---------- expiry helpers ----------

def _third_friday(year: int, month: int) -> date:
    """The 3rd Friday of a given calendar month."""
    for day in range(15, 22):
        d = date(year, month, day)
        if d.weekday() == 4:
            return d
    raise ValueError(f"no 3rd Friday in {year}-{month:02d}")

def _pick_monthlies(
    expiries: list[str], today: date, max_dte: int
) -> list[tuple[int, str]]:
    """
    Pick up to 2 standard-monthly expiries within DTE window.
    For each calendar month, choose the expiry closest to its 3rd Friday
    (handles Juneteenth/Good-Friday shifts that move expiry to Thursday).
    Only Thu/Fri expiries in days 15-22 are considered.
    """
    by_month: dict[tuple[int, int], list[tuple[int, date, str]]] = defaultdict(list)
    for exp_str in expiries:
        try:
            exp_dt = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_dt - today).days
        if dte <= 0 or dte > max_dte:
            continue
        # standard monthly window: Thursday or Friday, days 15-22
        if exp_dt.weekday() in (3, 4) and 15 <= exp_dt.day <= 22:
            by_month[(exp_dt.year, exp_dt.month)].append((dte, exp_dt, exp_str))

    monthlies: list[tuple[int, str]] = []
    for (yr, mo), candidates in by_month.items():
        target = _third_friday(yr, mo)
        # pick the expiry closest to the 3rd Friday (Thu shift = -1 day)
        best = min(candidates, key=lambda x: abs((x[1] - target).days))
        monthlies.append((best[0], best[2]))

    monthlies.sort()
    return monthlies[:2]


# ---------- Black-Scholes delta (r=0, q=0) ----------

def _call_delta(S: float, K: float, T: float, sigma: float) -> float:
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return float("nan")
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    return float(norm.cdf(d1))


def _put_delta(S: float, K: float, T: float, sigma: float) -> float:
    c = _call_delta(S, K, T, sigma)
    return c - 1.0 if c == c else float("nan")


# ---------- chain hygiene ----------

def _clean_chain(df: pd.DataFrame) -> pd.DataFrame:
    """Apply quote-quality filter to a yfinance call or put chain."""
    if df is None or df.empty:
        return pd.DataFrame()
    needed = {"strike", "impliedVolatility", "bid", "ask"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()

    df = df.dropna(subset=["strike", "impliedVolatility", "bid", "ask"]).copy()
    df = df[(df["strike"] > 0) & (df["bid"] > 0) & (df["ask"] > 0)]
    if df.empty:
        return df

    df["mid"]    = (df["bid"] + df["ask"]) / 2.0
    df["spread"] = df["ask"] - df["bid"]

    spread_cap = np.maximum(MAX_SPREAD_ABS, MAX_SPREAD_REL * df["mid"])
    df = df[df["spread"] <= spread_cap]
    df = df[df["mid"]    >= MIN_MID]

    if "openInterest" in df.columns:
        oi = df["openInterest"].fillna(0)
        df = df[oi >= MIN_OI]

    df = df[(df["impliedVolatility"] >= MIN_IV) &
            (df["impliedVolatility"] <= MAX_IV)]
    return df.reset_index(drop=True)


def _atm_iv_one_side(df: pd.DataFrame, spot: float) -> float | None:
    """IV at strike closest to spot, single side (calls or puts)."""
    if df.empty:
        return None
    row = df.iloc[(df["strike"] - spot).abs().argmin()]
    iv = float(row["impliedVolatility"])
    return iv if MIN_IV <= iv <= MAX_IV else None


def _skew_25d(calls: pd.DataFrame, puts: pd.DataFrame,
              spot: float, T_years: float) -> float | None:
    """25Δ put IV − 25Δ call IV at one expiry."""
    if calls.empty or puts.empty or T_years <= 0:
        return None

    c_deltas = calls.apply(
        lambda r: _call_delta(spot, float(r["strike"]), T_years,
                              float(r["impliedVolatility"])),
        axis=1,
    )
    c_diff = (c_deltas - DELTA_TARGET).abs()
    if c_diff.isna().all():
        return None
    iv_call_25d = float(calls.loc[c_diff.idxmin(), "impliedVolatility"])

    p_deltas = puts.apply(
        lambda r: _put_delta(spot, float(r["strike"]), T_years,
                             float(r["impliedVolatility"])),
        axis=1,
    )
    p_diff = (p_deltas + DELTA_TARGET).abs()
    if p_diff.isna().all():
        return None
    iv_put_25d = float(puts.loc[p_diff.idxmin(), "impliedVolatility"])

    return iv_put_25d - iv_call_25d


# ---------- per-symbol snapshot ----------

def snapshot_symbol(symbol: str, today: date) -> list[dict]:
    """Return up to 2 rows (M1, M2) for this symbol."""
    logger.debug("%s: start", symbol)
    try:
        tk = yf.Ticker(symbol)

        try:
            all_expiries = tk.options
        except Exception:
            logger.exception("  %s: tk.options raised", symbol)
            return []

        if not all_expiries:
            logger.debug("  %s: tk.options is empty (no chain available)", symbol)
            return []

        logger.debug("  %s: %d expiries available", symbol, len(all_expiries))

        # 1. select the two nearest 3rd-Friday expiries within the DTE window
        candidates = _pick_monthlies(list(all_expiries), today, MAX_DTE)

        if not candidates:
            logger.debug(
                "  %s: no monthly expiries within DTE≤%d (checked %d expiries)",
                symbol, MAX_DTE, len(all_expiries),
            )
            return []
        logger.debug("  %s: monthly candidates → %s", symbol, candidates)

        # 2. spot
        try:
            hist = tk.history(period="2d", auto_adjust=False)
        except Exception:
            logger.exception("  %s: tk.history raised", symbol)
            return []

        if hist.empty:
            logger.debug("  %s: history empty — cannot derive spot", symbol)
            return []
        spot = float(hist["Close"].iloc[-1])
        if spot <= 0:
            logger.warning("  %s: non-positive spot (%.4f) — skipping", symbol, spot)
            return []
        logger.debug("  %s: spot=%.4f", symbol, spot)

        # 3. per-tenor compute
        rows: list[dict] = []
        iv_for_slope: dict[str, float] = {}
        dte_for_slope: dict[str, int] = {}

        for idx, (dte, exp_str) in enumerate(candidates):
            label = f"M{idx + 1}"
            try:
                ch = tk.option_chain(exp_str)
            except Exception as e:
                logger.warning(
                    "  %s %s (%s): chain fetch fail: %s",
                    symbol, label, exp_str, e,
                )
                continue

            n_calls_raw = len(ch.calls) if ch.calls is not None else 0
            n_puts_raw  = len(ch.puts)  if ch.puts  is not None else 0
            logger.debug(
                "  %s %s raw cols=%s sample=%s",
                symbol, label, list(ch.calls.columns),
                ch.calls.head(2).to_dict("records"),
            )
            calls = _clean_chain(ch.calls)
            puts  = _clean_chain(ch.puts)

            logger.debug(
                "  %s %s (%s, dte=%d): calls %d→%d, puts %d→%d after clean",
                symbol, label, exp_str, dte,
                n_calls_raw, len(calls), n_puts_raw, len(puts),
            )

            if calls.empty or puts.empty:
                logger.debug(
                    "  %s %s (%s): empty after filter "
                    "(calls_raw=%d puts_raw=%d)",
                    symbol, label, exp_str, n_calls_raw, n_puts_raw,
                )
                continue

            atmc = _atm_iv_parabolic(calls, spot, n=5, side="C")
            atmp = _atm_iv_parabolic(puts,  spot, n=5, side="P")

            if atmc is None or atmp is None:
                logger.warning(
                "  %s %s dte=%d: insufficient strikes for parabolic ATM "
                "(call=%s put=%s) → NULL atm_iv",
                symbol, label, dte, atmc, atmp,
                )
                continue
            else:
                atm = (atmc + atmp) / 2
                logger.debug(
                "  %s %s dte=%d: atm_call=%.4f atm_put=%.4f atm=%.4f",
                symbol, label, dte, atmc, atmp, atm,
            )

            # sanity clip 1: absolute level
            if atm is not None and atm > MAX_ATM_IV:
                logger.warning(
                    "  %s %s dte=%d: atm_iv=%.3f > %s → NULL atm_iv",
                    symbol, label, dte, atm, MAX_ATM_IV,
                )
                atm = None

            # sanity clip 2: call/put divergence (relative to ATM level)
            if atm is not None:
                diff = abs(atmc - atmp)
                rel = diff / atm if atm > 0 else float("inf")
                if rel > MAX_CP_DIVERGE:
                    logger.warning(
                        "  %s %s dte=%d: |call-put|=%.3f rel=%.1f%% > %.0f%% → NULL atm_iv "
                        "(atm_call=%.4f atm_put=%.4f atm=%.4f)",
                        symbol, label, dte, diff, rel * 100, MAX_CP_DIVERGE * 100,
                        atmc, atmp, atm,
                    )
                    atm = None
                else:
                    logger.debug(
                        "  %s %s dte=%d: c/p divergence ok (|c-p|=%.3f rel=%.1f%%)",
                        symbol, label, dte, diff, rel * 100,
                    )

            # 25Δ skew, with clip
            T = dte / 365.0
            skew = _skew_25d(calls, puts, spot, T)
            if skew is not None and abs(skew) > MAX_SKEW:
                logger.warning(
                    "  %s %s dte=%d: |skew|=%.3f > %s → NULL skew",
                    symbol, label, dte, abs(skew), MAX_SKEW,
                )
                skew = None

            rows.append({
                "date": today,
                "symbol": symbol,
                "tenor_bucket": label,
                "dte": dte,
                "atm_iv": atm,
                "atm_iv_call": atmc,
                "atm_iv_put": atmp,
                "skew_25d": skew,
                "term_slope_m1_m2": None,   # filled below
            })

            if atm is not None:
                iv_for_slope[label] = atm
                dte_for_slope[label] = dte

        # 4. term slope (vol points per 30 days)
        slope: float | None = None
        if "M1" in iv_for_slope and "M2" in iv_for_slope:
            d1 = dte_for_slope["M1"]
            d2 = dte_for_slope["M2"]
            if d2 > d1:
                slope = (iv_for_slope["M2"] - iv_for_slope["M1"]) / (d2 - d1) * 30.0
                logger.debug(
                    "  %s: term_slope_m1_m2=%.4f (M1=%.4f@%dd, M2=%.4f@%dd)",
                    symbol, slope,
                    iv_for_slope["M1"], d1, iv_for_slope["M2"], d2,
                )
        for r in rows:
            r["term_slope_m1_m2"] = slope

        if not rows:
            logger.debug("  %s: produced 0 rows after all gates", symbol)
        else:
            logger.debug("  %s: produced %d row(s)", symbol, len(rows))

        return rows

    except Exception:
        logger.exception("  %s: unexpected failure in snapshot_symbol", symbol)
        return []


# ---------- main ----------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=["us", "US"])
    ap.add_argument("--symbols", help="Comma-separated override")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="DEBUG-level logging")
    ap.add_argument("--log-file", default=None,
                    help="Override log filename (relative resolves inside LOGS_DIR)")
    args = ap.parse_args()

    market = args.market.lower()

    log_path = setup_logging(
        name=f"iv_history_{market}",
        verbose=args.verbose,
        log_file=args.log_file,
    )

    today = date.today()

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]
    else:
        symbols = get_universe_for_market(market)
    symbols = list(dict.fromkeys(s.strip().upper() for s in symbols))
    if args.limit:
        symbols = symbols[: args.limit]

    logger.info(
        "build_iv_history start: market=%s symbols=%d date=%s "
        "(monthly only, DTE ≤ %d)",
        market.upper(), len(symbols), today, MAX_DTE,
    )
    logger.debug("log file: %s", log_path)
    logger.debug("universe: %s", symbols)

    all_rows: list[dict] = []
    failed: list[str] = []

    try:
        for i, sym in enumerate(symbols, 1):
            if i % 25 == 0:
                logger.info(
                    "  progress %d/%d  ok_rows=%d fail=%d",
                    i, len(symbols), len(all_rows), len(failed),
                )
            try:
                rows = snapshot_symbol(sym, today)
            except Exception:
                logger.exception("  %s: snapshot_symbol crashed", sym)
                failed.append(sym)
                continue

            if rows:
                all_rows.extend(rows)
            else:
                failed.append(sym)

        logger.info(
            "snapshot complete: %d/%d symbols, %d rows total",
            len(symbols) - len(failed), len(symbols), len(all_rows),
        )

        if all_rows:
            df = pd.DataFrame(all_rows)
            n = upsert_iv_history(df, market=market, dry_run=args.dry_run)
            logger.info(
                "  → wrote %d rows (%s)",
                n, "DRY RUN" if args.dry_run else "committed",
            )
        else:
            logger.warning("no rows produced — nothing to write")

        if failed:
            preview = failed[:20]
            more = " ..." if len(failed) > 20 else ""
            logger.warning(
                "failed symbols (%d): %s%s", len(failed), preview, more,
            )

        # Summary block — easy to grep across daily runs
        logger.info("=" * 60)
        logger.info(
            "SUMMARY  market=%s  ok=%d  fail=%d  rows=%d",
            market.upper(),
            len(symbols) - len(failed),
            len(failed),
            len(all_rows),
        )
        logger.info("log file: %s", log_path)
        logger.info("=" * 60)

        return 0

    except Exception:
        logger.exception("build_iv_history crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())