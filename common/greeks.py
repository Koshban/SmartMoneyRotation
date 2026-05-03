"""
common/greeks.py

Black-Scholes pricing and greeks for European options on
dividend-paying underlyings (Merton 1973 extension).

WHY THIS EXISTS
---------------
yfinance returns implied vol but NOT greeks. We compute them at ingest
time so downstream code never has to special-case missing greeks.
IBKR's `modelGreeks` is preferred when available — skip this module
and use IBKR's values directly (set greeks_source='ibkr_model').

CONVENTIONS (match IBKR / industry trader usage)
------------------------------------------------
    S        spot price of underlying
    K        strike
    T        time to expiry in YEARS (ACT/365 calendar)
    r        risk-free rate, decimal annualized (0.04 = 4%)
    q        continuous dividend yield, decimal annualized (0.015 = 1.5%)
    sigma    implied volatility, decimal annualized (0.25 = 25% IV)
    opt_type 'C' for call, 'P' for put

    delta    per $1 move in S        (calls: 0..1,  puts: -1..0)
    gamma    per $1 move in S        (always >= 0)
    vega     per 1 vol-POINT move    (i.e. d/dsigma * 0.01)
    theta    per CALENDAR DAY        (i.e. d/dT      / 365), usually negative
    rho      per 1 rate-POINT move   (i.e. d/dr      * 0.01)

AMERICAN vs EUROPEAN
--------------------
US/HK equity options are American. BS is European. The early-exercise
premium is negligible for OTM contracts without imminent dividends —
which covers ~all wheel-strategy candidates. Don't use these greeks
to price deep-ITM puts on high-yield names just before ex-div.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import norm

LOG = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────

DAYS_PER_YEAR = 365.0      # ACT/365; switch to 252 only if you commit
                           # everywhere to trading-day theta
EPS = 1e-12                # numerical floor for T, sigma

# Default risk-free rates (annualized, decimal). Override via
# get_risk_free_rate() once you wire a live source.
DEFAULT_RATES: dict[str, float] = {
    "us": 0.0425,          # ~3M T-bill area, May 2026
    "hk": 0.0400,          # ~3M HIBOR area
}

# Default continuous dividend yields. For per-symbol accuracy, pass q
# explicitly from a dividend-yield lookup table.
DEFAULT_DIV_YIELD: float = 0.0


# ─────────────────────────────────────────────────────────────────
# Date / time helpers
# ─────────────────────────────────────────────────────────────────

def year_fraction(from_date: date, to_date: date) -> float:
    """Calendar-day year fraction (ACT/365)."""
    if isinstance(from_date, datetime):
        from_date = from_date.date()
    if isinstance(to_date, datetime):
        to_date = to_date.date()
    days = (to_date - from_date).days
    return max(days, 0) / DAYS_PER_YEAR


def dte_to_T(dte_days: float | int | np.ndarray) -> np.ndarray:
    """Convert days-to-expiry → T in years (vectorized-safe)."""
    return np.maximum(np.asarray(dte_days, dtype=float), 0.0) / DAYS_PER_YEAR


# ─────────────────────────────────────────────────────────────────
# Risk-free rate (placeholder — wire to live source later)
# ─────────────────────────────────────────────────────────────────

def get_risk_free_rate(market: str) -> float:
    """
    Return the annualized risk-free rate for a market.

    Currently returns DEFAULT_RATES[market]. Replace with a daily fetch
    (e.g. yfinance ^IRX for US, HKMA HIBOR for HK) cached in a small
    `rates_history` table when accuracy matters.
    """
    market = market.lower()
    if market not in DEFAULT_RATES:
        LOG.warning("No default rate for market=%r; using 0.0", market)
        return 0.0
    return DEFAULT_RATES[market]


# ─────────────────────────────────────────────────────────────────
# Core BS internals (vectorized — work on scalars or arrays)
# ─────────────────────────────────────────────────────────────────

def _d1_d2(
    S: np.ndarray, K: np.ndarray, T: np.ndarray,
    r: np.ndarray, q: np.ndarray, sigma: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Compute d1 and d2 from Black-Scholes-Merton.

    Inputs are numpy arrays (scalars are auto-broadcast). Caller is
    responsible for floor-clipping T and sigma before this call.
    """
    sqrtT = np.sqrt(T)
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    return d1, d2


def _bs_price_arr(
    S, K, T, r, q, sigma, is_call: np.ndarray,
) -> np.ndarray:
    """Black-Scholes-Merton price (vectorized). Returns np.array."""
    d1, d2 = _d1_d2(S, K, T, r, q, sigma)
    disc_r = np.exp(-r * T)
    disc_q = np.exp(-q * T)

    call = S * disc_q * norm.cdf(d1) - K * disc_r * norm.cdf(d2)
    put  = K * disc_r * norm.cdf(-d2) - S * disc_q * norm.cdf(-d1)
    return np.where(is_call, call, put)


# ─────────────────────────────────────────────────────────────────
# Public scalar API  (unit-test friendly, one-off REPL use)
# ─────────────────────────────────────────────────────────────────

def bs_price(S, K, T, r, sigma, opt_type, q=DEFAULT_DIV_YIELD):
    """Black-Scholes-Merton fair value for a single contract."""
    if T <= 0 or sigma <= 0:
        return float(max(S - K, 0.0) if opt_type.upper() == "C"
                     else max(K - S, 0.0))
    return bs_greeks(S, K, T, r, sigma, opt_type, q=q)["price"]


def bs_greeks(
    S: float, K: float, T: float,
    r: float, sigma: float, opt_type: str,
    q: float = DEFAULT_DIV_YIELD,
) -> dict[str, float]:
    """
    Compute all five greeks for a single option.

    Returns dict with keys: delta, gamma, theta, vega, rho, price.
    Returns NaN for greeks if T<=0 or sigma<=0 (degenerate); price still
    returns intrinsic value in that case.
    """
    opt_type = opt_type.upper()
    is_call = (opt_type == "C")

    # Degenerate cases — return intrinsic price, NaN greeks
    if not np.isfinite(S) or not np.isfinite(K) or T <= 0 or sigma <= 0:
        intrinsic = max(S - K, 0.0) if is_call else max(K - S, 0.0)
        return {
            "price": float(intrinsic),
            "delta": np.nan, "gamma": np.nan,
            "theta": np.nan, "vega":  np.nan, "rho": np.nan,
        }

    sqrtT = np.sqrt(T)
    d1, d2 = _d1_d2(
        np.float64(S), np.float64(K), np.float64(T),
        np.float64(r), np.float64(q), np.float64(sigma),
    )

    # Cache all CDF/PDF lookups up front — avoids recomputing N(-d1), N(-d2)
    Nd1,  Nd2  = norm.cdf(d1),  norm.cdf(d2)
    Nmd1, Nmd2 = norm.cdf(-d1), norm.cdf(-d2)
    nd1        = norm.pdf(d1)
    disc_r     = np.exp(-r * T)
    disc_q     = np.exp(-q * T)

    # Price, delta, theta, rho — branch on call vs put
    if is_call:
        price = S * disc_q * Nd1 - K * disc_r * Nd2
        delta = disc_q * Nd1
        rho_math   = K * T * disc_r * Nd2
        theta_math = (
            - (S * disc_q * nd1 * sigma) / (2 * sqrtT)
            - r * K * disc_r * Nd2
            + q * S * disc_q * Nd1
        )
    else:
        price = K * disc_r * Nmd2 - S * disc_q * Nmd1
        delta = disc_q * (Nd1 - 1.0)
        rho_math   = -K * T * disc_r * Nmd2
        theta_math = (
            - (S * disc_q * nd1 * sigma) / (2 * sqrtT)
            + r * K * disc_r * Nmd2
            - q * S * disc_q * Nmd1
        )

    # Gamma and vega are the same for calls and puts
    gamma     = (disc_q * nd1) / (S * sigma * sqrtT)
    vega_math = S * disc_q * nd1 * sqrtT

    # Convert to trader conventions
    vega_per_volpt = vega_math  * 0.01      # per 1% IV move
    theta_per_day  = theta_math / DAYS_PER_YEAR
    rho_per_ratept = rho_math   * 0.01      # per 1% rate move

    return {
        "price": float(price),
        "delta": float(delta),
        "gamma": float(gamma),
        "vega":  float(vega_per_volpt),
        "theta": float(theta_per_day),
        "rho":   float(rho_per_ratept),
    }


# ─────────────────────────────────────────────────────────────────
# Public vectorized API — the workhorse for ingest
# ─────────────────────────────────────────────────────────────────

GREEK_COLS = ("delta", "gamma", "theta", "vega", "rho")


def compute_greeks_df(
    df: pd.DataFrame,
    *,
    spot_col:     str = "underlying_price",
    strike_col:   str = "strike",
    dte_col:      str = "dte",
    iv_col:       str = "iv",
    opttype_col:  str = "opt_type",
    r:            float | str = 0.04,
    q:            float | str = 0.0,
    overwrite:    bool = True,
) -> pd.DataFrame:
    """
    Add delta, gamma, theta, vega, rho columns to an option-chain DataFrame.

    Operates on a copy; returns the new DataFrame.

    Parameters
    ----------
    df : DataFrame
        Must contain columns: underlying_price, strike, dte, iv, opt_type.
    r, q : float OR str
        Either a scalar (applied to all rows) or a column name to read
        per-row rates / yields from.
    overwrite : bool
        If False, only fills NaN entries in existing greek columns.

    Returns
    -------
    DataFrame
        Copy of `df` with delta/gamma/theta/vega/rho columns added/updated.
        Rows with invalid inputs (T<=0, iv<=0, NaN spot/strike) get NaN greeks.

    Performance
    -----------
    Pure numpy under the hood. ~1ms per 1000 contracts on a modern CPU.
    """
    out = df.copy()
    n = len(out)
    if n == 0:
        for col in GREEK_COLS:
            if col not in out.columns:
                out[col] = pd.Series(dtype=float)
        return out

    # ── Pull arrays ───────────────────────────────────────────────
    S     = pd.to_numeric(out[spot_col],   errors="coerce").to_numpy(dtype=float)
    K     = pd.to_numeric(out[strike_col], errors="coerce").to_numpy(dtype=float)
    sigma = pd.to_numeric(out[iv_col],     errors="coerce").to_numpy(dtype=float)
    # dte: prefer explicit column; else derive from (expiry - date)
    if dte_col in out.columns and out[dte_col].notna().any():
        dte = pd.to_numeric(out[dte_col], errors="coerce").to_numpy(dtype=float)
    elif {"date", "expiry"}.issubset(out.columns):
        d = pd.to_datetime(out["date"]).dt.date
        e = pd.to_datetime(out["expiry"]).dt.date
        dte = np.array([(ee - dd).days for dd, ee in zip(d, e)], dtype=float)
        out[dte_col] = dte           # populate so writer doesn't recompute
    else:
        raise ValueError(
            "compute_greeks_df needs either a 'dte' column or both "
            "'date' and 'expiry' columns"
        )

    is_call = (
        out[opttype_col].astype(str).str.upper().to_numpy() == "C"
    )

    r_arr = (
        pd.to_numeric(out[r], errors="coerce").to_numpy(dtype=float)
        if isinstance(r, str) else
        np.full(n, float(r))
    )
    q_arr = (
        pd.to_numeric(out[q], errors="coerce").to_numpy(dtype=float)
        if isinstance(q, str) else
        np.full(n, float(q))
    )

    # ── Validity mask ─────────────────────────────────────────────
    T = dte / DAYS_PER_YEAR
    valid = (
        np.isfinite(S) & (S > 0)
        & np.isfinite(K) & (K > 0)
        & np.isfinite(T) & (T > EPS)
        & np.isfinite(sigma) & (sigma > EPS)
        & np.isfinite(r_arr)
        & np.isfinite(q_arr)
    )

    # ── Pre-allocate output arrays with NaN ───────────────────────
    delta = np.full(n, np.nan)
    gamma = np.full(n, np.nan)
    vega  = np.full(n, np.nan)
    theta = np.full(n, np.nan)
    rho   = np.full(n, np.nan)

    if valid.any():
        Sv, Kv, Tv = S[valid], K[valid], T[valid]
        rv, qv, sv = r_arr[valid], q_arr[valid], sigma[valid]
        cv         = is_call[valid]

        sqrtT = np.sqrt(Tv)
        d1, d2 = _d1_d2(Sv, Kv, Tv, rv, qv, sv)
        Nd1, Nd2 = norm.cdf(d1), norm.cdf(d2)
        Nmd1, Nmd2 = norm.cdf(-d1), norm.cdf(-d2)
        nd1 = norm.pdf(d1)
        disc_r = np.exp(-rv * Tv)
        disc_q = np.exp(-qv * Tv)

        # Delta
        d_call = disc_q * Nd1
        d_put  = disc_q * (Nd1 - 1.0)
        d_v    = np.where(cv, d_call, d_put)

        # Gamma (same for call/put)
        g_v = (disc_q * nd1) / (Sv * sv * sqrtT)

        # Vega (math, then to per-volpoint)
        vega_math = Sv * disc_q * nd1 * sqrtT
        v_v       = vega_math * 0.01

        # Theta (math, then per-day)
        common  = -(Sv * disc_q * nd1 * sv) / (2 * sqrtT)
        th_call = common - rv * Kv * disc_r * Nd2  + qv * Sv * disc_q * Nd1
        th_put  = common + rv * Kv * disc_r * Nmd2 - qv * Sv * disc_q * Nmd1
        th_math = np.where(cv, th_call, th_put)
        t_v     = th_math / DAYS_PER_YEAR

        # Rho (math, then per-ratepoint)
        rho_call = Kv * Tv * disc_r * Nd2
        rho_put  = -Kv * Tv * disc_r * Nmd2
        rho_math = np.where(cv, rho_call, rho_put)
        r_v      = rho_math * 0.01

        delta[valid] = d_v
        gamma[valid] = g_v
        vega[valid]  = v_v
        theta[valid] = t_v
        rho[valid]   = r_v

    # ── Assign back, respecting `overwrite` ───────────────────────
    new_vals = {
        "delta": delta, "gamma": gamma,
        "theta": theta, "vega":  vega, "rho":   rho,
    }
    for col, arr in new_vals.items():
        if col in out.columns and not overwrite:
            existing = pd.to_numeric(out[col], errors="coerce").to_numpy(float)
            mask = np.isnan(existing)
            existing[mask] = arr[mask]
            out[col] = existing
        else:
            out[col] = arr

    return out


# ─────────────────────────────────────────────────────────────────
# Self-test  (python common/greeks.py)
# ─────────────────────────────────────────────────────────────────

def _self_test():
    """Sanity-check against textbook values + benchmark vectorized speed."""
    import time

    # Hull, Options Futures and Other Derivatives, Ex 15.6
    # S=42, K=40, r=0.10, sigma=0.20, T=0.5, q=0
    # Expected call price ≈ 4.76, put price ≈ 0.81
    g_call = bs_greeks(S=42, K=40, T=0.5, r=0.10, sigma=0.20, opt_type="C")
    g_put  = bs_greeks(S=42, K=40, T=0.5, r=0.10, sigma=0.20, opt_type="P")

    print(f"Hull Ex 15.6:")
    print(f"  Call: price={g_call['price']:.4f}  (expected ~4.7594)")
    print(f"  Put:  price={g_put['price']:.4f}   (expected ~0.8086)")
    print(f"  Call delta={g_call['delta']:.4f}, gamma={g_call['gamma']:.4f}")
    print(f"  Call vega(/1%)={g_call['vega']:.4f}, "
          f"theta(/day)={g_call['theta']:.4f}, rho(/1%)={g_call['rho']:.4f}")

    # Put-call parity sanity:
    # C - P = S*e^(-qT) - K*e^(-rT)
    parity_lhs = g_call["price"] - g_put["price"]
    parity_rhs = 42 * np.exp(0) - 40 * np.exp(-0.10 * 0.5)
    print(f"  Put-call parity: LHS={parity_lhs:.4f}, RHS={parity_rhs:.4f} "
          f"(diff={abs(parity_lhs-parity_rhs):.2e})")

    # Vectorized benchmark
    rng = np.random.default_rng(42)
    n = 5000
    df = pd.DataFrame({
        "underlying_price": rng.uniform(50, 500, n),
        "strike":           rng.uniform(50, 500, n),
        "dte":              rng.integers(1, 365, n),
        "iv":               rng.uniform(0.10, 0.80, n),
        "opt_type":         rng.choice(["C", "P"], n),
    })

    t0 = time.perf_counter()
    out = compute_greeks_df(df, r=0.04, q=0.015)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"\nVectorized: {n} contracts in {elapsed_ms:.2f} ms "
          f"({n/elapsed_ms*1000:,.0f} contracts/sec)")
    print(f"  delta: min={out['delta'].min():.3f}, "
          f"max={out['delta'].max():.3f}, "
          f"NaN={out['delta'].isna().sum()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    _self_test()