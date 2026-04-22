"""
common/expiry.py
Monthly option expiry date utilities.

Markets supported:
  US    — 3rd Friday of expiry month
  HK    — Penultimate business day of expiry month (approx)
  India — Last Thursday of expiry month
"""

from datetime import date, timedelta
from typing import List, Tuple


def third_friday(year: int, month: int) -> date:
    """3rd Friday of month (US monthly expiry)."""
    first = date(year, month, 1)
    first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_fri + timedelta(days=14)


def last_thursday(year: int, month: int) -> date:
    """Last Thursday of month (India NSE monthly expiry)."""
    nxt = date(year + (month // 12), (month % 12) + 1, 1)
    last_day = nxt - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 3) % 7)


def hk_option_expiry(year: int, month: int) -> date:
    """
    Approximate HKEX stock option expiry: the business day
    immediately preceding the last business day of the expiry month.

    HKEX rule: Last trading day is the business day immediately
    before the last business day of the expiry month.  This is
    effectively the second-to-last business day.

    This is an approximation (ignores HKEX holidays).  For
    exact matching, use IBKR's available expiry list directly
    via ``select_expiries_from_chain()``.
    """
    # Last calendar day of the month
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)

    # Walk back to the last business day (skip weekends)
    while last_day.weekday() >= 5:  # Sat=5, Sun=6
        last_day -= timedelta(days=1)

    # Penultimate business day (one more step back, skip weekends)
    prev_bday = last_day - timedelta(days=1)
    while prev_bday.weekday() >= 5:
        prev_bday -= timedelta(days=1)

    return prev_bday


def next_monthly_expiries(
    ref_date: date | None = None,
    market: str = "us",
    n: int = 2,
) -> List[date]:
    """
    Return next *n* monthly expiry dates after ref_date.

    Supported markets: "us" (3rd Friday), "hk" (penultimate
    business day), "india" (last Thursday).
    """
    if ref_date is None:
        ref_date = date.today()

    market_lower = market.lower()

    if market_lower in ("hk", "hongkong", "sehk"):
        calc = hk_option_expiry
    elif market_lower in ("india", "in", "nse"):
        calc = last_thursday
    else:
        calc = third_friday

    expiries: List[date] = []
    y, m = ref_date.year, ref_date.month

    for _ in range(n + 6):  # generous lookahead
        exp = calc(y, m)
        if exp > ref_date:
            expiries.append(exp)
            if len(expiries) == n:
                break
        m += 1
        if m > 12:
            m, y = 1, y + 1

    return expiries


def match_expiry(
    targets: List[date],
    available: tuple | list,
    max_gap_days: int = 7,
) -> List[Tuple[date, str]]:
    """
    Match calculated target expiry dates to the closest available
    expiry strings.  Rejects matches > max_gap_days away.

    Parameters
    ----------
    targets : list[date]
        Pre-calculated target expiry dates.
    available : list/tuple of str
        ISO-format date strings from the exchange/broker.
    max_gap_days : int
        Maximum calendar-day gap for a valid match (default 7).

    Returns
    -------
    list[(date, str)]
        Matched (expiry_date, ISO string) pairs.
    """
    if not available:
        return []

    avail_dates = sorted(date.fromisoformat(s) for s in available)
    matched: List[Tuple[date, str]] = []

    for target in targets:
        if not avail_dates:
            break
        best = min(avail_dates, key=lambda d: abs((d - target).days))
        if abs((best - target).days) <= max_gap_days:
            matched.append((best, best.isoformat()))

    return matched


def select_expiries_from_chain(
    available_expiries: list[str],
    n: int = 2,
    ref_date: date | None = None,
) -> List[Tuple[date, str]]:
    """
    Pick the next N distinct-month expiries directly from an
    exchange/broker's available expiry list.

    This is the **robust** approach for any market: instead of
    pre-calculating target dates (which requires knowing the
    exact expiry convention and holiday calendar), it picks
    directly from what's actually tradeable.

    Works for US (3rd Friday), HK (penultimate business day),
    and any other market regardless of convention.

    Parameters
    ----------
    available_expiries : list[str]
        Expiry strings in YYYYMMDD or YYYY-MM-DD format.
    n : int
        Number of monthly expiries to return.
    ref_date : date
        Reference date (default: today).

    Returns
    -------
    list[(date, str)]
        List of (expiry_date, ISO string) pairs, one per month.

    Example
    -------
    >>> exps = ["20260529", "20260626", "20260731", "20260828"]
    >>> select_expiries_from_chain(exps, n=2)
    [(date(2026, 5, 29), '2026-05-29'), (date(2026, 6, 26), '2026-06-26')]
    """
    if ref_date is None:
        ref_date = date.today()

    # Parse all expiry strings to dates
    parsed: list[tuple[date, str]] = []
    for exp_str in available_expiries:
        # Handle both YYYYMMDD and YYYY-MM-DD
        clean = exp_str.strip()
        if len(clean) == 8 and clean.isdigit():
            iso = f"{clean[:4]}-{clean[4:6]}-{clean[6:]}"
        elif len(clean) == 10 and "-" in clean:
            iso = clean
        else:
            continue

        try:
            exp_date = date.fromisoformat(iso)
            if exp_date > ref_date:
                parsed.append((exp_date, iso))
        except ValueError:
            continue

    # Sort by date ascending
    parsed.sort(key=lambda x: x[0])

    # Pick the earliest expiry in each distinct month
    selected: list[tuple[date, str]] = []
    seen_months: set[tuple[int, int]] = set()

    for exp_date, iso_str in parsed:
        month_key = (exp_date.year, exp_date.month)
        if month_key not in seen_months:
            seen_months.add(month_key)
            selected.append((exp_date, iso_str))
            if len(selected) >= n:
                break

    return selected