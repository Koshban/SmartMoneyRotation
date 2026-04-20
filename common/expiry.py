"""
common/expiry.py
Monthly option expiry date utilities."""

from datetime import date, timedelta
from typing import List, Tuple


def third_friday(year: int, month: int) -> date:
    """3rd Friday of month (US / HK monthly expiry)."""
    first = date(year, month, 1)
    first_fri = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_fri + timedelta(days=14)


def last_thursday(year: int, month: int) -> date:
    """Last Thursday of month (India NSE monthly expiry)."""
    nxt = date(year + (month // 12), (month % 12) + 1, 1)
    last_day = nxt - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() - 3) % 7)


def next_monthly_expiries(
    ref_date: date | None = None,
    market: str = "us",
    n: int = 2,
) -> List[date]:
    """Return next *n* monthly expiry dates after ref_date."""
    if ref_date is None:
        ref_date = date.today()

    calc = last_thursday if market == "india" else third_friday
    expiries: List[date] = []
    y, m = ref_date.year, ref_date.month

    for _ in range(n + 4):          # generous lookahead
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
) -> List[Tuple[date, str]]:
    """
    Match calculated target expiry dates to the closest available
    expiry strings from yfinance.  Rejects matches > 7 days away.
    """
    avail_dates = sorted(date.fromisoformat(s) for s in available)
    matched: List[Tuple[date, str]] = []

    for target in targets:
        best = min(avail_dates, key=lambda d: abs((d - target).days))
        if abs((best - target).days) <= 7:
            matched.append((best, best.isoformat()))

    return matched