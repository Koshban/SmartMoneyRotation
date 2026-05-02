""" phase2/common/market_config_v2.py"""
from __future__ import annotations

from common.universe import (
    ETF_UNIVERSE,
    HK_UNIVERSE,
    INDIA_UNIVERSE,
    get_all_single_names,
    get_hk_only,
    get_india_only,
    is_hk_ticker,
    is_india_ticker,
)


def _get_us_equities() -> list[str]:
    return sorted(s for s in get_all_single_names() if not is_hk_ticker(s) and not is_india_ticker(s))


def _get_us_tradable() -> list[str]:
    return sorted(set(list(ETF_UNIVERSE) + _get_us_equities()))


def _get_hk_tradable() -> list[str]:
    return sorted(set(get_hk_only()))


def _get_india_tradable() -> list[str]:
    return sorted(set(get_india_only()))


MARKET_CONFIG_V2 = {
    "US": {
        "market": "US",
        "benchmark": "SPY",
        "leadership_universe_fn": lambda: list(ETF_UNIVERSE),
        "tradable_universe_fn": _get_us_tradable,
        "enable_sector_rotation": True,
        "enable_breadth": True,
        "enable_vol_regime": True,
        "max_positions": 12,
        "max_sector_weight": 0.30,
        "max_theme_names": 3,
    },
    "HK": {
        "market": "HK",
        "benchmark": "2800.HK",
        "leadership_universe_fn": lambda: [s for s in HK_UNIVERSE if s.endswith('.HK')][: min(8, len(HK_UNIVERSE))],
        "tradable_universe_fn": _get_hk_tradable,
        "enable_sector_rotation": False,
        "enable_breadth": False,
        "enable_vol_regime": True,
        "max_positions": 10,
        "max_sector_weight": 0.35,
        "max_theme_names": 3,
    },
    "IN": {
        "market": "IN",
        "benchmark": "NIFTYBEES.NS",
        "leadership_universe_fn": lambda: [s for s in INDIA_UNIVERSE if s.endswith('.NS')][: min(10, len(INDIA_UNIVERSE))],
        "tradable_universe_fn": _get_india_tradable,
        "enable_sector_rotation": False,
        "enable_breadth": False,
        "enable_vol_regime": True,
        "max_positions": 12,
        "max_sector_weight": 0.30,
        "max_theme_names": 3,
    },
}


def get_market_config_v2(market: str) -> dict:
    m = market.upper()
    if m not in MARKET_CONFIG_V2:
        raise ValueError(f"Unknown market {market!r}")
    cfg = dict(MARKET_CONFIG_V2[m])
    cfg["leadership_universe"] = list(cfg["leadership_universe_fn"]())
    cfg["tradable_universe"] = list(cfg["tradable_universe_fn"]())
    return cfg