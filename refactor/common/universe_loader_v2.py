from __future__ import annotations

def get_universe_for_market(market: str):
    from common.universe import get_universe_for_market as gufm
    return gufm(market)
