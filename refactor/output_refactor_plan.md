# V2 Refactor Plan

## Goal
Run the strategy across all tradable names for a selected market (US, HK, IN), while using ETFs and proxies differently inside the strategy to produce final tradeable names.

## Refactor
- Separate `leadership_universe` from `tradable_universe` in `market_config_v2.py`
- For US, leadership = ETFs; tradable = ETFs + US single names
- For HK and IN, tradable = all market names from the relevant market helpers
- In `runner_v2.py`, split incoming frames into leadership and tradable layers
- In `pipeline_v2.py`, build a leadership snapshot and inject `leadership_strength` into tradable scoring
- Keep final output as ranked tradeable names with weights
- Update `report_v2.py` to show tradable vs leadership sizes and instrument type

## Expected behavior
A market run should evaluate all tradeable names in that market, use the leadership layer to bias ranking, and return concrete names to buy or sell.
