# run_once_identify_unknowns.py
from common.sector_map import get_sector_or_class
from common.universe import get_india_only  # adjust to however you get the ticker list

unknowns = []
for t in get_india_only():
    s = get_sector_or_class(t)
    if s in (None, "Unknown", "Other", ""):
        unknowns.append(t)

print(f"\n{len(unknowns)} unmapped tickers:")
for t in sorted(unknowns):
    print(f"  {t}")