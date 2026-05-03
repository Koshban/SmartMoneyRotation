import yfinance as yf
# df = yf.download(
#     ["0001.HK", "0700.HK", "0005.HK"],
#     start="2023-08-17", end="2026-05-03",
#     auto_adjust=True, progress=False, group_by="ticker", threads=True,
# )
# print(df.shape)
# print(df.columns[:10].tolist())
# print(df["0700.HK"].tail(3) if "0700.HK" in df.columns.get_level_values(0) else "0700.HK NOT in columns")

# ingest/test_ingest_build2.py
import yfinance as yf
from ingest.universe import get_symbols  # or however you load HK symbols

syms = get_symbols("hk")[:5]
print("requested:", syms)

data = yf.download(
    syms, start="2023-08-17", end="2026-05-03",
    auto_adjust=True, progress=False, group_by="ticker", threads=True,
)
print("returned top-level keys:", data.columns.get_level_values(0).unique().tolist())

for sym in syms:
    try:
        sub = data[sym].dropna(how="all")
        print(f"  {sym}: {len(sub)} rows, last close={sub['Close'].iloc[-1] if len(sub) else 'N/A'}")
    except KeyError:
        print(f"  {sym}: KEYERROR — not in returned data")