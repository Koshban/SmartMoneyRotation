"""
End-to-end test: full pipeline from download → portfolio with
breadth integration.
"""

import warnings
import yfinance as yf
import pandas as pd
import numpy as np
from utils.run_logger import RunLogger

from compute.indicators import compute_all_indicators
from compute.scoring import compute_composite_score
from compute.relative_strength import compute_all_rs
from compute.sector_rs import (
    compute_all_sector_rs,
    merge_sector_context,
)
from compute.breadth import compute_all_breadth, breadth_report
from strategy_phase1.signals import generate_signals
from strategy_phase1.portfolio import (
    build_portfolio,
    portfolio_report,
)
from common.config import TICKER_SECTOR_MAP, SECTOR_ETF_MAP

warnings.filterwarnings("ignore", category=FutureWarning)


# ═══════════════════════════════════════════════════════════════
#  UNIVERSE DEFINITION
# ═══════════════════════════════════════════════════════════════

TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
    "META", "JPM", "GS", "XOM", "CVX",
    "JNJ", "UNH", "PG", "HD", "LIN",
]

SECTOR_ETF_LIST = list(set(SECTOR_ETF_MAP.values()))
BENCHMARK = "SPY"


# ═══════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════

def extract_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Pull a single ticker from a multi-ticker yfinance download."""
    if not isinstance(data.columns, pd.MultiIndex):
        df = data.copy()
    else:
        lvl1 = data.columns.get_level_values(1)
        if ticker in lvl1.unique():
            mask = lvl1 == ticker
            df = data.loc[:, mask].copy()
            df.columns = df.columns.get_level_values(0)
        else:
            try:
                df = data[ticker].copy()
            except KeyError:
                return pd.DataFrame()

    df.columns = [str(c).lower().replace(" ", "_") for c in df.columns]
    if "adj_close" in df.columns:
        if "close" in df.columns:
            df = df.drop(columns=["adj_close"])
        else:
            df = df.rename(columns={"adj_close": "close"})
    df = df.dropna(subset=["close"])
    return df


def safe_last(series, default="?"):
    """Safely get last value of a Series."""
    if series is None or (hasattr(series, 'empty') and series.empty):
        return default
    try:
        val = series.iloc[-1]
        return default if pd.isna(val) else val
    except (IndexError, KeyError):
        return default


# ═══════════════════════════════════════════════════════════════
#  MAIN TEST
# ═══════════════════════════════════════════════════════════════


def main():
    from utils.run_logger import RunLogger

    log = RunLogger("test_portfolio")

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Download price data
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 1: Fetching price data for all tickers + SPY")

    all_symbols = list(set(
        TICKERS + SECTOR_ETF_LIST + [BENCHMARK]
    ))

    raw = yf.download(all_symbols, period="2y", progress=True)

    # ── Extract benchmark ─────────────────────────────────────
    spy = extract_ticker(raw, BENCHMARK)
    log.kv("Benchmark (SPY)", f"{len(spy)} rows")

    # ── Extract stock data ────────────────────────────────────
    stock_data: dict[str, pd.DataFrame] = {}
    for t in TICKERS:
        df = extract_ticker(raw, t)
        if not df.empty and len(df) > 100:
            stock_data[t] = df

    log.kv("Stocks loaded", f"{len(stock_data)} / {len(TICKERS)}")

    # ── Extract sector ETF data (keyed by sector NAME) ────────
    etf_to_sector = {v: k for k, v in SECTOR_ETF_MAP.items()}
    sector_etf_data: dict[str, pd.DataFrame] = {}
    for etf in SECTOR_ETF_LIST:
        df = extract_ticker(raw, etf)
        if not df.empty and len(df) > 100:
            sector_name = etf_to_sector.get(etf, etf)
            sector_etf_data[sector_name] = df

    log.kv("Sector ETFs loaded", f"{len(sector_etf_data)}")

    missing_stocks = set(TICKERS) - set(stock_data.keys())
    if missing_stocks:
        for ms in sorted(missing_stocks):
            log.warning(f"Missing or too short: {ms}")
    else:
        log.success("All tickers loaded successfully")

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Compute market breadth
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 2: Computing market breadth from stock universe")

    breadth = compute_all_breadth(stock_data)
    if not breadth.empty:
        last_b = breadth.iloc[-1]
        log.breadth_summary({
            "regime":       last_b.get("breadth_regime", "?"),
            "score":        last_b.get("breadth_score", 0),
            "score_smooth": last_b.get("breadth_score_smooth", 0),
            "ad_line":      int(last_b.get("ad_line", 0)),
            "pct_above_50": last_b.get("pct_above_50", 0),
            "pct_above_200": last_b.get("pct_above_200", 0),
            "thrust_active": last_b.get("thrust_active", 0),
        })
    else:
        log.warning("No breadth data (universe too small?)")

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Compute sector relative strength
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 3: Computing sector relative strength")

    sector_rs = compute_all_sector_rs(sector_etf_data, spy)
    log.kv("Sector RS shape", f"{sector_rs.shape}")

    if not sector_rs.empty:
        latest_date = sector_rs.index.get_level_values("date").max()
        latest = sector_rs.xs(latest_date, level="date")
        latest = latest.sort_values(
            "sect_rs_pctrank", ascending=False
        )

        log.h2(f"Sector rankings ({latest_date.strftime('%Y-%m-%d')})")

        sector_rows = []
        for sec_name, row in latest.iterrows():
            regime = row.get("sect_rs_regime", "?")
            pctrank = row.get("sect_rs_pctrank", 0)
            sector_rows.append({
                "sector":  str(sec_name),
                "pctrank": pctrank,
                "regime":  regime,
            })
        log.sector_rankings(sector_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Process each stock through full pipeline
    # ══════════════════════════════════════════════════════════
    log.h1(f"STEP 4: Processing {len(stock_data)} stocks through full pipeline (with breadth)")

    signaled_universe: dict[str, pd.DataFrame] = {}
    step4_rows = []

    for ticker, df in stock_data.items():
        try:
            # Indicators
            enriched = compute_all_indicators(df)

            # Relative strength vs SPY
            rs = compute_all_rs(enriched, spy)

            # Scoring
            scored = compute_composite_score(rs)

            # Merge sector context
            sector = TICKER_SECTOR_MAP.get(ticker, "Unknown")
            merged = merge_sector_context(
                scored, sector_rs, sector
            )
            merged["sector_name"] = sector

            # Signals — now with breadth!
            signaled = generate_signals(merged, breadth=breadth)
            signaled_universe[ticker] = signaled

            last = signaled.iloc[-1]
            sig = last.get("sig_confirmed", 0)
            score = last.get("score_adjusted",
                             last.get("score_composite", 0))
            regime = last.get("rs_regime", "?")
            sector_regime = last.get("sect_rs_regime", "?")
            b_ok = last.get("sig_breadth_ok", True)
            reason = last.get("sig_reason", "?")

            status = "LONG" if sig == 1 else "flat"
            b_flag = " [breadth-blocked]" if not b_ok else ""

            # Color-code the status for rich output
            if sig == 1:
                status_styled = "[bold green]LONG [/]"
            else:
                status_styled = "[dim]flat [/]"

            regime_badge = log.regime_badge(str(regime))
            sect_badge = log.regime_badge(str(sector_regime))

            log.print(
                f"  {ticker:<6} {status_styled} "
                f"score={score:.3f}  "
                f"regime={regime_badge}  "
                f"sector={sect_badge}  "
                f"reason=[dim]{reason}[/]"
                f"{'[bold red] ⛔ breadth-blocked[/]' if not b_ok else ''}"
            )

            step4_rows.append({
                "ticker": ticker,
                "status": status,
                "score": score,
                "regime": str(regime),
                "sector_regime": str(sector_regime),
                "reason": reason,
                "breadth_ok": b_ok,
            })

        except Exception as e:
            import traceback
            log.error(f"{ticker:6s} — {e}", exc_info=True)

    log.divider()
    log.kv("Processed", f"{len(signaled_universe)} / {len(stock_data)}")
    long_count = sum(1 for r in step4_rows if r["status"] == "LONG")
    flat_count = sum(1 for r in step4_rows if r["status"] == "flat")
    blocked_count = sum(1 for r in step4_rows if not r["breadth_ok"])
    log.kv("LONG signals", f"[bold green]{long_count}[/]")
    log.kv("Flat", f"[dim]{flat_count}[/]")
    if blocked_count:
        log.kv("Breadth-blocked", f"[bold red]{blocked_count}[/]")

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Build portfolio WITH breadth
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 5: Building target portfolio (with breadth)")

    result = build_portfolio(
        signaled_universe,
        breadth=breadth,
    )

    meta_b = result.get("metadata", {})

    log.kv("Breadth regime", log.regime_badge(meta_b.get("breadth_regime", "?")))
    log.kv("Exposure scale", f"{meta_b.get('breadth_exposure', 1.0):.0%}")
    log.kv("Universe", f"{meta_b.get('universe_size', 0)} stocks")
    log.kv("Candidates", f"{meta_b.get('num_candidates', 0)} passed all gates")
    log.kv("Holdings", f"{meta_b.get('num_holdings', 0)} positions")
    log.kv("Invested", f"{meta_b.get('total_invested', 0):.1%}")
    log.kv("Cash", f"{meta_b.get('cash_pct', 1):.1%}")

    holdings_list = result.get("holdings")
    if holdings_list is not None and len(holdings_list) > 0:
        # Handle both DataFrame and list-of-dicts
        if isinstance(holdings_list, pd.DataFrame):
            h_rows = holdings_list.to_dict("records")
        else:
            h_rows = holdings_list

        log.portfolio_table([
            {
                "ticker": h.get("ticker", "?"),
                "weight": h.get("weight", 0),
                "sector": h.get("sector", h.get("sector_name", "")),
                "score":  h.get("score_adjusted", h.get("score_composite", 0)),
                "signal": "BUY" if h.get("sig_confirmed", 0) == 1 else "",
            }
            for h in h_rows
        ])

    # Also print the text report for completeness
    log.h2("Portfolio report (text)")
    log.print(portfolio_report(result))

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Build portfolio WITHOUT breadth (comparison)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 6: Building portfolio WITHOUT breadth (comparison)")

    result_no_breadth = build_portfolio(
        signaled_universe,
        breadth=None,
    )

    meta_nb = result_no_breadth.get("metadata", {})

    # Side-by-side comparison table
    log.table(
        title="Breadth Impact Comparison",
        columns=[
            {"header": "Metric", "style": "bold"},
            {"header": "WITH Breadth", "justify": "right"},
            {"header": "WITHOUT Breadth", "justify": "right"},
        ],
        rows=[
            [
                "Regime",
                meta_b.get("breadth_regime", "?"),
                meta_nb.get("breadth_regime", "?"),
            ],
            [
                "Exposure scale",
                f"{meta_b.get('breadth_exposure', 1.0):.0%}",
                f"{meta_nb.get('breadth_exposure', 1.0):.0%}",
            ],
            [
                "Holdings",
                str(meta_b.get("num_holdings", 0)),
                str(meta_nb.get("num_holdings", 0)),
            ],
            [
                "Invested",
                f"{meta_b.get('total_invested', 0):.1%}",
                f"{meta_nb.get('total_invested', 0):.1%}",
            ],
            [
                "Cash",
                f"{meta_b.get('cash_pct', 1):.1%}",
                f"{meta_nb.get('cash_pct', 1):.1%}",
            ],
        ],
    )

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Rebalance from hypothetical current portfolio
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 7: Rebalance from hypothetical current portfolio")

    current = {
        "AAPL": 0.15, "MSFT": 0.15, "NVDA": 0.15,
        "GOOGL": 0.15, "AMZN": 0.15,
    }

    log.h2("Current holdings")
    for t, w in current.items():
        log.kv(t, f"{w:.0%}")

    result_rebal = build_portfolio(
        signaled_universe,
        current_holdings=current,
        breadth=breadth,
    )

    trades = result_rebal.get("trades")
    if trades is not None and not trades.empty:
        rebal_actions = []
        for _, row in trades.iterrows():
            rebal_actions.append({
                "ticker":         str(row["ticker"]),
                "current_weight": row["current_weight"],
                "target_weight":  row["target_weight"],
                "delta":          row["delta"],
                "action":         row["action"],
            })
        log.rebalance_table(rebal_actions)
    else:
        log.warning("No trades generated.")

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Breadth report
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 8: Full breadth report")
    log.print(breadth_report(breadth, lookback=5))

    # ══════════════════════════════════════════════════════════
    #  STEP 9 — Edge cases
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 9: Edge cases")

    # 9a — Zero candidates with existing holdings → full sell
    log.h2("9a: Zero candidates with holdings → full sell")
    empty_universe: dict[str, pd.DataFrame] = {}
    result_empty = build_portfolio(
        empty_universe,
        current_holdings=current,
        breadth=breadth,
    )
    t_e = result_empty.get("trades")
    if t_e is not None and not t_e.empty:
        actions = t_e["action"].value_counts()
        action_str = ", ".join(
            f"{act}: [bold]{cnt}[/]" for act, cnt in actions.items()
        )
        log.success(f"Trade actions generated: {action_str}")
    else:
        log.warning("No trades (expected SELL for all)")

    # 9b — No breadth data → graceful fallback
    log.h2("9b: No breadth data → graceful fallback")
    result_nb2 = build_portfolio(
        signaled_universe,
        breadth=None,
    )
    meta_nb2 = result_nb2.get("metadata", {})
    log.kv("Regime", meta_nb2.get("breadth_regime", "?"))
    log.kv("Holdings", meta_nb2.get("num_holdings", 0))
    log.kv("Invested", f"{meta_nb2.get('total_invested', 0):.1%}")
    log.success("Graceful fallback OK")

    # 9c — Empty breadth DataFrame
    log.h2("9c: Empty breadth DataFrame → graceful fallback")
    result_eb = build_portfolio(
        signaled_universe,
        breadth=pd.DataFrame(),
    )
    meta_eb = result_eb.get("metadata", {})
    log.kv("Regime", meta_eb.get("breadth_regime", "?"))
    log.kv("Holdings", meta_eb.get("num_holdings", 0))
    log.success("Empty DataFrame fallback OK")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY
    # ══════════════════════════════════════════════════════════
    log.h1("SUMMARY")

    log.print(
        "[bold bright_cyan]Pipeline:[/] download → indicators → RS → scoring "
        "→ sector RS → breadth → signals → portfolio"
    )
    log.kv("Universe", f"{len(stock_data)} stocks")
    log.kv("Candidates", f"{meta_b.get('num_candidates', 0)} passed all gates")
    log.kv("Holdings", f"[bold]{meta_b.get('num_holdings', 0)}[/]")
    log.kv("Invested", f"[bold]{meta_b.get('total_invested', 0):.1%}[/]")
    log.kv("Cash", f"{meta_b.get('cash_pct', 1):.1%}")
    log.kv("Breadth regime", log.regime_badge(meta_b.get("breadth_regime", "?")))

    log.print()
    log.success("[bold green]ALL STEPS COMPLETE[/]")

    # ── Save the HTML log ─────────────────────────────────────
    log_path = log.save()
    print(f"\n📄 HTML log saved: {log_path}")


if __name__ == "__main__":
    main()
    import webbrowser
    webbrowser.open(log_path)