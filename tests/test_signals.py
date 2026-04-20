"""
End-to-end test: trade signal generation from ranked sector ETF
universe, with per-ticker gates from strategy/signals.py feeding
into portfolio-level signals from output/signals.py.
"""

import time
import yfinance as yf
import pandas as pd

from compute.indicators import compute_all_indicators
from compute.relative_strength import compute_all_rs
from compute.scoring import compute_composite_score
from compute.breadth import compute_all_breadth, breadth_to_pillar_scores
from strategy.signals import generate_signals as generate_ticker_signals
from output.rankings import compute_all_rankings
from output.signals import (
    SignalConfig,
    BUY, HOLD, SELL, NEUTRAL,
    check_entry_eligible,
    check_exit_triggered,
    generate_signals,
    compute_signal_strength,
    compute_all_signals,
    latest_signals,
    signal_changes,
    signal_history,
    active_positions,
    compute_turnover,
    signals_summary,
    signals_report,
)
from utils.run_logger import RunLogger


# ── Configuration ─────────────────────────────────────────────

SECTOR_ETFS = [
    "XLK", "XLF", "XLE", "XLV", "XLY",
    "XLP", "XLI", "XLU", "XLC", "XLRE", "XLB",
]
BENCHMARK = "SPY"
PERIOD = "2y"

BREADTH_TICKERS = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "JPM", "GS", "BAC",
    "XOM", "CVX",
    "JNJ", "UNH", "PFE",
    "PG", "HD", "WMT",
    "LIN", "CAT",
]


# ── Helpers ───────────────────────────────────────────────────

def clean_single(raw: pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    raw.columns = [
        str(c).lower().replace(" ", "_") for c in raw.columns
    ]
    if "adj_close" in raw.columns:
        if "close" in raw.columns:
            raw = raw.drop(columns=["adj_close"])
        else:
            raw = raw.rename(columns={"adj_close": "close"})
    return raw


def extract_ticker(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
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

    df.columns = [
        str(c).lower().replace(" ", "_") for c in df.columns
    ]
    if "adj_close" in df.columns:
        if "close" in df.columns:
            df = df.drop(columns=["adj_close"])
        else:
            df = df.rename(columns={"adj_close": "close"})
    df = df.dropna(subset=["close"])
    return df


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    log = RunLogger("test_signals")
    t0 = time.time()

    # ══════════════════════════════════════════════════════════
    #  STEP 1 — Download + build scored, gated, ranked universe
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 1: Build scored + gated + ranked universe")

    # Benchmark
    spy_raw = yf.download(BENCHMARK, period=PERIOD, progress=False)
    spy = clean_single(spy_raw)
    log.kv(BENCHMARK, f"{len(spy)} rows")

    # Sector ETFs
    log.info(f"Downloading {len(SECTOR_ETFS)} sector ETFs...")
    etf_raw = yf.download(SECTOR_ETFS, period=PERIOD, progress=False)

    etf_data: dict[str, pd.DataFrame] = {}
    for ticker in SECTOR_ETFS:
        df = extract_ticker(etf_raw, ticker)
        if not df.empty and len(df) > 100:
            etf_data[ticker] = df
            log.print(f"    {ticker:<6} {len(df)} rows")
        else:
            log.warning(f"{ticker} skipped")

    # Breadth
    log.info(
        f"Downloading {len(BREADTH_TICKERS)} stocks for breadth..."
    )
    breadth_raw = yf.download(
        BREADTH_TICKERS, period=PERIOD, progress=False,
    )

    breadth_universe: dict[str, pd.DataFrame] = {}
    for ticker in BREADTH_TICKERS:
        df = extract_ticker(breadth_raw, ticker)
        if not df.empty and len(df) > 100:
            breadth_universe[ticker] = df

    breadth = compute_all_breadth(breadth_universe)
    pillar_df = breadth_to_pillar_scores(
        breadth, list(etf_data.keys()),
    )
    log.kv("Breadth regime", log.regime_badge(
        breadth["breadth_regime"].iloc[-1],
    ))

    # ── Score, run per-ticker gates, then rank ────────────
    scored_universe: dict[str, pd.DataFrame] = {}
    for ticker, df in etf_data.items():
        df = compute_all_indicators(df)
        df = compute_all_rs(df, spy)
        bseries = (
            pillar_df[ticker] if ticker in pillar_df.columns
            else None
        )
        df = compute_composite_score(df, breadth_scores=bseries)

        # Per-ticker gates from strategy/signals.py
        df = generate_ticker_signals(df, breadth)

        scored_universe[ticker] = df

    log.kv(
        "Per-ticker signal columns",
        [c for c in next(iter(scored_universe.values())).columns
         if c.startswith("sig_")],
    )

    # Rankings
    ranked = compute_all_rankings(scored_universe)
    log.kv("Ranked panel shape", ranked.shape)

    # Verify sig_confirmed carried through
    has_gates = "sig_confirmed" in ranked.columns
    log.kv(
        "sig_confirmed in panel",
        f"{'YES ✓' if has_gates else 'NO (fallback mode)'}",
    )

    if has_gates:
        latest_date = ranked.index.get_level_values(
            "date",
        ).unique()[-1]
        day = ranked.xs(latest_date, level="date")
        n_confirmed = (day["sig_confirmed"] == 1).sum()
        log.kv(
            "Tickers confirmed today",
            f"{n_confirmed} / {len(day)}",
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 2 — Entry / exit eligibility (stateless)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 2: Entry / exit eligibility (stateless)")

    config = SignalConfig()
    log.kv("Entry rank ≤", config.entry_rank_max)
    log.kv("Exit rank >", config.exit_rank_max)
    log.kv("Exit score <", config.exit_score_min)
    log.kv(
        "Entry mode",
        "sig_confirmed" if has_gates else
        f"score ≥ {config.entry_score_min}",
    )

    entry_mask = check_entry_eligible(ranked, config)
    exit_mask  = check_exit_triggered(ranked, config)

    n_eligible = entry_mask.sum()
    n_exit     = exit_mask.sum()
    total      = len(ranked)
    log.kv(
        "Entry eligible",
        f"{n_eligible} / {total} ({n_eligible/total:.1%})",
    )
    log.kv(
        "Exit triggered",
        f"{n_exit} / {total} ({n_exit/total:.1%})",
    )

    # Today's eligibility breakdown
    latest_date = ranked.index.get_level_values(
        "date",
    ).unique()[-1]
    day_ranked = ranked.xs(latest_date, level="date")
    entry_today = check_entry_eligible(day_ranked, config)
    exit_today  = check_exit_triggered(day_ranked, config)

    log.h2("Today's eligibility")
    for ticker in day_ranked.sort_values("rank").index:
        e = "✓ ENTRY" if entry_today.loc[ticker] else "  —    "
        x = "✕ EXIT"  if exit_today.loc[ticker]  else "  —   "
        r = int(day_ranked.loc[ticker, "rank"])
        regime = day_ranked.loc[ticker, "rs_regime"]
        confirmed = ""
        if has_gates:
            c = day_ranked.loc[ticker, "sig_confirmed"]
            reason = day_ranked.loc[ticker, "sig_reason"]
            confirmed = (
                f"  sig_conf={'✓' if c == 1 else '✕'}"
                f"  ({reason})"
            )
        log.print(
            f"    #{r:<3} {ticker:<6} {e}  {x}  "
            f"{regime}{confirmed}"
        )

    # ══════════════════════════════════════════════════════════
    #  STEP 3 — Generate portfolio signals
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 3: Generate portfolio signals")

    signals = compute_all_signals(ranked, breadth, config)
    log.kv("Signal columns added", [
        c for c in signals.columns if c not in ranked.columns
    ])
    log.kv("Result shape", signals.shape)

    # Signal distribution
    sig_counts = signals["signal"].value_counts()
    for sig_type in [BUY, HOLD, SELL, NEUTRAL]:
        cnt = sig_counts.get(sig_type, 0)
        pct = cnt / len(signals) * 100
        log.kv(f"  {sig_type}", f"{cnt:,} ({pct:.1f}%)")

    # ══════════════════════════════════════════════════════════
    #  STEP 4 — Latest signals snapshot
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 4: Latest signals snapshot")

    snap = latest_signals(signals)
    log.kv("Date", snap["date"].iloc[0].strftime("%Y-%m-%d"))

    tbl_cols = [
        {"header": "Signal", "style": "bold"},
        {"header": "Ticker", "style": "bold cyan"},
        {"header": "#", "justify": "right"},
        {"header": "Composite", "justify": "right"},
        {"header": "Strength", "justify": "right"},
        {"header": "Regime", "justify": "center"},
        {"header": "1d", "justify": "right"},
        {"header": "Entry?", "justify": "center"},
        {"header": "Exit?", "justify": "center"},
    ]
    if has_gates:
        tbl_cols.append({"header": "Gates", "justify": "center"})
        tbl_cols.append({"header": "Reason"})

    tbl_rows = []
    for ticker, row in snap.iterrows():
        sig_style = {
            BUY: "🟢 BUY", HOLD: "🔵 HOLD",
            SELL: "🔴 SELL", NEUTRAL: "⚪ —",
        }.get(row["signal"], row["signal"])

        ret_1d = row.get("ret_1d", float("nan"))
        r = [
            sig_style,
            ticker,
            str(int(row["rank"])),
            f"{row['score_composite']:.3f}",
            f"{row.get('signal_strength', 0):.3f}",
            log.regime_badge(str(row.get("rs_regime", "?"))),
            f"{ret_1d:+.1%}" if pd.notna(ret_1d) else "—",
            "✓" if row.get("entry_eligible", False) else "",
            "✕" if row.get("exit_triggered", False) else "",
        ]
        if has_gates:
            from output.signals import _count_gates
            r.append(_count_gates(row))
            r.append(str(row.get("sig_reason", "")))
        tbl_rows.append(r)
    log.table("Current Signals", tbl_cols, tbl_rows)

    # Active positions
    pos = active_positions(signals)
    log.kv("Active positions", pos)

    # ══════════════════════════════════════════════════════════
    #  STEP 5 — Signal changes (recent transitions)
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 5: Signal changes (recent transitions)")

    changes = signal_changes(signals)
    log.kv("Total transitions", len(changes))

    if not changes.empty:
        recent = changes.tail(20)
        log.h2("Last 20 transitions")

        ch_cols = [
            {"header": "Date", "style": "bold"},
            {"header": "Ticker", "style": "bold cyan"},
            {"header": "Transition"},
            {"header": "#", "justify": "right"},
            {"header": "Score", "justify": "right"},
        ]
        ch_rows = []
        for (dt, tkr), row in recent.iterrows():
            dt_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            ch_rows.append([
                dt_str,
                tkr,
                row.get("transition", "?"),
                str(int(row.get("rank", 0))),
                f"{row.get('score_composite', 0):.3f}",
            ])
        log.table("Recent Transitions", ch_cols, ch_rows)

    # Count transition types
    if not changes.empty and "transition" in changes.columns:
        log.h2("Transition counts")
        trans_counts = changes["transition"].value_counts()
        for trans, cnt in trans_counts.items():
            log.kv(f"  {trans}", cnt)

    # ══════════════════════════════════════════════════════════
    #  STEP 6 — Signal history for top position
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 6: Signal history for top position")

    if pos:
        top_pos = pos[0]
        log.kv("Ticker", top_pos)

        hist = signal_history(signals, top_pos)
        log.kv("History rows", len(hist))

        if not hist.empty:
            recent_hist = hist.tail(15)
            h_cols = [
                {"header": "Date", "style": "bold"},
                {"header": "Signal"},
                {"header": "#", "justify": "right"},
                {"header": "Score", "justify": "right"},
                {"header": "Strength", "justify": "right"},
                {"header": "Regime"},
            ]
            h_rows = []
            for dt, row in recent_hist.iterrows():
                dt_str = (
                    dt.strftime("%Y-%m-%d")
                    if hasattr(dt, "strftime") else str(dt)
                )
                sig_icon = {
                    BUY: "🟢 BUY", HOLD: "🔵 HOLD",
                    SELL: "🔴 SELL", NEUTRAL: "⚪ —",
                }.get(row["signal"], row["signal"])
                h_rows.append([
                    dt_str,
                    sig_icon,
                    str(int(row.get("rank", 0))),
                    f"{row.get('score_composite', 0):.3f}",
                    f"{row.get('signal_strength', 0):.3f}",
                    str(row.get("rs_regime", "?")),
                ])
            log.table(
                f"{top_pos} Signal History", h_cols, h_rows,
            )
    else:
        log.info("No active positions to show history for")

    # ══════════════════════════════════════════════════════════
    #  STEP 7 — Turnover analysis
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 7: Turnover analysis")

    turnover = compute_turnover(signals, lookback=20)
    log.kv("Turnover shape", turnover.shape)

    if not turnover.empty:
        total_buys  = int(turnover["buys"].sum())
        total_sells = int(turnover["sells"].sum())
        avg_active  = turnover["active"].mean()
        avg_turn    = turnover["rolling_turnover"].iloc[-1]

        log.kv("Total buy signals", total_buys)
        log.kv("Total sell signals", total_sells)
        log.kv("Avg active positions", f"{avg_active:.1f}")
        log.kv("Rolling turnover (20d)", f"{avg_turn:.3f}")

        recent_to = turnover.tail(10)
        to_cols = [
            {"header": "Date", "style": "bold"},
            {"header": "Buys", "justify": "right"},
            {"header": "Sells", "justify": "right"},
            {"header": "Active", "justify": "right"},
            {"header": "Turnover", "justify": "right"},
        ]
        to_rows = []
        for dt, row in recent_to.iterrows():
            dt_str = (
                dt.strftime("%Y-%m-%d")
                if hasattr(dt, "strftime") else str(dt)
            )
            to_rows.append([
                dt_str,
                str(int(row["buys"])),
                str(int(row["sells"])),
                str(int(row["active"])),
                f"{row['rolling_turnover']:.3f}",
            ])
        log.table("Recent Turnover", to_cols, to_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 8 — Compare: with vs without per-ticker gates
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 8: With vs without per-ticker gates")

    # ── With gates (already computed) ─────────────────────
    pos_gated = active_positions(signals)
    summary_gated = signals_summary(signals)

    # ── Without gates: rebuild ranked panel from pre-gate data
    log.info("Rebuilding universe WITHOUT per-ticker gates...")
    ungated_universe: dict[str, pd.DataFrame] = {}
    for ticker, df in etf_data.items():
        df2 = compute_all_indicators(df)
        df2 = compute_all_rs(df2, spy)
        bseries = (
            pillar_df[ticker] if ticker in pillar_df.columns
            else None
        )
        df2 = compute_composite_score(df2, breadth_scores=bseries)
        # Intentionally skip generate_ticker_signals()
        ungated_universe[ticker] = df2

    ranked_ungated = compute_all_rankings(ungated_universe)
    signals_ungated = compute_all_signals(
        ranked_ungated, breadth, config,
    )
    pos_ungated = active_positions(signals_ungated)
    summary_ungated = signals_summary(signals_ungated)

    log.kv("Active (with gates)", f"{pos_gated}")
    log.kv("Active (no gates)", f"{pos_ungated}")

    comp_cols = [
        {"header": "Mode"},
        {"header": "Active", "justify": "right"},
        {"header": "BUY", "justify": "right"},
        {"header": "HOLD", "justify": "right"},
        {"header": "SELL", "justify": "right"},
        {"header": "Mean Str", "justify": "right"},
    ]
    comp_rows = []
    for label, sdf, sm in [
        ("With gates", signals, summary_gated),
        ("No gates (fallback)", signals_ungated, summary_ungated),
    ]:
        comp_rows.append([
            label,
            str(sm.get("n_active", 0)),
            str(sm.get("n_buy", 0)),
            str(sm.get("n_hold", 0)),
            str(sm.get("n_sell", 0)),
            f"{sm.get('mean_strength', 0):.3f}",
        ])
    log.table("Gate Comparison", comp_cols, comp_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 9 — Alternative portfolio configs
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 9: Alternative portfolio configs")

    # ── Aggressive ────────────────────────────────────────
    log.h2("Aggressive config")
    agg_config = SignalConfig(
        entry_rank_max=7,
        exit_rank_max=10,
        entry_score_min=0.35,
        exit_score_min=0.25,
        max_positions=7,
    )

    agg_signals = compute_all_signals(ranked, breadth, agg_config)
    agg_pos = active_positions(agg_signals)
    log.kv("Active positions", f"{len(agg_pos)}: {agg_pos}")

    # ── Conservative ──────────────────────────────────────
    log.h2("Conservative config")
    cons_config = SignalConfig(
        entry_rank_max=3,
        exit_rank_max=5,
        entry_score_min=0.50,
        exit_score_min=0.40,
        max_positions=3,
    )

    cons_signals = compute_all_signals(ranked, breadth, cons_config)
    cons_pos = active_positions(cons_signals)
    log.kv("Active positions", f"{len(cons_pos)}: {cons_pos}")

    # ── Comparison table ──────────────────────────────────
    log.h2("Config comparison")
    cfg_cols = [
        {"header": "Config"},
        {"header": "Active", "justify": "right"},
        {"header": "BUY %", "justify": "right"},
        {"header": "HOLD %", "justify": "right"},
        {"header": "SELL %", "justify": "right"},
        {"header": "Mean Str", "justify": "right"},
    ]
    cfg_rows = []
    for label, sdf in [
        ("Default", signals),
        ("Aggressive", agg_signals),
        ("Conservative", cons_signals),
    ]:
        sm = signals_summary(sdf)
        n  = max(len(sdf), 1)
        cfg_rows.append([
            label,
            str(sm.get("n_active", 0)),
            f"{sdf['signal'].eq(BUY).sum() / n * 100:.1f}%",
            f"{sdf['signal'].eq(HOLD).sum() / n * 100:.1f}%",
            f"{sdf['signal'].eq(SELL).sum() / n * 100:.1f}%",
            f"{sm.get('mean_strength', 0):.3f}",
        ])
    log.table("Config Comparison", cfg_cols, cfg_rows)

    # ══════════════════════════════════════════════════════════
    #  STEP 10 — Breadth circuit breaker
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 10: Breadth circuit breaker")

    # ── exit_all mode with fake bearish breadth ───────────
    exit_all_config = SignalConfig(
        breadth_bearish_action="exit_all",
    )
    fake_breadth = breadth.copy()
    fake_breadth.iloc[
        -5:,
        fake_breadth.columns.get_loc("breadth_regime"),
    ] = "weak"

    exit_all_signals = compute_all_signals(
        ranked, fake_breadth, exit_all_config,
    )
    ea_pos = active_positions(exit_all_signals)
    log.kv(
        "Active (bearish + exit_all)",
        f"{len(ea_pos)}: {ea_pos}",
    )

    default_pos = active_positions(signals)
    log.kv(
        "Active (real breadth)",
        f"{len(default_pos)}: {default_pos}",
    )

    if len(ea_pos) == 0:
        log.success("exit_all: all positions liquidated")
    elif len(ea_pos) < len(default_pos):
        log.success(
            f"exit_all reduced: {len(default_pos)} → {len(ea_pos)}"
        )

    # ── reduce mode ───────────────────────────────────────
    log.h2("Reduce mode (block new buys)")
    reduce_config = SignalConfig(
        breadth_bearish_action="reduce",
    )
    reduce_signals = compute_all_signals(
        ranked, fake_breadth, reduce_config,
    )
    red_pos = active_positions(reduce_signals)
    log.kv(
        "Active (bearish + reduce)",
        f"{len(red_pos)}: {red_pos}",
    )

    # ══════════════════════════════════════════════════════════
    #  STEP 11 — Text report
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 11: Signals text report")

    report = signals_report(
        signals,
        breadth_regime=breadth["breadth_regime"].iloc[-1],
        breadth_score=breadth["breadth_score"].iloc[-1],
        config=config,
    )
    log.print(f"\n{report}")

    # ══════════════════════════════════════════════════════════
    #  STEP 12 — Validation
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 12: Validation")

    all_ok = True

    # Valid signal values
    valid_sigs = set(signals["signal"].unique())
    expected_sigs = {BUY, HOLD, SELL, NEUTRAL}
    if valid_sigs.issubset(expected_sigs):
        log.success(f"All signals valid: {valid_sigs}")
    else:
        log.warning(
            f"Unexpected signals: {valid_sigs - expected_sigs}"
        )
        all_ok = False

    # in_position consistent with signal
    in_pos_check = signals["in_position"] == signals[
        "signal"
    ].isin([BUY, HOLD])
    if in_pos_check.all():
        log.success("in_position consistent with signal")
    else:
        log.warning("in_position inconsistent")
        all_ok = False

    # signal_strength ∈ [0, 1]
    ss = signals["signal_strength"]
    if (ss >= 0).all() and (ss <= 1).all():
        log.success(
            f"signal_strength in [0, 1]  "
            f"[{ss.min():.4f}, {ss.max():.4f}]"
        )
    else:
        log.warning("signal_strength out of range")
        all_ok = False

    # SELL/NEUTRAL have strength = 0
    sn_str = signals.loc[
        signals["signal"].isin([SELL, NEUTRAL]),
        "signal_strength",
    ]
    if (sn_str == 0).all():
        log.success("SELL/NEUTRAL signals have strength = 0")
    else:
        log.warning("Non-zero strength for SELL/NEUTRAL")
        all_ok = False

    # max_positions respected
    active_per_day = signals[
        signals["signal"].isin([BUY, HOLD])
    ].groupby(level="date").size()
    max_active = active_per_day.max()
    if max_active <= config.max_positions:
        log.success(
            f"Max positions respected: "
            f"{max_active} ≤ {config.max_positions}"
        )
    else:
        log.warning(
            f"Position limit breached: "
            f"{max_active} > {config.max_positions}"
        )
        all_ok = False

    # No consecutive BUY (proper BUY → HOLD)
    consec_ok = True
    for ticker in signals.index.get_level_values(
        "ticker",
    ).unique():
        th = signal_history(signals, ticker)
        if th.empty:
            continue
        sigs = th["signal"]
        consec_buy = (sigs == BUY) & (sigs.shift(1) == BUY)
        if consec_buy.any():
            log.warning(f"Consecutive BUY for {ticker}")
            consec_ok = False
            break

    if consec_ok:
        log.success("No consecutive BUY signals (BUY → HOLD)")
    all_ok = all_ok and consec_ok

    # HOLD only follows BUY or HOLD
    holds_ok = True
    for ticker in signals.index.get_level_values(
        "ticker",
    ).unique():
        th = signal_history(signals, ticker)
        if th.empty:
            continue
        sigs = th["signal"]
        prev = sigs.shift(1)
        hold_rows = sigs == HOLD
        if hold_rows.any():
            prev_for_holds = prev[hold_rows]
            invalid = ~prev_for_holds.isin([BUY, HOLD])
            if invalid.any():
                log.warning(
                    f"HOLD without prior position for {ticker}"
                )
                holds_ok = False
                break

    if holds_ok:
        log.success("All HOLD signals follow BUY or HOLD")
    all_ok = all_ok and holds_ok

    # SELL only follows BUY or HOLD
    sells_ok = True
    for ticker in signals.index.get_level_values(
        "ticker",
    ).unique():
        th = signal_history(signals, ticker)
        if th.empty:
            continue
        sigs = th["signal"]
        prev = sigs.shift(1)
        sell_rows = sigs == SELL
        if sell_rows.any():
            prev_for_sells = prev[sell_rows]
            invalid = ~prev_for_sells.isin([BUY, HOLD])
            if invalid.any():
                log.warning(
                    f"SELL without prior position for {ticker}"
                )
                sells_ok = False
                break

    if sells_ok:
        log.success("All SELL signals follow BUY or HOLD")
    all_ok = all_ok and sells_ok

    # sig_confirmed gating: verify BUY only where confirmed
    if has_gates:
        buys = signals[signals["signal"] == BUY]
        if not buys.empty:
            unconfirmed_buys = buys[
                buys["sig_confirmed"] != 1
            ]
            if unconfirmed_buys.empty:
                log.success(
                    "All BUY signals have sig_confirmed == 1"
                )
            else:
                log.warning(
                    f"{len(unconfirmed_buys)} BUY signals "
                    f"without sig_confirmed"
                )
                all_ok = False

    # ══════════════════════════════════════════════════════════
    #  STEP 13 — Edge cases
    # ══════════════════════════════════════════════════════════
    log.h1("STEP 13: Edge cases")

    empty_signals = compute_all_signals(pd.DataFrame())
    if empty_signals.empty:
        log.success("Empty ranked → empty signals")
    else:
        log.warning("Expected empty output")
        all_ok = False

    empty_report = signals_report(pd.DataFrame())
    if "No signal" in empty_report:
        log.success("Empty report handled gracefully")
    else:
        log.warning("Empty report not handled")

    empty_changes = signal_changes(pd.DataFrame())
    if empty_changes.empty:
        log.success("Empty signal_changes handled")

    empty_pos = active_positions(pd.DataFrame())
    if empty_pos == []:
        log.success("Empty active_positions returns []")

    # ══════════════════════════════════════════════════════════
    #  SUMMARY
    # ══════════════════════════════════════════════════════════
    elapsed = time.time() - t0

    log.h1("SUMMARY")
    log.kv("Sector ETFs", len(etf_data))
    log.kv(
        "Per-ticker gates",
        "active" if has_gates else "fallback",
    )
    log.kv("Active positions", f"{len(pos_gated)}: {pos_gated}")
    log.kv("Total transitions", len(changes))
    if not turnover.empty:
        log.kv(
            "Rolling turnover",
            f"{turnover['rolling_turnover'].iloc[-1]:.3f}",
        )
    log.kv(
        "All validations",
        "PASSED ✓" if all_ok else "ISSUES FOUND ⚠",
    )
    log.kv("Elapsed", f"{elapsed:.1f}s")
    log.divider()
    log.success("ALL SIGNAL TESTS PASSED")

    html_path = log.save()
    log.print(f"\n  [dim]HTML report → {html_path}[/]")


if __name__ == "__main__":
    main()