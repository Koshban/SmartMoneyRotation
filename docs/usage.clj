;; ═══════════════════════════════════════════════════════════════════════════
;; Usage.clj — CASH Strategy System: Operations & Execution Guide
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                       SYSTEM OVERVIEW                                  │
;; │                                                                        │
;; │  The CASH system has two operational layers:                            │
;; │                                                                        │
;; │    1. DATA LAYER   — ingest, store, and serve OHLCV + options data     │
;; │    2. STRATEGY LAYER — analyse, score, rotate, and recommend           │
;; │                                                                        │
;; │  Data must be loaded BEFORE strategy scripts can run.                  │
;; │  The strategy layer reads exclusively through loader.py.               │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                    THREE STRATEGY MODES                                │
;; │                                                                        │
;; │                                                                        │
;; │  ┌───────────┐     ┌────────────┐     ┌──────────────────────────┐     │
;; │  │  TOP-DOWN  │     │ BOTTOM-UP  │     │          FULL            │     │
;; │  │           │     │            │     │                          │     │
;; │  │ Sector    │     │ Per-ticker │     │ Bottom-up + Top-down     │     │
;; │  │ rotation  │     │ scoring    │     │ + convergence merge      │     │
;; │  │ + RS rank │     │ pipeline   │     │                          │     │
;; │  │           │     │            │     │ Indicator data from      │     │
;; │  │ Fast      │     │ Thorough   │     │ scoring feeds rotation   │     │
;; │  │ (~10-30s) │     │ (~1-3min)  │     │ quality filter           │     │
;; │  └─────┬─────┘     └─────┬──────┘     └────────────┬─────────────┘     │
;; │        │                 │                         │                   │
;; │        ▼                 ▼                         ▼                   │
;; │   "Where is         "Which stocks            "What should I           │
;; │    money flowing?"   look strongest?"         buy/sell/hold?"         │
;; │                                                                        │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                  MARKET COVERAGE                                       │
;; │                                                                        │
;; │  Market │ Benchmark     │ Top-Down Engine    │ Bottom-Up │ Rotation    │
;; │  ───────┼───────────────┼────────────────────┼───────────┼─────────── │
;; │  US     │ SPY           │ Sector rotation    │ ✓         │ ✓          │
;; │         │               │ (11 GICS sectors)  │           │            │
;; │  HK     │ 2800.HK       │ RS ranking vs      │ ✓         │ ✗          │
;; │         │               │ Tracker Fund       │           │            │
;; │  IN     │ NIFTYBEES.NS  │ RS ranking vs      │ ✓         │ ✗          │
;; │         │               │ Nifty BeES         │           │            │
;; │  ALL    │ (each above)  │ (each above)       │ ✓         │ US only    │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; PREREQUISITE: DATA LOADING
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; Strategy scripts CANNOT run without data.  Complete these steps first.
;; See DataArchitecture.clj for full details on the data pipeline.
;;
;;
;; ── Option A: Full Backfill (first time / weekly) ─────────────────────────
;;
;;   # 1. Download 2 years of OHLCV for all markets
;;   python src/ingest_cash.py --market all --period 20y
;;
;;   # 2. Download options chains (optional — needed for options analysis)
;;   python src/ingest_options.py --market all --consolidate
;;
;;   # 3. Load into PostgreSQL
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;;
;; ── Option B: Daily Refresh (cron / morning routine) ──────────────────────
;;
;;   # Refresh last 5 days (auto-selects IBKR for fresh data)
;;   python src/ingest_cash.py --market all --period 5d
;;
;;   # Reload into DB
;;   python src/db/load_db.py --market all --type cash
;;
;;
;; ── Option C: Single Market Only ──────────────────────────────────────────
;;
;;   python src/ingest_cash.py --market us --period 2y
;;   python src/db/load_db.py --market us --type cash
;;
;;
;; ── Verify Data Is Available ──────────────────────────────────────────────
;;
;;   # Quick check — loader.py tries PostgreSQL → Parquet → yfinance
;;   python -c "from src.db.loader import load_ohlcv; \
;;              df = load_ohlcv('SPY'); \
;;              print(f'SPY: {len(df)} bars, {df.index[0]} to {df.index[-1]}')"
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; MODE 1: TOP-DOWN  (Sector Rotation / RS Ranking)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; What it does:
;;   - Loads OHLCV → builds wide price matrix
;;   - US:    Ranks 11 GICS sectors by composite RS
;;            Identifies leading (top 3) and lagging sectors
;;            Picks strongest stocks within leading sectors
;;            Flags holdings in lagging sectors for selling
;;   - HK/IN: Ranks all tickers by composite RS vs benchmark
;;            Splits into top / middle / bottom tiers
;;
;; When to use:
;;   - Quick morning check: "Where is sector momentum?"
;;   - Deciding which sectors to overweight / underweight
;;   - Fast screening before deeper bottom-up analysis
;;
;; Execution time: ~10-30 seconds
;;
;;
;; ── Basic US Rotation ─────────────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market US
;;
;;   Output:
;;     - 11 sectors ranked by composite RS (21d + 63d + 126d blend)
;;     - Leading sectors (top 3): BUY candidates within each
;;     - Lagging sectors (bottom 3): SELL flags for holdings
;;     - Stock picks ranked by RS within each leading sector
;;
;;
;; ── US Rotation with Quality Filter ───────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market US --quality
;;
;;   Adds ~30-60s for inline indicator computation.
;;   Each stock candidate is scored on 6 dimensions:
;;     MA structure, RSI, MACD, ADX, volume, volatility
;;   Quality gate rejects weak setups; quality score blends with RS.
;;
;;   With custom quality weight (default 0.40):
;;     python -m scripts.run_strategy top-down --market US \
;;         --quality --quality-weight 0.5
;;
;;
;; ── US Rotation with Current Holdings ─────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market US \
;;       --holdings NVDA,CRWD,CEG,LMT,VST
;;
;;   Holdings are evaluated against sector rankings:
;;     - In leading sector  →  HOLD (confirmed)
;;     - In stagnant sector →  REDUCE (trim exposure)
;;     - In lagging sector  →  SELL (rotate out)
;;
;;
;; ── US Rotation with All Options ──────────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market US \
;;       --quality \
;;       --holdings NVDA,CRWD,CEG \
;;       --stocks-per-sector 5 \
;;       --max-positions 15 \
;;       --lookback 365
;;
;;
;; ── HK Top-Down (RS Ranking) ──────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market HK
;;
;;   Output:
;;     - All HK tickers ranked by composite RS vs 2800.HK
;;     - Tiered display: 🟢 Top / ⚪ Middle / 🔴 Bottom
;;     - Per-ticker return breakdown (21d, 63d, 126d)
;;
;;
;; ── India Top-Down (RS Ranking) ───────────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market IN
;;
;;   Output:
;;     - All India tickers ranked by composite RS vs NIFTYBEES.NS
;;     - Same tiered display as HK
;;
;;
;; ── Export Top-Down Results to JSON ───────────────────────────────────────
;;
;;   python -m scripts.run_strategy top-down --market US \
;;       --quality -o results/topdown_us.json
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; MODE 2: BOTTOM-UP  (Per-Ticker Scoring Pipeline)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; What it does:
;;   Runs the full orchestrator pipeline (phases 0 → 1 → 2 → 3 → 4):
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │  Phase 0 │ Data Loading                                                │
;; │          │ Load OHLCV for all tickers + benchmark                      │
;; │          │ When --lookback is set: fetches lookback + 220 warmup days  │
;; │          │                                                             │
;; │  Phase 1 │ Universe-Level Context                                      │
;; │          │ Breadth indicators (advance/decline, % above 50d SMA)       │
;; │          │ Sector relative strength (11 sectors vs SPY)                │
;; │          │ Breadth regime: strong / neutral / weak                     │
;; │          │                                                             │
;; │  Phase 2 │ Per-Ticker Pipeline  (runner.run_batch)                     │
;; │          │ For each ticker:                                            │
;; │          │   indicators → RS → scoring → sector merge → signals       │
;; │          │                                                             │
;; │          │ Indicators computed (compute/indicators.py):                │
;; │          │   returns, RSI, MACD, ADX, EMA/SMA, ATR, realized vol,     │
;; │          │   OBV, A/D line, volume metrics, Amihud, VWAP distance     │
;; │          │                                                             │
;; │          │ Scoring pillars:                                            │
;; │          │   P1: Momentum (returns + RS)                               │
;; │          │   P2: Trend (MA structure + ADX)                            │
;; │          │   P3: Volume (OBV slope + relative volume + A/D)            │
;; │          │   P4: Risk (volatility + ATR + Amihud)                      │
;; │          │   P5: Breadth (universe context from Phase 1)               │
;; │          │                                                             │
;; │  ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
;; │  SKIP   │ Phase 2.5  (rotation)    — not run in bottom-up mode        │
;; │  SKIP   │ Phase 2.75 (convergence) — not run in bottom-up mode        │
;; │  ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  │
;; │          │                                                             │
;; │  Phase 3 │ Cross-Sectional Analysis                                    │
;; │          │ 3a: Rankings (cross-sectional rank per date)                │
;; │          │ 3b: Portfolio (position selection + weight allocation)       │
;; │          │ 3c: Signals (BUY/HOLD/SELL with hysteresis)                │
;; │          │                                                             │
;; │  Phase 4 │ Reporting                                                   │
;; │          │ Recommendation report (regime, risk flags, actions)          │
;; │          │ Optional backtest (not enabled in bottom-up by default)      │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;; When to use:
;;   - Deep analysis of individual stock quality
;;   - Generating ranked watchlists with composite scores
;;   - Portfolio allocation with position sizing
;;   - When you want scored signals without rotation overlay
;;
;; Execution time: ~1-3 minutes per market
;;
;;
;; ── Basic US Bottom-Up ────────────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market US
;;
;;   Output:
;;     - Scored: N tickers  │  Errors: M  │  Compute time: Xs
;;     - Breadth regime: strong/neutral/weak
;;     - Top 15 scored tickers (score, signal, RS, RSI, ADX)
;;     - Signal summary: N BUY │ M HOLD │ K SELL
;;     - Portfolio: P positions │ Cash: X%
;;
;;
;; ── HK Bottom-Up ─────────────────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market HK
;;
;;   Scores ~45 HK tickers against 2800.HK benchmark.
;;   Breadth is computed from the HK universe itself.
;;   Sector RS is skipped (no HK sector ETFs defined).
;;
;;
;; ── India Bottom-Up ───────────────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market IN
;;
;;   Scores ~50 India large-caps against NIFTYBEES.NS benchmark.
;;
;;
;; ── Bottom-Up with Lookback Window ────────────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market US --lookback 365
;;
;;   Loads 365 + 220 = 585 calendar days from source.
;;   The extra 220 days are warmup for 200-day SMA and similar indicators.
;;   Analysis window covers the most recent 365 days.
;;
;;   Without --lookback: loads ALL available data (original behaviour).
;;
;;
;; ── Bottom-Up with Verbose Logging ────────────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market US -v
;;
;;   Shows debug-level logs from every phase:
;;     - Per-ticker indicator computation
;;     - RS calculation details
;;     - Scoring pillar breakdowns
;;     - Signal generation logic
;;
;;
;; ── Bottom-Up Quiet Mode (results only) ───────────────────────────────────
;;
;;   python -m scripts.run_strategy bottom-up --market US -q
;;
;;   Suppresses all log output; prints only the final results table.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; MODE 3: FULL  (Combined Pipeline — Maximum Conviction)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; What it does:
;;   Runs ALL orchestrator phases including rotation + convergence:
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                                                                        │
;; │  Phase 0     Data Loading                                              │
;; │      │                                                                 │
;; │      ▼                                                                 │
;; │  Phase 1     Universe Context (breadth + sector RS)                    │
;; │      │                                                                 │
;; │      ▼                                                                 │
;; │  Phase 2     Per-Ticker Scoring Pipeline                               │
;; │      │         (indicators → RS → scoring → signals)                   │
;; │      │                                                                 │
;; │      ├───────────────────────────────────┐                             │
;; │      │                                   │                             │
;; │      ▼                                   ▼                             │
;; │  Phase 2.5   Rotation Engine         indicator_data                    │
;; │      │         (US: sector rotation     feeds quality                  │
;; │      │          HK/IN: skipped)         filter in                     │
;; │      │                                  rotation                      │
;; │      ▼                                                                 │
;; │  Phase 2.75  Convergence Merge                                         │
;; │      │         Scoring list  ←→  Rotation list                        │
;; │      │         Both agree?   →  STRONG_BUY                            │
;; │      │         Disagree?     →  CONFLICT (flagged)                    │
;; │      │         US: dual-list merge                                    │
;; │      │         HK/IN: scoring passthrough                             │
;; │      │                                                                 │
;; │      ▼                                                                 │
;; │  Phase 3     Cross-Sectional (rankings + portfolio + signals)          │
;; │      │                                                                 │
;; │      ▼                                                                 │
;; │  Phase 4     Reports + Optional Backtest                               │
;; │                                                                        │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;; When to use:
;;   - Final decision-making: "What do I actually trade today?"
;;   - Highest conviction — both engines must agree for STRONG_BUY
;;   - Weekly portfolio review with rotation + scoring combined
;;   - Generating reports for record-keeping
;;
;; Execution time: ~2-5 minutes per market
;;
;;
;; ── Single Market Full Pipeline ───────────────────────────────────────────
;;
;;   python -m scripts.run_strategy full --market US
;;
;;   Output includes:
;;     - Bottom-up scoring summary (top 10 scored tickers)
;;     - Sector rotation overlay (leading/lagging sectors, BUY/SELL)
;;     - Convergence merge (STRONG_BUY, CONFLICT flags)
;;     - Portfolio allocation
;;
;;
;; ── Full Pipeline with Holdings ───────────────────────────────────────────
;;
;;   python -m scripts.run_strategy full --market US \
;;       --holdings NVDA,CRWD,CEG,LMT,VST
;;
;;   Holdings are evaluated by both engines:
;;     - Scoring: individual signal (BUY/HOLD/SELL per composite score)
;;     - Rotation: sector-level signal (is the holding's sector leading?)
;;     - Convergence: reconciles both signals
;;
;;
;; ── Multi-Market Full Pipeline ────────────────────────────────────────────
;;
;;   python -m scripts.run_strategy full --market ALL
;;
;;   Runs US → HK → IN sequentially:
;;     - Each market gets its own Orchestrator instance
;;     - US gets rotation + convergence
;;     - HK/IN get scoring passthrough (no rotation)
;;     - Returns dict[market_code, PipelineResult]
;;
;;
;; ── Multi-Market with Holdings + Lookback + Export ────────────────────────
;;
;;   python -m scripts.run_strategy full --market ALL \
;;       --holdings NVDA,CRWD,CEG \
;;       --lookback 365 \
;;       -o results/full_report.json \
;;       -v
;;
;;   Holdings are routed to US (only market with rotation).
;;   Each market loads 365 + 220 = 585 calendar days.
;;   JSON export contains scored tickers, rotation picks, convergence.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; COMMON WORKFLOWS
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── Workflow A: Morning Scan (Daily, ~2 min) ──────────────────────────────
;;
;;   # 1. Refresh data (5d window → IBKR source)
;;   python src/ingest_cash.py --market all --period 5d
;;   python src/db/load_db.py --market all --type cash
;;
;;   # 2. Quick top-down sector check
;;   python -m scripts.run_strategy top-down --market US
;;
;;   # 3. If sectors look interesting, run full for trade decisions
;;   python -m scripts.run_strategy full --market US \
;;       --holdings NVDA,CRWD,CEG
;;
;;
;; ── Workflow B: Weekly Portfolio Review (Weekly, ~5 min) ──────────────────
;;
;;   # 1. Full data refresh
;;   python src/ingest_cash.py --market all --period 2y
;;   python src/db/load_db.py --market all --type cash
;;
;;   # 2. Complete analysis across all markets
;;   python -m scripts.run_strategy full --market ALL \
;;       --holdings NVDA,CRWD,CEG,LMT,VST,HDFCBANK.NS \
;;       -o results/weekly_$(date +%Y%m%d).json
;;
;;   # 3. Review JSON output for trade actions
;;
;;
;; ── Workflow C: Research / Deep Dive (Ad-hoc) ─────────────────────────────
;;
;;   # Bottom-up scoring for one market with full verbosity
;;   python -m scripts.run_strategy bottom-up --market HK -v
;;
;;   # Top-down with quality filter + custom params
;;   python -m scripts.run_strategy top-down --market US \
;;       --quality \
;;       --quality-weight 0.5 \
;;       --stocks-per-sector 5 \
;;       --max-positions 20
;;
;;
;; ── Workflow D: Notebook / Interactive (Python REPL) ──────────────────────
;;
;;   from pipeline.orchestrator import Orchestrator
;;
;;   orch = Orchestrator(market="US", lookback_days=365)
;;
;;   # Phase-by-phase control
;;   orch.load_data()
;;   orch.compute_universe_context()
;;   orch.run_tickers()
;;
;;   # Inspect intermediate state
;;   print(orch._breadth["breadth_regime"].iloc[-1])
;;   print(len(orch._scored_universe))
;;
;;   # Optionally run rotation
;;   orch.run_rotation_engine(current_holdings=["NVDA", "CRWD"])
;;   orch.apply_convergence()
;;
;;   # Finish pipeline
;;   orch.cross_sectional_analysis()
;;   result = orch.generate_reports()
;;
;;   # Explore results
;;   print(result.summary())
;;   for s in result.top_n(5):
;;       print(f"{s['ticker']:8s}  {s['composite_score']:.3f}  {s['signal']}")
;;
;;   # Re-run portfolio with different capital (no recompute)
;;   result2 = orch.rebuild_portfolio(capital=50_000)
;;
;;   # Re-run a single ticker
;;   tr = orch.rerun_ticker("NVDA")
;;   print(tr.snapshot)
;;
;;
;; ── Workflow E: Programmatic Invocation (from tests / scripts) ────────────
;;
;;   from scripts.run_strategy import main
;;
;;   # Returns dict[str, Any] — results keyed by market
;;   results = main(["top-down", "--market", "US", "--quality"])
;;   results = main(["bottom-up", "--market", "HK", "-q"])
;;   results = main(["full", "--market", "ALL", "-o", "out.json"])
;;
;;   # From orchestrator directly
;;   from pipeline.orchestrator import run_full_pipeline
;;
;;   result = run_full_pipeline(market="US", lookback_days=365)
;;   print(result.summary())
;;
;;   # Multi-market
;;   from pipeline.orchestrator import run_multi_market_pipeline
;;
;;   results = run_multi_market_pipeline(
;;       active_markets=["US", "HK", "IN"],
;;       lookback_days=365,
;;       current_holdings={"US": ["NVDA", "CRWD"], "HK": [], "IN": []},
;;   )
;;   for market, r in results.items():
;;       print(f"[{market}] {r.summary()}")
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; CLI REFERENCE
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                     GLOBAL FLAGS (all modes)                           │
;; │                                                                        │
;; │  --market CODE     Market: US, HK, IN, or ALL       (default: US)     │
;; │  --lookback DAYS   Calendar days of history          (default: all)    │
;; │  --holdings T,T,T  Comma-separated current positions (default: none)  │
;; │  --output PATH     Export results to JSON file       (default: none)  │
;; │  --verbose / -v    Debug-level logging                                │
;; │  --quiet / -q      Suppress logs (results only)                       │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                     TOP-DOWN FLAGS                                     │
;; │                                                                        │
;; │  --quality              Enable quality filter (US only)               │
;; │  --quality-weight W     Quality vs RS blend weight     (default: 0.40)│
;; │  --stocks-per-sector N  Picks per leading sector       (default: 3)   │
;; │  --max-positions N      Max total portfolio positions   (default: 12)  │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                     BOTTOM-UP FLAGS                                    │
;; │                                                                        │
;; │  (no additional flags — uses global flags only)                        │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                     FULL FLAGS                                         │
;; │                                                                        │
;; │  (no additional flags — uses global flags only)                        │
;; │  Rotation + convergence are auto-enabled for US.                      │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; OUTPUT INTERPRETATION
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── Top-Down US: Sector Rotation Output ───────────────────────────────────
;;
;;   SECTOR RANKINGS (11 sectors, ranked by composite RS):
;;     Rank 1  Technology   [XLK]   LEADING    RS: +0.0832
;;     Rank 2  Industrials  [XLI]   LEADING    RS: +0.0541
;;     Rank 3  Financials   [XLF]   LEADING    RS: +0.0312
;;       ...
;;     Rank 10 Real Estate  [XLRE]  LAGGING    RS: -0.0421
;;     Rank 11 Utilities    [XLU]   LAGGING    RS: -0.0614
;;
;;   LEADING sectors generate BUY picks.
;;   LAGGING sectors generate SELL flags for holdings.
;;   STAGNANT sectors (middle) generate REDUCE flags.
;;
;;   RECOMMENDATIONS:
;;     🟢 BUY   NVDA  — Technology (rank 1), RS: +0.1204
;;     🟢 BUY   AVGO  — Technology (rank 1), RS: +0.0983
;;     🔴 SELL  XLU   — Utilities lagging (rank 11)
;;     ⚠  REDUCE CEG  — Utilities stagnant (rank 8)
;;
;;
;; ── Top-Down HK/IN: RS Ranking Output ─────────────────────────────────────
;;
;;   🟢 TOP TIER    (strongest RS vs benchmark)
;;   ⚪ MIDDLE TIER (average RS)
;;   🔴 BOTTOM TIER (weakest RS vs benchmark)
;;
;;   Per ticker: composite RS score + return breakdown (21d, 63d, 126d)
;;
;;
;; ── Bottom-Up: Scoring Output ─────────────────────────────────────────────
;;
;;   Each ticker gets a composite_score (0.000 to 1.000):
;;     > 0.700  Strong (likely BUY signal)
;;     0.500-0.700  Moderate (likely HOLD)
;;     < 0.500  Weak (likely SELL or avoid)
;;
;;   Signals are generated with hysteresis (avoids whipsaws):
;;     BUY   — composite score above threshold + breadth gate + rank gate
;;     SELL  — score drops below threshold for sustained period
;;     HOLD  — everything in between
;;
;;
;; ── Full: Convergence Labels ──────────────────────────────────────────────
;;
;;   STRONG_BUY  — Both scoring AND rotation recommend BUY
;;                 Highest conviction; prioritise for capital allocation
;;
;;   BUY         — One engine recommends BUY, other is neutral
;;                 Good candidate; standard position sizing
;;
;;   CONFLICT    — Engines disagree (one says BUY, other says SELL)
;;                 Flagged for manual review; no automatic action
;;
;;   SELL        — Both engines recommend SELL or one SELL + neutral
;;                 Exit position
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; PIPELINE RESULT OBJECT  (PipelineResult dataclass)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; The full and bottom-up modes return PipelineResult objects.
;; Key attributes for downstream consumption:
;;
;;   result.snapshots          List[dict] — scored tickers, sorted by score
;;     [0]["ticker"]           Ticker symbol
;;     [0]["composite_score"]  Float 0-1
;;     [0]["signal"]           "BUY" / "HOLD" / "SELL"
;;     [0]["rs_score"]         Relative strength vs benchmark
;;     [0]["rsi_14"]           14-period RSI
;;     [0]["adx_14"]           14-period ADX
;;     [0]["weight_pct"]       Portfolio weight (if allocated)
;;     [0]["dollar_alloc"]     Dollar allocation
;;     [0]["shares"]           Share count
;;
;;   result.scored_universe    Dict[str, DataFrame] — full indicator data
;;   result.rankings           DataFrame — cross-sectional ranks
;;   result.portfolio          Dict — target_weights, sector_exposure, metadata
;;   result.signals            DataFrame — BUY/HOLD/SELL with dates
;;   result.breadth            DataFrame — breadth indicators + regime
;;   result.sector_rs          DataFrame — sector RS panel
;;   result.rotation_result    RotationResult (US only, full mode)
;;   result.convergence        MarketSignalResult (US only, full mode)
;;   result.errors             List[str] — failed tickers
;;   result.timings            Dict[str, float] — phase timings
;;
;;   result.summary()          One-line summary string
;;   result.top_n(10)          Top 10 snapshots
;;   result.n_tickers          Count of scored tickers
;;   result.n_errors           Count of errors
;;   result.total_time         Total compute time (seconds)
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; JSON EXPORT FORMAT
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; The --output / -o flag writes JSON with this structure:
;;
;;   {
;;     "mode": "full",
;;     "run_date": "2026-04-21",
;;     "markets": {
;;       "US": {
;;         "type": "pipeline",
;;         "n_tickers": 85,
;;         "n_errors": 2,
;;         "total_time": 142.3,
;;         "top_30": [
;;           {"ticker": "NVDA", "composite_score": 0.823, "signal": "BUY", ...},
;;           ...
;;         ],
;;         "rotation": {
;;           "leading_sectors": ["Technology", "Industrials", "Financials"],
;;           "lagging_sectors": ["Utilities", "Real Estate"],
;;           "buys": ["NVDA", "AVGO", "AMD", ...],
;;           "sells": ["XLU"]
;;         },
;;         "convergence": {
;;           "strong_buys": ["NVDA", "AVGO"],
;;           "n_conflicts": 1
;;         }
;;       },
;;       "HK": { ... },
;;       "IN": { ... }
;;     }
;;   }
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; TROUBLESHOOTING
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── "No data for benchmark" error ─────────────────────────────────────────
;;
;;   Data has not been loaded for the benchmark ticker.
;;   Fix: run ingest + load_db for the relevant market.
;;
;;     python src/ingest_cash.py --market us --period 2y
;;     python src/db/load_db.py --market us --type cash
;;
;;
;; ── "Phase 'load_data' has not been run yet" ──────────────────────────────
;;
;;   Orchestrator phases must run in order.
;;   When using the class directly, call orch.load_data() first.
;;
;;
;; ── Empty scored universe / 0 tickers scored ──────────────────────────────
;;
;;   Possible causes:
;;     - Insufficient data (< 60 bars per ticker)
;;     - All tickers failed indicator computation
;;   Fix: increase --lookback or check data quality with -v flag.
;;
;;
;; ── "Quality filter enabled but no indicator data available" ──────────────
;;
;;   In top-down --quality mode: indicator computation failed for all
;;   tickers.  Falls back to RS-only ranking (quality disabled).
;;   Check data availability and try with -v for details.
;;
;;
;; ── Slow performance (>5 min for single market) ──────────────────────────
;;
;;   - Use --lookback to limit data window (e.g. --lookback 365)
;;   - Use -q to suppress logging overhead
;;   - Check if DB is responding (loader.py tries PostgreSQL first)
;;   - Reduce universe size in common/config.py if testing
;;
;;
;; ── NaN values in scores / indicators ─────────────────────────────────────
;;
;;   Normal for early rows — indicators need warmup periods:
;;     RSI:    14 bars
;;     MACD:   26 bars (slow EMA)
;;     ADX:    28 bars (14 + 14 smoothing)
;;     SMA200: 200 bars
;;   The 220-day warmup buffer (added automatically when --lookback is set)
;;   is designed to handle this.  If loading all data, the first ~200 rows
;;   will have NaN for long-period indicators.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; CRON SCHEDULING EXAMPLES
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── Daily (weekdays, 6:30 AM ET — after premarket data available) ─────────
;;
;;   30 6 * * 1-5  cd /opt/cash && \
;;     python src/ingest_cash.py --market all --period 5d && \
;;     python src/db/load_db.py --market all --type cash && \
;;     python -m scripts.run_strategy full --market ALL \
;;       --holdings NVDA,CRWD,CEG \
;;       -o results/daily_$(date +\%Y\%m\%d).json \
;;       -q
;;
;;
;; ── Weekly (Sunday 8 PM ET — full backfill for clean data) ────────────────
;;
;;   0 20 * * 0  cd /opt/cash && \
;;     python src/ingest_cash.py --market all --period 2y && \
;;     python src/db/load_db.py --market all --type cash && \
;;     python -m scripts.run_strategy full --market ALL \
;;       --lookback 365 \
;;       -o results/weekly_$(date +\%Y\%m\%d).json
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; COMPLETE COMMAND REFERENCE (QUICK COPY)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   # ── Data Loading ─────────────────────────────────────────────────────
;;   python src/ingest_cash.py --market all --period 2y
;;   python src/ingest_cash.py --market us --period 5d
;;   python src/ingest_options.py --market all --consolidate
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;;   # ── Top-Down ─────────────────────────────────────────────────────────
;;   python -m scripts.run_strategy top-down --market US
;;   python -m scripts.run_strategy top-down --market US --quality
;;   python -m scripts.run_strategy top-down --market US --quality --holdings NVDA,CRWD
;;   python -m scripts.run_strategy top-down --market HK
;;   python -m scripts.run_strategy top-down --market IN
;;
;;   # ── Bottom-Up ────────────────────────────────────────────────────────
;;   python -m scripts.run_strategy bottom-up --market US
;;   python -m scripts.run_strategy bottom-up --market US --lookback 365 -v
;;   python -m scripts.run_strategy bottom-up --market HK
;;   python -m scripts.run_strategy bottom-up --market IN
;;
;;   # ── Full Pipeline ────────────────────────────────────────────────────
;;   python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG
;;   python -m scripts.run_strategy full --market ALL -o results/report.json
;;   python -m scripts.run_strategy full --market ALL --lookback 365 -v
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════

(ns usage
  "Documentation namespace — describes system usage, execution modes,
   CLI commands, workflows, and output interpretation.
   This file is not executed; it exists as living documentation.")


(def strategy-modes
  {:top-down
   {:description "Sector rotation (US) / RS ranking (HK, IN)"
    :speed       "fast (~10-30s)"
    :question    "Where is the smart money flowing?"
    :phases      ["load_data"]
    :us-engine   "run_rotation() — 11 GICS sectors"
    :hk-engine   "composite_rs_all() vs 2800.HK"
    :in-engine   "composite_rs_all() vs NIFTYBEES.NS"
    :flags       {:quality         "Enable quality filter (US only)"
                  :quality-weight  "RS/quality blend weight (default 0.40)"
                  :stocks-per-sector "Picks per leading sector (default 3)"
                  :max-positions   "Max total positions (default 12)"}
    :examples
    ["python -m scripts.run_strategy top-down --market US"
     "python -m scripts.run_strategy top-down --market US --quality --holdings NVDA,CRWD"
     "python -m scripts.run_strategy top-down --market HK"
     "python -m scripts.run_strategy top-down --market IN"]}

   :bottom-up
   {:description "Per-ticker scoring pipeline via orchestrator"
    :speed       "thorough (~1-3min per market)"
    :question    "Which individual stocks look strongest?"
    :phases      ["load_data" "universe_context" "run_tickers"
                  "cross_sectional" "reports"]
    :skipped     ["rotation" "convergence"]
    :indicators  ["returns" "RSI" "MACD" "ADX" "EMA/SMA" "ATR"
                  "realized_vol" "OBV" "A/D_line" "volume_metrics"
                  "Amihud" "VWAP_distance"]
    :scoring     {:P1 "Momentum (returns + RS)"
                  :P2 "Trend (MA structure + ADX)"
                  :P3 "Volume (OBV slope + relative volume + A/D)"
                  :P4 "Risk (volatility + ATR + Amihud)"
                  :P5 "Breadth (universe context)"}
    :examples
    ["python -m scripts.run_strategy bottom-up --market US"
     "python -m scripts.run_strategy bottom-up --market HK"
     "python -m scripts.run_strategy bottom-up --market IN"
     "python -m scripts.run_strategy bottom-up --market US --lookback 365 -v"]}

   :full
   {:description "Combined bottom-up + top-down + convergence"
    :speed       "comprehensive (~2-5min per market)"
    :question    "What should I buy, sell, or hold?"
    :phases      ["load_data" "universe_context" "run_tickers"
                  "rotation" "convergence"
                  "cross_sectional" "reports"]
    :convergence {:STRONG_BUY "Both engines agree on BUY"
                  :BUY        "One BUY + one neutral"
                  :CONFLICT   "Engines disagree (flagged)"
                  :SELL       "Both agree on SELL or SELL + neutral"}
    :examples
    ["python -m scripts.run_strategy full --market US --holdings NVDA,CRWD,CEG"
     "python -m scripts.run_strategy full --market ALL"
     "python -m scripts.run_strategy full --market ALL --lookback 365 -o results/report.json"]}})


(def global-flags
  {:market   {:flag "--market"   :values ["US" "HK" "IN" "ALL"] :default "US"}
   :lookback {:flag "--lookback" :type :int  :unit "calendar days" :default "all available"}
   :holdings {:flag "--holdings" :type :csv  :example "NVDA,CRWD,CEG" :default "none"}
   :output   {:flag "--output"   :alias "-o" :type :path :default "none (stdout only)"}
   :verbose  {:flag "--verbose"  :alias "-v" :effect "debug-level logging"}
   :quiet    {:flag "--quiet"    :alias "-q" :effect "suppress logs, results only"}})


(def data-loading
  {:prerequisite "Strategy scripts cannot run without data"
   :full-backfill
   {:when     "First time / weekly"
    :commands ["python src/ingest_cash.py --market all --period 2y"
               "python src/ingest_options.py --market all --consolidate"
               "python src/db/load_db.py --market all --type cash"
               "python src/db/load_db.py --market all --type options"]}
   :daily-refresh
   {:when     "Weekday mornings"
    :commands ["python src/ingest_cash.py --market all --period 5d"
               "python src/db/load_db.py --market all --type cash"]}
   :single-market
   {:when     "Testing / targeted analysis"
    :commands ["python src/ingest_cash.py --market us --period 2y"
               "python src/db/load_db.py --market us --type cash"]}})


(def workflows
  {:morning-scan
   {:frequency "daily"
    :duration  "~2 minutes"
    :steps     [{:action "Refresh data"
                 :cmd    "python src/ingest_cash.py --market all --period 5d && python src/db/load_db.py --market all --type cash"}
                {:action "Quick sector check"
                 :cmd    "python -m scripts.run_strategy top-down --market US"}
                {:action "Full analysis if needed"
                 :cmd    "python -m scripts.run_strategy full --market US --holdings ..."}]}

   :weekly-review
   {:frequency "weekly"
    :duration  "~5 minutes"
    :steps     [{:action "Full backfill"
                 :cmd    "python src/ingest_cash.py --market all --period 2y && python src/db/load_db.py --market all --type cash"}
                {:action "All-market full pipeline"
                 :cmd    "python -m scripts.run_strategy full --market ALL --holdings ... -o results/weekly.json"}]}

   :research
   {:frequency "ad-hoc"
    :steps     [{:action "Deep bottom-up with verbosity"
                 :cmd    "python -m scripts.run_strategy bottom-up --market HK -v"}
                {:action "Custom top-down params"
                 :cmd    "python -m scripts.run_strategy top-down --market US --quality --quality-weight 0.5 --stocks-per-sector 5"}]}

   :notebook
   {:frequency "ad-hoc"
    :steps     [{:action "Phase-by-phase via Orchestrator class"
                 :code   "(see interactive workflow in comments above)"}]}})


(def pipeline-result
  {:snapshots        "List[dict] — scored tickers sorted by composite score"
   :scored-universe  "Dict[str, DataFrame] — full indicator data per ticker"
   :rankings         "DataFrame — cross-sectional ranks by date"
   :portfolio        "Dict — target_weights, sector_exposure, metadata"
   :signals          "DataFrame — BUY/HOLD/SELL with dates"
   :breadth          "DataFrame — breadth indicators + regime label"
   :sector-rs        "DataFrame — sector RS panel"
   :rotation-result  "RotationResult — sector rotation (US, full mode)"
   :convergence      "MarketSignalResult — convergence merge (US, full mode)"
   :errors           "List[str] — failed tickers"
   :timings          "Dict[str, float] — seconds per phase"
   :methods          {:summary  "One-line summary string"
                      :top-n    "Top N snapshots"
                      :n-tickers "Count of scored tickers"
                      :total-time "Total compute seconds"}})


(def market-config
  {:US {:benchmark "SPY"
        :universe-size "~90 ETFs + single names"
        :engines ["scoring" "rotation"]
        :sector-rs true
        :breadth true}
   :HK {:benchmark "2800.HK"
        :universe-size "~45 tickers"
        :engines ["scoring"]
        :sector-rs false
        :breadth true}
   :IN {:benchmark "NIFTYBEES.NS"
        :universe-size "~50 tickers"
        :engines ["scoring"]
        :sector-rs false
        :breadth true}})