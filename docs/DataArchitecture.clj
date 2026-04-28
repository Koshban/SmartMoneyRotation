;; ═══════════════════════════════════════════════════════════════════════════
;; DataArchitecture.clj — Data Pipeline: Ingestion, Storage & Read Layers
;; ═══════════════════════════════════════════════════════════════════════════
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                        SOURCE SELECTION LOGIC                          │
;; │                                                                        │
;; │  Historical backfill (>5 days)   →  yfinance   (fast, bulk)            │
;; │  Recent refresh     (≤5 days)    →  IBKR       (fresh, accurate)       │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                       END-TO-END DATA FLOW                             │
;; │                                                                        │
;; │  ingest_cash.py     →  data/{market}_cash.parquet         (cumulative) │
;; │                     →  data/universe_ohlcv.parquet         (combined)   │
;; │                                                                        │
;; │  ingest_options.py  →  data/options/{market}/*.csv         (per-ticker) │
;; │                          ↓  --consolidate                              │
;; │                        data/{market}_options.parquet                    │
;; │                          ↓                                             │
;; │  load_db.py         →  PostgreSQL                          (upsert)    │
;; │                          ↓                                             │
;; │  loader.py          →  compute pipeline reads              (DB-first)  │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                       STORAGE ARCHITECTURE                             │
;; │                                                                        │
;; │  ┌──────────────────────────────────────────────────────────────────┐  │
;; │  │  PostgreSQL (CANONICAL)                                          │  │
;; │  │  ─────────────────────                                           │  │
;; │  │  • Accumulates via INSERT ... ON CONFLICT DO UPDATE              │  │
;; │  │  • Full history retained (no trimming)                           │  │
;; │  │  • Primary read source for loader.py                             │  │
;; │  └──────────────────────────────────────────────────────────────────┘  │
;; │                                                                        │
;; │  ┌──────────────────────────────────────────────────────────────────┐  │
;; │  │  Parquet (CUMULATIVE ROLLING CACHE)                              │  │
;; │  │  ─────────────────────────────────                               │  │
;; │  │  • ingest_cash.py appends new rows, deduplicates, saves back     │  │
;; │  │  • Rolling window: 450 calendar days (≈310 trading days)         │  │
;; │  │  • Enough for EMA(200) + convergence runway                      │  │
;; │  │  • Fallback read source when DB is unavailable                   │  │
;; │  │  • Also feeds load_db.py for DB population                       │  │
;; │  └──────────────────────────────────────────────────────────────────┘  │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │  INGEST LAYER (decides source)        STORAGE          READ LAYER      │
;; │  ─────────────────────────────        ───────          ──────────      │
;; │                                                                        │
;; │  ingest_cash.py                                                        │
;; │    ├─ yfinance (bulk)          →  parquet (cumul)  ←  loader.py        │
;; │    └─ IBKR    (recent)         →  parquet (cumul)  ←   (DB-first)     │
;; │                                        ↓                               │
;; │  load_db.py                    →  PostgreSQL (canon)←  loader.py       │
;; │                                                                        │
;; │  ingest_options.py                                                     │
;; │    ├─ yfinance (US backfill)   →  csv / parquet    ←  loader.py        │
;; │    ├─ IBKR    (US recent)      →  csv / parquet    ←   (reads whatever │
;; │    └─ IBKR    (HK)             →  csv / parquet    ←    was ingested)  │
;; │                                        ↓                               │
;; │  load_db.py                    →  PostgreSQL        ←  loader.py       │
;; │                                                                        │
;; │  Pipeline (orchestrator/runner) reads ONLY from loader.py              │
;; │  loader.py calls check_minimum_history() before indicators compute     │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                    loader.py READ PRIORITY                             │
;; │                                                                        │
;; │   1. PostgreSQL ── canonical store, full accumulated history            │
;; │          ↓ (if DB unavailable or ticker not found)                     │
;; │   2. Parquet ───── cumulative rolling cache, works offline             │
;; │          ↓ (if parquet unavailable or ticker not found)                │
;; │   3. yfinance ──── live download, last resort                          │
;; │                                                                        │
;; │   Minimum history guard:                                               │
;; │     check_minimum_history() raises ValueError if median < 60 bars      │
;; │     check_minimum_history() warns            if median < 200 bars      │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │                  CUMULATIVE PARQUET UPSERT LOGIC                       │
;; │                  (ingest_cash.py → upsert_parquet)                     │
;; │                                                                        │
;; │   1. Read existing {market}_cash.parquet  (if it exists)               │
;; │   2. Concatenate with newly fetched rows                               │
;; │   3. Deduplicate by (symbol, date)  — last fetch wins                  │
;; │   4. Trim rows older than 450 calendar days                            │
;; │   5. Sort by (symbol, date) and save back                              │
;; │                                                                        │
;; │   Result: a 2-day daily fetch accumulates to full history over time.   │
;; │   A 2y backfill populates the full window in one shot.                 │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; STEP-BY-STEP COMMANDS
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── STEP 1 : Download Cash Data → Parquet (cumulative) ────────────────────
;;
;;   python src/ingest_cash.py --market all --period 2y
;;
;;   Creates / accumulates:
;;     data/us_cash.parquet
;;     data/hk_cash.parquet
;;     data/in_cash.parquet
;;     data/universe_ohlcv.parquet          (combined superset)
;;
;;   Per-market variant:
;;     python src/ingest_cash.py --market us --period 2y
;;     python src/ingest_cash.py --market hk --period 2y
;;     python src/ingest_cash.py --market in --period 2y
;;
;;   Days-based variant (exact calendar day count):
;;     python src/ingest_cash.py --market all --days 365
;;
;;
;; ── STEP 2 : Download Options Data → CSV → Parquet ────────────────────────
;;
;;   python src/ingest_options.py --market us
;;   python src/ingest_options.py --market hk
;;
;;   Creates (raw per-ticker):
;;     data/options/us/*.csv
;;     data/options/hk/*.csv
;;
;;   Consolidate into parquet:
;;     python src/ingest_options.py --market us --consolidate
;;     python src/ingest_options.py --market hk --consolidate
;;
;;   Creates:
;;     data/us_options.parquet
;;     data/hk_options.parquet
;;
;;
;; ── STEP 3 : Load Parquet → PostgreSQL (upsert, idempotent) ──────────────
;;
;;   python src/db/load_db.py --market all --type cash
;;
;;   Reads:
;;     data/us_cash.parquet       →  us_cash       table
;;     data/hk_cash.parquet       →  hk_cash       table
;;     data/in_cash.parquet       →  in_cash        table
;;
;;   python src/db/load_db.py --market all --type options
;;
;;   Reads:
;;     data/options/us/*.csv      →  us_options     table
;;     data/options/hk/*.csv      →  hk_options     table
;;
;;   All loads use INSERT ... ON CONFLICT DO UPDATE (safe to re-run).
;;
;;   Per-market variant:
;;     python src/db/load_db.py --market us --type cash
;;     python src/db/load_db.py --market us --type options
;;
;;   Check status:
;;     python src/db/load_db.py --status
;;
;;
;; ── STEP 4 : Compute Pipeline Reads via loader.py ─────────────────────────
;;
;;   loader.py is the SINGLE read interface for the compute pipeline.
;;
;;   Resolution order (source="auto"):
;;     1. PostgreSQL   (canonical — full accumulated history)
;;     2. Parquet      (fallback  — cumulative rolling cache)
;;     3. yfinance     (last-resort — live download)
;;
;;   History safety check (called by runner before indicators):
;;     check_minimum_history(universe_frames)
;;       - Raises ValueError if median ticker < 60 bars
;;       - Logs warning      if median ticker < 200 bars
;;       - Logs OK           if median ticker >= 200 bars
;;
;;   The pipeline / orchestrator / runner imports loader.py and never
;;   calls ingest_cash.py, ingest_options.py, or load_db.py directly.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; QUICK-START  (initial full backfill, all markets)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   # 1. Cash equities — 2-year backfill populates cumulative parquet
;;   python src/ingest_cash.py --market all --period 2y
;;
;;   # 2. Options chains
;;   python src/ingest_options.py --market us
;;   python src/ingest_options.py --market hk
;;   python src/ingest_options.py --market us --consolidate
;;   python src/ingest_options.py --market hk --consolidate
;;
;;   # 3. Load everything into PostgreSQL (canonical store)
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;;   # 4. Run the pipeline (reads from loader.py → DB → parquet → yfinance)
;;   python src/run_pipeline.py
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; DAILY REFRESH  (incremental, accumulates into existing stores)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   # 1. Fetch last 2 days of cash (IBKR for US/HK, yfinance for India)
;;   #    Appends to existing parquet → cumulative store grows
;;   python src/ingest_cash.py --market all --days 2
;;
;;   # 2. Refresh today's options chains (appends to per-ticker CSVs)
;;   python src/ingest_options.py --market us
;;   python src/ingest_options.py --market hk
;;
;;   # 3. Upsert into DB (ON CONFLICT DO UPDATE — safe to re-run)
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;;   # The parquet files now have N+2 days of data.
;;   # The DB now has the full accumulated history.
;;   # Tomorrow's run adds another 2 days, deduplicates, and so on.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; DAILY REFRESH — WHAT HAPPENS UNDER THE HOOD
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   Example: HK market, day 150 of daily runs (initial backfill was 2y)
;;
;;   ingest_cash.py --market hk --days 2
;;     │
;;     ├─ IBKR fetches 2 new bars per symbol
;;     │
;;     ├─ upsert_parquet():
;;     │    ├─ Reads hk_cash.parquet           (298 trading days)
;;     │    ├─ Appends 2 new bars
;;     │    ├─ Deduplicates (symbol, date)      — last fetch wins
;;     │    ├─ Trims rows > 450 calendar days   — rolling window
;;     │    └─ Saves back                       (300 trading days)
;;     │
;;     └─ universe_ohlcv.parquet rebuilt from merged per-market files
;;
;;   load_db.py --market hk --type cash
;;     │
;;     ├─ Reads hk_cash.parquet                (300 trading days)
;;     └─ Upserts all rows into hk_cash table  (ON CONFLICT DO UPDATE)
;;         └─ DB now has full 300+ trading days
;;
;;   runner_v2 --market hk
;;     │
;;     ├─ loader.load_ohlcv("0700.HK", source="auto")
;;     │    └─ Tries DB first → gets 300 bars from hk_cash table  ✓
;;     │
;;     ├─ check_minimum_history(universe_frames)
;;     │    └─ median=300 bars → OK  ✓
;;     │
;;     └─ RSI(14), EMA(50), SMA(50), EMA(200) all compute correctly  ✓
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; RECOVERY SCENARIOS
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   Scenario: DB is down or empty
;;     loader.py falls back to parquet (cumulative, 450-day window)
;;     Indicators still compute — parquet has enough history
;;
;;   Scenario: Parquet files deleted
;;     loader.py reads from DB (full history)
;;     Next ingest_cash.py run rebuilds parquet from scratch
;;     Run with --period 2y to repopulate full window immediately
;;
;;   Scenario: Both DB and parquet empty (fresh install)
;;     loader.py falls back to yfinance (live download)
;;     check_minimum_history() will likely FAIL (< 60 bars)
;;     Fix: run initial backfill first
;;       python src/ingest_cash.py --market all --period 2y
;;       python src/db/load_db.py --market all --type cash
;;
;;   Scenario: DB has full history, parquet was overwritten with 5d
;;     loader.py reads from DB first — no data loss
;;     This was the original bug — now fixed by:
;;       a) DB-first read order in loader.py
;;       b) Cumulative parquet upsert in ingest_cash.py
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; FILE TREE (generated artifacts)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   data/
;;   ├── universe_ohlcv.parquet          ← combined cash (all markets)
;;   ├── us_cash.parquet                 ← US equities OHLCV  (cumulative)
;;   ├── hk_cash.parquet                 ← HK equities OHLCV  (cumulative)
;;   ├── in_cash.parquet                 ← India equities OHLCV (cumulative)
;;   ├── us_options.parquet              ← US options (consolidated)
;;   ├── hk_options.parquet              ← HK options (consolidated)
;;   └── options/
;;       ├── us/
;;       │   ├── AAPL.csv               ← per-ticker, append-safe
;;       │   ├── MSFT.csv
;;       │   └── ...
;;       └── hk/
;;           ├── 0700_HK.csv
;;           └── ...
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; DATABASE TABLES
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   us_cash      (date, symbol, open, high, low, close, volume)
;;                 UNIQUE(date, symbol)
;;
;;   hk_cash      (date, symbol, open, high, low, close, volume)
;;                 UNIQUE(date, symbol)
;;
;;   in_cash      (date, symbol, open, high, low, close, volume)
;;                 UNIQUE(date, symbol)
;;
;;   us_options   (date, symbol, expiry, strike, opt_type,
;;                 bid, ask, last, volume, oi, iv,
;;                 delta, gamma, theta, vega, rho,
;;                 underlying_price, dte, source)
;;                 UNIQUE(date, symbol, expiry, strike, opt_type)
;;
;;   hk_options   (date, symbol, expiry, strike, opt_type,
;;                 bid, ask, last, volume, oi, iv,
;;                 delta, gamma, theta, vega, rho,
;;                 underlying_price, dte, source)
;;                 UNIQUE(date, symbol, expiry, strike, opt_type)
;;
;;   All tables use INSERT ... ON CONFLICT DO UPDATE (idempotent upsert).
;;   load_db.py --status shows row counts and date ranges for all tables.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; KEY DESIGN DECISIONS
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   1. DB is canonical, parquet is cache
;;      The DB accumulates indefinitely via upsert.  Parquet is a
;;      rolling 450-day window — enough for all indicators but not
;;      unbounded.  loader.py reads DB first for this reason.
;;
;;   2. Cumulative parquet (not overwrite)
;;      ingest_cash.py uses upsert_parquet() which reads the existing
;;      file, appends new rows, deduplicates, trims, and saves back.
;;      A --days 2 fetch never destroys existing history.
;;
;;   3. 450-day rolling window
;;      450 calendar days ≈ 310 trading days.  This covers EMA(200)
;;      with ~110 bars of convergence runway.  Configurable via
;;      MAX_PARQUET_CALENDAR_DAYS in ingest_cash.py.
;;
;;   4. check_minimum_history() guard
;;      Called by runner_v2 after loading data and before computing
;;      indicators.  Hard-fails at 60 bars, warns at 200 bars.
;;      Prevents silent garbage output from under-converged EMAs.
;;
;;   5. Market codes: us, hk, in
;;      India uses market code "in" (not "india") throughout the
;;      pipeline.  File: in_cash.parquet.  Table: in_cash.
;;
;;   6. Options: US and HK only
;;      yfinance provides US options.  IBKR provides US + HK options.
;;      India options are not currently supported.
;;
;; ═══════════════════════════════════════════════════════════════════════════

(ns data-architecture
  "Documentation namespace — describes the data pipeline architecture.
   This file is not executed; it exists as living documentation.")

(def architecture
  {:source-selection
   {:historical {:window "> 5 days"  :source "yfinance" :reason "fast, bulk downloads"}
    :recent     {:window "<= 5 days" :source "IBKR"     :reason "fresh, accurate"}}

   :storage-model
   {:postgres {:role        "canonical store"
               :retention   "unbounded (full history)"
               :write       "INSERT ... ON CONFLICT DO UPDATE"
               :read-order  1}
    :parquet  {:role        "cumulative rolling cache"
               :retention   "450 calendar days (~310 trading days)"
               :write       "upsert_parquet() — read, append, dedup, trim, save"
               :read-order  2}
    :yfinance {:role        "last-resort live fallback"
               :read-order  3}}

   :ingest-layer
   {:cash    {:script "src/ingest_cash.py"
              :sources {:yfinance "bulk historical (> 5 days)"
                        :ibkr     "recent refresh (<= 5 days)"}
              :save-mode "cumulative upsert (not overwrite)"
              :outputs ["data/us_cash.parquet"
                        "data/hk_cash.parquet"
                        "data/in_cash.parquet"
                        "data/universe_ohlcv.parquet"]}
    :options {:script "src/ingest_options.py"
              :sources {:yfinance "US options"
                        :ibkr     "US + HK options"}
              :markets-supported ["us" "hk"]
              :outputs-raw    "data/options/{market}/*.csv"
              :outputs-parquet ["data/us_options.parquet"
                                "data/hk_options.parquet"]}}

   :storage-layer
   {:parquet   {:location "data/"
                :role     "cumulative rolling cache + input for load_db.py"}
    :postgres  {:loader   "src/db/load_db.py"
                :role     "canonical read store (DB-first in loader.py)"}}

   :read-layer
   {:loader {:script "src/db/loader.py"
             :role   "single read interface for the compute pipeline"
             :fallback-chain ["PostgreSQL" "Parquet" "yfinance"]
             :history-guard {:hard-min  60
                             :warn-min  200
                             :function  "check_minimum_history()"}}}

   :steps
   [{:step 1 :name "Download cash → parquet (cumulative)"
     :cmd  "python src/ingest_cash.py --market all --period 2y"}
    {:step 2 :name "Download options → csv"
     :cmd  ["python src/ingest_options.py --market us"
            "python src/ingest_options.py --market hk"]}
    {:step 3 :name "Load parquet/csv → PostgreSQL (upsert)"
     :cmd  ["python src/db/load_db.py --market all --type cash"
            "python src/db/load_db.py --market all --type options"]}
    {:step 4 :name "Pipeline reads via loader.py (DB → parquet → yfinance)"
     :cmd  "python src/run_pipeline.py"}]

   :daily-refresh
   [{:step 1 :name "Incremental cash (appends to cumulative parquet)"
     :cmd  "python src/ingest_cash.py --market all --days 2"}
    {:step 2 :name "Today's options"
     :cmd  ["python src/ingest_options.py --market us"
            "python src/ingest_options.py --market hk"]}
    {:step 3 :name "Upsert into DB"
     :cmd  ["python src/db/load_db.py --market all --type cash"
            "python src/db/load_db.py --market all --type options"]}]

   :db-tables
   {:cash    {:tables ["us_cash" "hk_cash" "in_cash"]
              :key    "(date, symbol)"
              :cols   ["date" "symbol" "open" "high" "low" "close" "volume"]}
    :options {:tables ["us_options" "hk_options"]
              :key    "(date, symbol, expiry, strike, opt_type)"
              :cols   ["date" "symbol" "expiry" "strike" "opt_type"
                       "bid" "ask" "last" "volume" "oi" "iv"
                       "delta" "gamma" "theta" "vega" "rho"
                       "underlying_price" "dte" "source"]}}})