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
;; │  ingest_cash.py     →  data/universe_ohlcv.parquet        (download)   │
;; │                                                                        │
;; │  ingest_options.py  →  data/options/{market}/*.csv         (download)   │
;; │                          ↓  --consolidate                              │
;; │                        data/{market}_options.parquet                    │
;; │                          ↓                                             │
;; │  load_db.py         →  PostgreSQL                          (load)      │
;; │                          ↓                                             │
;; │  loader.py          →  compute pipeline reads              (read)      │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ┌─────────────────────────────────────────────────────────────────────────┐
;; │  INGEST LAYER (decides source)        STORAGE          READ LAYER      │
;; │  ─────────────────────────────        ───────          ──────────      │
;; │                                                                        │
;; │  ingest_cash.py                                                        │
;; │    ├─ yfinance (bulk)          →  parquet / DB  ←  loader.py           │
;; │    └─ IBKR    (recent)         →  parquet / DB  ←   (source-agnostic) │
;; │                                                                        │
;; │  ingest_options.py                                                     │
;; │    ├─ yfinance (US backfill)   →  parquet / DB  ←  loader.py           │
;; │    ├─ IBKR    (US recent)      →  parquet / DB  ←   (reads whatever   │
;; │    ├─ IBKR    (HK)             →  parquet / DB  ←    was ingested)    │
;; │    └─ IBKR    (India)          →  parquet / DB  ←                     │
;; │                                                                        │
;; │  Pipeline (orchestrator/runner) reads ONLY from loader.py              │
;; └─────────────────────────────────────────────────────────────────────────┘
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; STEP-BY-STEP COMMANDS
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;
;; ── STEP 1 : Download Cash Data → Parquet ─────────────────────────────────
;;
;;   python src/ingest_cash.py --market all --period 2y
;;
;;   Creates:
;;     data/us_cash.parquet
;;     data/hk_cash.parquet
;;     data/india_cash.parquet
;;     data/universe_ohlcv.parquet          (combined superset)
;;
;;   Per-market variant:
;;     python src/ingest_cash.py --market us    --period 2y
;;     python src/ingest_cash.py --market hk    --period 2y
;;     python src/ingest_cash.py --market india --period 2y
;;
;;
;; ── STEP 2 : Download Options Data → CSV → Parquet ────────────────────────
;;
;;   python src/ingest_options.py --market all
;;
;;   Creates (raw):
;;     data/options/us/*.csv
;;     data/options/hk/*.csv
;;     data/options/india/*.csv
;;
;;   Consolidate into parquet:
;;     python src/ingest_options.py --market all --consolidate
;;
;;   Creates:
;;     data/us_options.parquet
;;     data/hk_options.parquet
;;     data/india_options.parquet
;;
;;   Per-market variant:
;;     python src/ingest_options.py --market us    --consolidate
;;     python src/ingest_options.py --market hk    --consolidate
;;     python src/ingest_options.py --market india --consolidate
;;
;;
;; ── STEP 3 : Load Parquet → PostgreSQL ────────────────────────────────────
;;
;;   python src/db/load_db.py --market all --type cash
;;
;;   Reads:
;;     data/us_cash.parquet       →  us_cash       table
;;     data/hk_cash.parquet       →  hk_cash       table
;;     data/india_cash.parquet    →  india_cash     table
;;
;;   python src/db/load_db.py --market all --type options
;;
;;   Reads:
;;     data/us_options.parquet    →  us_options     table
;;     data/hk_options.parquet    →  hk_options     table
;;     data/india_options.parquet →  india_options   table
;;
;;   Per-market variant:
;;     python src/db/load_db.py --market us --type cash
;;     python src/db/load_db.py --market us --type options
;;
;;
;; ── STEP 4 : Compute Pipeline Reads via loader.py ─────────────────────────
;;
;;   loader.py is the SINGLE read interface for the compute pipeline.
;;
;;   Resolution order (fallback chain):
;;     1. PostgreSQL   (preferred — fastest, pre-loaded)
;;     2. Parquet      (fallback  — local file)
;;     3. yfinance     (last-resort — live download)
;;
;;   The pipeline / orchestrator / runner imports loader.py and never
;;   calls ingest_cash.py, ingest_options.py, or load_db.py directly.
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; QUICK-START  (full refresh, all markets)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   # 1. Cash equities
;;   python src/ingest_cash.py --market all --period 2y
;;
;;   # 2. Options chains
;;   python src/ingest_options.py --market all --consolidate
;;
;;   # 3. Load everything into PostgreSQL
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;;   # 4. Run the pipeline (reads from loader.py automatically)
;;   python src/run_pipeline.py
;;
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; DAILY REFRESH  (recent data only, IBKR source)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   # Refresh last 5 days of cash (uses IBKR automatically)
;;   python src/ingest_cash.py --market all --period 5d
;;
;;   # Refresh today's options chains
;;   python src/ingest_options.py --market all --consolidate
;;
;;   # Reload into DB (upsert / replace)
;;   python src/db/load_db.py --market all --type cash
;;   python src/db/load_db.py --market all --type options
;;
;; ═══════════════════════════════════════════════════════════════════════════
;; FILE TREE (generated artifacts)
;; ═══════════════════════════════════════════════════════════════════════════
;;
;;   data/
;;   ├── universe_ohlcv.parquet          ← combined cash (all markets)
;;   ├── us_cash.parquet                 ← US equities OHLCV
;;   ├── hk_cash.parquet                 ← HK equities OHLCV
;;   ├── india_cash.parquet              ← India equities OHLCV
;;   ├── us_options.parquet              ← US options (consolidated)
;;   ├── hk_options.parquet              ← HK options (consolidated)
;;   ├── india_options.parquet           ← India options (consolidated)
;;   └── options/
;;       ├── us/
;;       │   ├── AAPL_2026-05-15.csv
;;       │   ├── MSFT_2026-06-19.csv
;;       │   └── ...
;;       ├── hk/
;;       │   └── ...
;;       └── india/
;;           └── ...
;;
;; ═══════════════════════════════════════════════════════════════════════════

(ns data-architecture
  "Documentation namespace — describes the data pipeline architecture.
   This file is not executed; it exists as living documentation.")

(def architecture
  {:source-selection
   {:historical {:window "> 5 days"  :source "yfinance" :reason "fast, bulk downloads"}
    :recent     {:window "<= 5 days" :source "IBKR"     :reason "fresh, accurate"}}

   :ingest-layer
   {:cash    {:script "src/ingest_cash.py"
              :sources {:yfinance "bulk historical"
                        :ibkr     "recent refresh"}
              :outputs ["data/us_cash.parquet"
                        "data/hk_cash.parquet"
                        "data/india_cash.parquet"
                        "data/universe_ohlcv.parquet"]}
    :options {:script "src/ingest_options.py"
              :sources {:yfinance "US backfill"
                        :ibkr     "US recent, HK, India"}
              :outputs-raw    "data/options/{market}/*.csv"
              :outputs-parquet ["data/us_options.parquet"
                                "data/hk_options.parquet"
                                "data/india_options.parquet"]}}

   :storage-layer
   {:parquet   {:location "data/"         :role "intermediate + fallback"}
    :postgres  {:loader   "src/db/load_db.py" :role "primary read store"}}

   :read-layer
   {:loader {:script "src/loader.py"
             :role   "single read interface for the compute pipeline"
             :fallback-chain ["PostgreSQL" "Parquet" "yfinance"]}}

   :steps
   [{:step 1 :name "Download cash → parquet"
     :cmd  "python src/ingest_cash.py --market all --period 2y"}
    {:step 2 :name "Download options → csv → parquet"
     :cmd  "python src/ingest_options.py --market all --consolidate"}
    {:step 3 :name "Load parquet → PostgreSQL"
     :cmd  ["python src/db/load_db.py --market all --type cash"
            "python src/db/load_db.py --market all --type options"]}
    {:step 4 :name "Pipeline reads via loader.py"
     :cmd  "python src/run_pipeline.py"}]})