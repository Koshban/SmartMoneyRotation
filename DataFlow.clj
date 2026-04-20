ingest_cash.py → data/universe_ohlcv.parquet
                          ↓
src/db/loader.py  →  {ticker: OHLCV DataFrame}
                          ↓
orchestrator.py Phase 0 (load_data)
         ↓
Phase 1: compute_all_breadth()     → self._breadth
         compute_all_sector_rs()   → self._sector_rs
         breadth_to_pillar_scores()→ self._breadth_scores
         ↓
Phase 2: runner.run_batch()        → self._ticker_results
         results_to_scored_universe() → self._scored_universe
         results_to_snapshots()    → self._snapshots
         ↓
Phase 3a: compute_all_rankings()   → self._rankings
Phase 3b: build_portfolio()        → self._portfolio
          _enrich_snapshots_with_allocations()
Phase 3c: compute_all_signals()    → self._signals
          _enrich_snapshots_with_signals()
         ↓
Phase 4: _build_report_input()     → dict (correct keys ✓)
         build_report(dict)        → self._recommendation_report
         ↓
PipelineResult assembled
         ↓
main.py: save_text / save_html / print_report
         build_rebalance_plan() (if positions provided)