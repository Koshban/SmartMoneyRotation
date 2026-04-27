run_backtest.py
  └─ run_comparison()
       └─ BacktestEngine.run()             # day loop
            ├─ _run_pipeline(day, tickers)  # calls run_pipeline_v2()
            │    └─ refactor.pipeline_v2.run_pipeline_v2(
            │         tradable_frames, bench_df, breadth_df,
            │         market, leadership_frames, portfolio_params,
            │         config={
            │           "scoring_weights": SCORINGWEIGHTS_V2,
            │           "scoring_params":  SCORINGPARAMS_V2,
            │           "signal_params":   SIGNALPARAMS_V2,      ← thresholds live here
            │           "convergence_params": CONVERGENCEPARAMS_V2,
            │           "action_params":   None (not in your configs)
            │         }
            │       )
            │    returns a dict with key "action_table" or "snapshot" or "actions"
            │
            ├─ _extract_actions(output)     # just reads a column from the DataFrame
            │    looks for column: "action_v2" > "action" > "signal"
            │    returns {ticker: "BUY"/"SELL"/"HOLD"/...}
            │
            └─ tracker.process_signals()    # blind executor, no logic


run_pipeline_v2()
  │
  ├─ compute_composite_v2(latest, weights, params)     ← scoring_v2.py (need to see)
  │    produces: scorecomposite_v2
  │
  ├─ leadership boost (hardcoded +10%)                  ← pipeline_v2.py line ~after scoring
  │    scored["scorecomposite_v2"] += 0.10 * leadership_strength
  │    ⚠ leadership_strength = 0.0 for India (no LEADERSHIP_TICKERS in config)
  │
  ├─ apply_signals_v2(scored, params=signal_params)     ← signals_v2.py (need to see)
  │    produces: sigconfirmed_v2, sigexit_v2, sigeffectiveentrymin_v2
  │
  ├─ apply_convergence_v2(signaled, params=convergence_params)  ← signals_v2.py (need to see)
  │    produces: scoreadjusted_v2
  │
  └─ _generate_actions(converged, params=action_params) ← pipeline_v2.py ★ THE PROBLEM ★
       produces: action_v2 = "BUY" / "SELL" / "HOLD" / "STRONG_BUY"
       action_params = None (not in your config)
       ⚠ IGNORES params ENTIRELY — never references it


config_refactor.py
  │
  ├── SCORINGWEIGHTS_V2 ──→ compute_composite_v2()  ✅ reads weights param
  ├── SCORINGPARAMS_V2  ──→ compute_composite_v2()  ✅ reads params param
  │     produces: scorecomposite_v2
  │
  ├── (hardcoded 0.10) ───→ pipeline leadership boost  ❌ ignores config
  │     scored["scorecomposite_v2"] += 0.10 * leadership_strength
  │
  ├── SIGNALPARAMS_V2  ──→ apply_signals_v2()       ✅ reads params param
  │     produces: sigconfirmed_v2, sigexit_v2, sigeffectiveentrymin_v2
  │     ⚠ BUT: gates sector via blocked_sector_regimes
  │
  ├── CONVERGENCEPARAMS_V2 → apply_convergence_v2()  ✅ reads params param
  │     produces: scoreadjusted_v2
  │     ⚠ BUT: rotationrec is never stamped — always defaults to "HOLD"
  │         so convergence is effectively a no-op
  │
  └── ACTIONPARAMS_V2  ──→ _generate_actions()       ❌ DOES NOT EXIST
        receives params= but NEVER reads from it
        ALL thresholds hardcoded: 0.90, 0.76, 0.65, 0.62, etc.
        RE-CHECKS regimes that signals already handled


