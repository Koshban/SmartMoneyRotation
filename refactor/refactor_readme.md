# Smart Money Rotation Refactor v2

## Overview
This project is a refactor of a market-scanning and portfolio-construction workflow that aims to rank tradable instruments, filter them through regime-aware risk controls, and convert them into actionable outputs such as review tables, action labels, and portfolio candidates. The current implementation is organized around `runner_v2`, `pipeline_v2`, strategy submodules, and market-specific input data loaded from parquet files.[1]

The practical purpose of the project is to replace a loosely coupled or difficult-to-debug strategy stack with a clearer, auditable pipeline where each stage has a single responsibility and emits diagnostics that explain why symbols were accepted, downgraded, or skipped.[1][2]

## Goals
The project is trying to achieve four things:

- Build a consistent per-symbol feature frame from raw OHLCV-style market history plus benchmark and breadth context.
- Score symbols using a composite model that blends trend, participation, risk, and market regime.
- Apply signal logic and convergence rules so that strong names are only promoted when the surrounding environment is supportive.
- Produce a final decision layer that can be reviewed by a human, rather than only emitting opaque rankings.

In simpler terms, the system is meant to answer: *which symbols look strong now, which are too risky or too extended, and which ones are worth allocating capital to under the current market regime?*

## Why this project exists
A momentum or rotation strategy is only useful if it can distinguish between:

- Strong trends with healthy participation.
- Weak or low-quality moves caused by noise, gaps, illiquidity, or overstretch.
- Good setups in good market environments versus good-looking setups in bad environments.

The v2 design exists because a modern systematic selection model should not rely on raw price strength alone. It should include relative strength, breadth, volatility regime, participation, and penalties for fragility. That makes the output more conservative, more explainable, and more useful for real portfolio decisions.[1]

Another reason for the refactor is maintainability. A strategy that mixes feature engineering, fallback defaults, scoring, and reporting in an unclear way becomes hard to trust and even harder to debug. The v2 structure is intended to separate those concerns so failures can be localized faster.[2][3]

## Strategy design
The strategy is fundamentally **trend-first and long-biased**.

It prefers names that:
- Are showing strong stock-level relative strength.
- Are supported by sector or thematic leadership.
- Show participation through volume and accumulation-style metrics.
- Are not too volatile, too extended, too illiquid, or too gap-prone.
- Operate in a market environment where breadth and volatility regime do not strongly argue against new longs.

This is not just a ranking model. It is a staged decision system:
1. Compute features.
2. Normalize and standardize them.
3. Score them.
4. Gate them through signal logic.
5. Adjust them for convergence and regime.
6. Turn them into action labels and portfolio candidates.

## High-level architecture
The current architecture is centered around a top-down flow.

### 1. `refactor.runner_v2`
This is the command-line entry point. It is responsible for:
- Loading the market data from parquet.
- Filtering the date range.
- Identifying the market universe, benchmark, tradable symbols, and leadership symbols.
- Invoking `run_pipeline_v2`.
- Building the report output from the returned result bundle.

### 2. `refactor.pipeline_v2`
This is the orchestration layer. It does not own every calculation itself; instead it coordinates the workflow. Its responsibilities include:
- Preparing per-symbol data.
- Building a leadership snapshot.
- Pulling benchmark volatility regime context.
- Pulling breadth context.
- Running scoring, signals, convergence, action generation, and portfolio assembly.
- Producing diagnostics and skipped-symbol outputs.

### 3. `refactor.strategy.adapters_v2`
This is the feature-shape and safety layer. It standardizes the dataframe columns expected by the rest of the strategy and flags whether each row is scoreable.

This module is **not supposed to invent authoritative trading signals**. Its role is to:
- Preserve real upstream values when they exist.
- Add missing columns only to stabilize schema shape.
- Mark rows as unhealthy if critical fields are absent or placeholder-filled.
- Emit diagnostics so downstream failures are explainable.

### 4. Strategy modules
The scoring and decision logic is split across strategy modules such as:
- `regime_v2`
- `scoring_v2`
- `signals_v2`
- `portfolio_v2`

This split is meant to ensure that volatility regime logic, scoring logic, signal gating, and portfolio construction can each evolve independently.

## Intended data flow
The intended data flow is:

1. Load raw market panel from parquet.
2. Split the panel into benchmark, tradable, leadership, and breadth-relevant frames.
3. Compute technical indicators for each frame.
4. Attach benchmark volatility regime information.
5. Attach breadth context.
6. Compute relative-strength and leadership context.
7. Standardize columns via `ensure_columns()`.
8. Mark rows as scoreable or not scoreable.
9. Score scoreable rows using composite logic.
10. Apply signal thresholds.
11. Apply convergence logic.
12. Convert results into action labels such as `STRONG_BUY`, `BUY`, `HOLD`, or `SELL`.
13. Build portfolio candidates and report tables.

If this flow is working correctly, the output should contain meaningful candidates, diagnostics, and a coherent portfolio summary.

## Core concepts
Several concepts are central to the design.

### Relative strength
Relative strength is meant to capture how well a symbol is performing versus its comparison set, not just whether its own chart is rising. In this refactor, `rszscore` is a critical field because it should carry normalized relative-strength information into scoring.

### Breadth
Breadth is meant to represent market participation. Even a strong-looking stock may be a lower-quality opportunity if the surrounding market has weak breadth. The strategy expects both `breadthscore` and `breadthregime` so the scoring and signal logic can react to the broader environment.

### Volatility regime
The benchmark is used to classify the market into volatility states such as calm, volatile, or chaotic. This regime then affects entry thresholds, blocking logic, and position sizing.

### Participation
Participation tries to separate real institutional-quality moves from thin or noisy moves. Metrics such as relative volume, OBV slope, AD line slope, and dollar volume support this layer.

### Risk penalties
The model explicitly penalizes conditions such as:
- Excess volatility.
- Poor liquidity.
- Gap-prone trading behavior.
- Excess extension from reference trend levels.

This means a symbol can look strong on trend but still be downgraded as a low-quality setup.

## Current scoring philosophy
The current v2 scoring philosophy is composite and regime-aware. It uses major pillars such as:
- Trend.
- Participation.
- Risk.
- Regime.

A symbol should score well only if it performs well across these dimensions, not because one metric is extreme while the rest are weak. This reduces the chance of promoting unstable leaders.

The signal layer then applies stricter logic than the raw score alone. High score is necessary but not always sufficient. The system wants confirmation, acceptable regime context, and reasonable extension before escalating a symbol into a high-conviction action.

## Output expectations
When the project is healthy, it should produce the following major outputs:

- A `latest` table containing the prepared latest-row snapshot for each tradable symbol.
- A `scored` table with composite score breakdowns.
- A `signals` table containing signal thresholds and confirmations.
- A `converged` table with final adjusted scores and convergence information.
- An `action_table` with action labels and reasons.
- A `review_table` for human inspection.
- A `selling_exhaustion_table` for special reversal monitoring.
- A `portfolio` bundle with selected names and metadata.
- A `skipped_table` explaining which names were rejected before scoring and why.

These outputs are valuable because they make the system auditable. A human should be able to see not only *what* the model selected, but also *why* it selected or skipped each symbol.

## Current issue
The project is currently blocked by a **feature-availability problem upstream of scoring**.

The recent HK run shows that the pipeline itself is executing, but the tradable and leadership frames reach the adapter without several critical fields populated. In particular, the logs show that `rszscore`, `breadthscore`, and `gaprate20` are missing, while `breadthregime` and `rsregime` remain placeholder-style `unknown` values. Because those fields are required for the scoring model to behave as intended, the pipeline skips all tradable names instead of manufacturing unreliable results.

This is an important distinction:
- The pipeline is not failing because the thresholds are too strict.
- The pipeline is failing because the required features are missing before scoring begins.

## What the logs are telling us
The logs indicate several consistent facts:

- `ensure_columns()` is injecting critical columns instead of preserving authoritative upstream values.
- `rszscore` has no finite values in the leadership and tradable frames.
- `breadthscore` is fully null.
- `breadthregime` remains `unknown` everywhere.
- `gaprate20` is missing on tradable rows.
- As a result, scoreable row count is zero and every tradable symbol is skipped.

This means the adapter rewrite improved visibility but did not and should not solve the underlying issue, because the adapter is only the schema-and-health layer.

## Root-cause hypothesis
The most likely root cause is that the upstream frame-building process is not yet computing or attaching all required context before calling `ensure_columns()`.

The likely missing steps are:
- Relative-strength z-score generation for the chosen comparison universe.
- Breadth metric calculation and breadth regime classification.
- Gap-rate calculation for the symbol frame.
- Possible joining or alignment of context frames by date/index before the pipeline takes the last row.

In other words, the project currently has a downstream strategy shell that expects richer inputs than the upstream builder is actually supplying.

## What success looks like
A healthy run should show the following characteristics in logs and outputs:

- `rszscore` present and finite for leadership and tradable symbols.
- `breadthscore` present and finite where breadth context exists.
- `breadthregime` populated with meaningful states, not just `unknown`.
- `gaprate20` present for tradable rows.
- `scoreable rows` greater than zero.
- Non-empty `scored`, `signals`, `converged`, and `action_table` outputs.
- A portfolio metadata block that reflects real candidate selection rather than an empty fallback state.

## Development principles
This project should continue under a few clear principles:

### 1. Do not hide missing upstream logic
Placeholder defaults should stabilize dataframe shape, not simulate real signals.

### 2. Fail loudly on critical missing inputs
If relative strength, breadth, or gap-rate inputs are missing, the system should make that obvious in logs and skipped tables.

### 3. Keep modules narrow in responsibility
Adapters should normalize shape and diagnostics. They should not become full feature-engineering engines unless that is explicitly their job.

### 4. Preserve explainability
Every major decision should be traceable through diagnostics, not buried in a black box.

### 5. Fix upstream before tuning downstream
There is no value in adjusting thresholds, weights, or action logic until the core feature inputs exist and look sane.

## Recommended debugging sequence
The next debugging steps should follow this order:

1. Identify the exact upstream function that builds each per-symbol frame before `_prepare_frame()` or `ensure_columns()` is called.
2. Confirm where `gaprate20` is supposed to be computed and why it is absent from HK data.
3. Confirm where `rszscore` is supposed to be computed, including the comparison universe and normalization method.
4. Confirm where `breadthscore` and `breadthregime` are supposed to come from and how they are aligned onto symbol frames.
5. Validate that date/index alignment is correct when joining benchmark and breadth context.
6. Re-run the HK pipeline and verify that scoreable rows become nonzero before modifying any scoring thresholds.

## Near-term implementation plan
A practical implementation plan is:

### Phase 1: Trace and inspect
- Inspect the upstream market-frame builder.
- Inspect the breadth-generation logic.
- Inspect the RS normalization logic.
- Inspect the indicator function responsible for `gaprate20`.

### Phase 2: Repair feature generation
- Populate `gaprate20` from symbol OHLC history.
- Compute and attach `rszscore` before adapter validation.
- Compute and attach `breadthscore` and `breadthregime`.
- Ensure these fields survive through the latest-row extraction step.

### Phase 3: Validate health
- Re-run HK.
- Confirm that scoreable rows are present.
- Inspect score distributions and regime distributions.
- Confirm that leadership normalization no longer reports `nan` bounds.

### Phase 4: Revisit model behavior
Only after upstream data is healthy should the project revisit:
- Scoring weights.
- Entry and exit thresholds.
- Convergence adjustments.
- Portfolio concentration rules.

## Project status summary
The project is structurally promising but operationally incomplete.

What is working:
- Runner orchestration.
- Pipeline orchestration.
- Adapter diagnostics and scoreability guardrails.
- Downstream empty-state handling.

What is not yet working:
- Upstream feature wiring for critical scoring inputs.
- Breadth context delivery.
- Relative-strength normalization delivery.
- Gap-rate availability on tradable rows.

## Working understanding for future sessions
This README should be used as the reset context for future debugging sessions.

The most important current takeaway is:

> The project is not blocked by scoring logic or action thresholds. It is blocked because critical upstream features required by the v2 model are not being computed or attached before the adapter and scoring stages.

Any future work should start by checking the upstream builder path first, not by changing downstream thresholds or action logic.