# Usage Guide

## Table of Contents
- [Runner v2 — Live Pipeline](#runner-v2--live-pipeline)
- [Backtester — run_backtest.py](#backtester--run_backtestpy)

---

## Runner v2 — Live Pipeline

### Arguments

| Flag               | Default | Description                              |
|--------------------|---------|------------------------------------------|
| `--market`         | `US`    | Market to run: `US`, `HK`, or `IN`       |
| `--start-date`     | `None`  | Inclusive start date `YYYY-MM-DD`         |
| `--end-date`       | `None`  | Inclusive end date `YYYY-MM-DD`           |
| `--parquet-path`   | `None`  | Explicit path to parquet data file        |
| `--print-report`   |         | Print plain-text v2 report to stdout      |
| `-v`, `--verbose`  |         | Enable verbose / debug logging            |

### Examples

```bash
# ── Basics ────────────────────────────────────────────────────────

# Default: US market, auto dates, minimal output
python cash/phase2/runner_v2.py

# US market with explicit date range
python cash/phase2/runner_v2.py --market US --start-date 2024-01-02 --end-date 2025-04-25

# ── Reports & Logging ────────────────────────────────────────────

# Print the plain-text report to terminal
python cash/phase2/runner_v2.py --market US --print-report

# Verbose logging + report (recommended first smoke test)
python cash/phase2/runner_v2.py --market US --print-report -v

# Full flags: date range + verbose + report
python cash/phase2/runner_v2.py --market US --start-date 2024-06-01 --end-date 2025-04-25 --print-report -v

# ── Custom Data Path ─────────────────────────────────────────────

# Point to a specific parquet file
python cash/phase2/runner_v2.py --market US --parquet-path data/us_cash.parquet

# ── Other Markets ────────────────────────────────────────────────

# Hong Kong
python cash/phase2/runner_v2.py --market HK --print-report -v

# India with date range
python cash/phase2/runner_v2.py --market IN --parquet-path data/in_cash.parquet --print-report -v

# ── Single Day Run ───────────────────────────────────────────────

# Run for a single trading day
python cash/phase2/runner_v2.py --market US --start 2025-04-25 --end 2025-04-25 --print-report -v

# ── Full run: both models ───────────────────────────────────
python run_combined.py --market US
python run_combined.py --market US --holdings NVDA,CRWD,CEG -v
python run_combined.py --market IN
python run_combined.py --market HK --date 2025-04-25

# ── With HTML report ────────────────────────────────────────
python run_combined.py --market US --html results/report.html --open

# ── Run only one model ──────────────────────────────────────
python run_combined.py --market US --skip-phase1    # Phase 2 only
python run_combined.py --market US --skip-phase2    # Phase 1 only

# ── Just combine existing signals (no model re-run) ────────
python run_combined.py --market US --combine-only

# ── Custom model commands ───────────────────────────────────
python run_combined.py --market US \
    --p1-cmd "python -m scripts.run_strategy full" \
    --p2-cmd "python cash/phase2/runner_v2.py"