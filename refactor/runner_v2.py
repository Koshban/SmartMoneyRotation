from __future__ import annotations
import pandas as pd
from .common.market_config_v2 import get_market_config_v2
from .pipeline_v2 import run_pipeline_v2
from .report_v2 import build_report_v2, to_text_v2
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from common.config import LOGS_DIR


logger = logging.getLogger("refactor.runner_v2")


def setup_logging(verbose: bool = False) -> Path:
    log_dir = Path(LOGS_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"runner_v2_{ts}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="w", encoding="utf-8"),
        ],
        force=True,
    )
    return log_file


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--market", default="US")
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def run_strategy_v2(market: str, universe_frames: dict[str, pd.DataFrame], bench_df: pd.DataFrame, breadth_df: pd.DataFrame | None = None) -> dict:
    cfg = get_market_config_v2(market)
    tradable = set(cfg['tradable_universe'])
    leadership = set(cfg['leadership_universe'])
    tradable_frames = {k: v for k, v in universe_frames.items() if k in tradable}
    leadership_frames = {k: v for k, v in universe_frames.items() if k in leadership}
    result = run_pipeline_v2(
        tradable_frames=tradable_frames,
        leadership_frames=leadership_frames,
        bench_df=bench_df,
        breadth_df=breadth_df,
        market=cfg['market'],
        portfolio_params={
            'max_positions': cfg['max_positions'],
            'max_sector_weight': cfg['max_sector_weight'],
            'max_theme_names': cfg['max_theme_names'],
        },
    )
    result['market_config_v2'] = cfg
    result['tradable_universe'] = sorted(tradable)
    result['leadership_universe'] = sorted(leadership)
    report = build_report_v2(result)
    result['report_v2'] = report
    result['report_text_v2'] = to_text_v2(report)
    return result

def main(argv=None):
    args = build_parser().parse_args(argv)
    log_file = setup_logging(args.verbose)

    logger.info("Starting runner_v2")
    logger.info("Log file: %s", log_file)
    logger.info("Market: %s", args.market)
    logger.info("Start date: %s", args.start_date)
    logger.info("End date: %s", args.end_date)

    result = run_pipeline_v2(
        market=args.market,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    logger.info("runner_v2 completed")
    return result


if __name__ == "__main__":
    main()