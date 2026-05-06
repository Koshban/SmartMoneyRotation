"""
Centralized logging configuration for ingest / batch scripts.

Usage:
    from common.logging_setup import setup_logging
    log_path = setup_logging("iv_history", verbose=args.verbose)
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from common.config import LOGS_DIR


def setup_logging(
    name: str = "run",
    verbose: bool = False,
    log_file: str | None = None,
) -> str:
    """
    Configure the root logger with a stdout handler and a file handler.

    Parameters
    ----------
    name :
        Short tag used in the default filename, e.g. ``"iv_history"`` →
        ``LOGS_DIR/iv_history_20260504_113725.log``.
    verbose :
        If True, set level to DEBUG; otherwise INFO.
    log_file :
        Explicit path (absolute or relative). Relative paths resolve
        inside ``LOGS_DIR``. If None, a timestamped name is generated.

    Returns
    -------
    str
        The resolved absolute path of the log file.
    """
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s  %(levelname)-8s  %(name)-28s  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    if log_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_path / f"{name}_{ts}.log")
    else:
        lf = Path(log_file)
        if not lf.is_absolute():
            log_file = str(logs_path / lf)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, mode="w", encoding="utf-8"),
    ]

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
        force=True,    # wipe any prior basicConfig from imported modules
    )

    # Quiet noisy third-party loggers
    for noisy in ("urllib3", "yfinance", "asyncio", "peewee", "matplotlib"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root = logging.getLogger()
    root.info("logging initialised → %s  (level=%s)",
              log_file, logging.getLevelName(level))
    return log_file