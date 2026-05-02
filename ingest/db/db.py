"""
ingest/db/db.py

Database connection utilities.
All table definitions live in schema.py — this file only provides
the connection engine and health check.
"""
import sys
from pathlib import Path

_SRC  = Path(__file__).resolve().parent.parent  # .../src
_ROOT = _SRC.parent                             # .../SmartMoneyRotation
for _p in (str(_ROOT), str(_SRC)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SRC = Path(__file__).resolve().parent.parent
ROOT = SRC.parent
sys.path.insert(0, str(ROOT))

from common.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

LOG = logging.getLogger(__name__)

# ── Connection string ─────────────────────────────────────────
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Singleton engine (reused across the process)
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Return a SQLAlchemy engine (singleton per process).

    Uses connection pooling — safe to call repeatedly.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,
        )
    return _engine


def test_connection() -> bool:
    """Verify DB is reachable and responsive."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                LOG.info(
                    f"DB connection OK → "
                    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
                )
                return True
    except Exception as e:
        LOG.error(f"DB connection FAILED: {e}")
    return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_connection()