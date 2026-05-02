"""
test_connections.py
-------------------
Smoke-test connectivity to PostgreSQL and IBKR TWS/Gateway.

Run from project root:
    python tests/test_connections.py
    python tests/test_connections.py --db-only
    python tests/test_connections.py --ibkr-only
"""

import sys
import os

# ── Make sure project root is on the path ──────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# ── PostgreSQL test ────────────────────────────────────────────

def test_postgres() -> bool:
    """Test PostgreSQL connection, table creation, and a round-trip query."""
    print("\n" + "=" * 55)
    print("   POSTGRESQL CONNECTION TEST")
    print("=" * 55)

    try:
        from common.config import DB_URL

        # Mask password in display
        masked = DB_URL
        try:
            pre, rest = DB_URL.split("://", 1)
            user_pass, host_part = rest.split("@", 1)
            user = user_pass.split(":")[0]
            masked = f"{pre}://{user}:****@{host_part}"
        except Exception:
            pass
        print(f"  URL          : {masked}")

    except Exception as e:
        print(f"  [FAIL] Could not load config: {e}")
        return False

    try:
        from ingest.db import test_connection, init_db, get_engine
        from sqlalchemy import text
    except Exception as e:
        print(f"  [FAIL] Could not import db module: {e}")
        return False

    # 1. Basic connectivity
    if not test_connection():
        print("  [FAIL] Connection : FAILED")
        print("\n  Troubleshooting:")
        print("    1. Is PostgreSQL running?")
        print("    2. Does the 'smart_rotation' database exist?")
        print("       Run:  psql -U postgres -p <your_port>")
        print("             CREATE DATABASE smart_rotation;")
        print("    3. Check user/password in common/credentials.py")
        return False
    print("  [OK] Connection  : SUCCESS")

    # 2. Create / verify tables
    try:
        init_db()
    except Exception as e:
        print(f"  [FAIL] Schema creation: {e}")
        return False

    # 3. Round-trip test: insert and read back
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text(
                "INSERT INTO daily_prices (symbol, date, close, volume) "
                "VALUES ('_TEST_', '2000-01-01', 99.99, 100) "
                "ON CONFLICT (symbol, date) DO NOTHING"
            ))
            result = conn.execute(text(
                "SELECT close FROM daily_prices "
                "WHERE symbol = '_TEST_' AND date = '2000-01-01'"
            ))
            row = result.fetchone()
            assert row is not None and float(row[0]) == 99.99

            # Clean up test row
            conn.execute(text(
                "DELETE FROM daily_prices WHERE symbol = '_TEST_'"
            ))
            conn.commit()

        print("  [OK] Read/Write  : VERIFIED")
    except Exception as e:
        print(f"  [FAIL] Read/Write test: {e}")
        return False

    # 4. Show table counts
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            ))
            tables = [r[0] for r in result.fetchall()]
            print(f"  [OK] Tables      : {', '.join(tables)}")
    except Exception:
        pass

    return True


# ── IBKR test ──────────────────────────────────────────────────

def test_ibkr() -> bool:
    """Test IBKR TWS/Gateway connection."""
    print("\n" + "=" * 55)
    print("   IBKR TWS / GATEWAY CONNECTION TEST")
    print("=" * 55)

    try:
        from common.config import IBKR_CONFIG
        print(f"  Host         : {IBKR_CONFIG['host']}")
        print(f"  Port         : {IBKR_CONFIG['port']}")
        print(f"  Client ID    : {IBKR_CONFIG['client_id']}")
        print(f"  Read-only    : {IBKR_CONFIG['readonly']}")
    except Exception as e:
        print(f"  [FAIL] Could not load config: {e}")
        return False

    try:
        from ib_insync import IB
    except ImportError:
        print("  [FAIL] ib_insync not installed. Run: pip install ib_insync")
        return False

    ib = IB()
    try:
        ib.connect(
            host=IBKR_CONFIG["host"],
            port=IBKR_CONFIG["port"],
            clientId=IBKR_CONFIG["client_id"],
            readonly=IBKR_CONFIG["readonly"],
            timeout=IBKR_CONFIG["timeout"],
        )

        print(f"  [OK] Connected   : YES")
        print(f"  [OK] Server ver  : {ib.client.serverVersion()}")

        # Show managed accounts
        accounts = ib.managedAccounts()
        print(f"  [OK] Accounts    : {accounts}")

        # Quick market data test: request SPY contract details
        from ib_insync import Stock
        contract = Stock("SPY", "SMART", "USD")
        details = ib.reqContractDetails(contract)
        if details:
            print(f"  [OK] Market data : SPY contract resolved ({details[0].longName})")
        else:
            print("  [WARN] Could not resolve SPY contract (market may be closed)")

        ib.disconnect()
        print(f"  [OK] Disconnect  : CLEAN")
        return True

    except ConnectionRefusedError:
        print("  [FAIL] Connection REFUSED")
        print("\n  Troubleshooting:")
        print("    1. Is TWS or IB Gateway running?")
        print("    2. In TWS: Edit > Global Config > API > Settings")
        print("       - Enable ActiveX and Socket Clients")
        print("       - Socket port should match config (7497=TWS, 4001=Gateway)")
        print("    3. Uncheck 'Read-Only API' if you need write access")
        return False

    except TimeoutError:
        print("  [FAIL] Connection TIMED OUT")
        print("    Check firewall settings and TWS API port.")
        return False

    except Exception as e:
        print(f"  [FAIL] Error: {e}")
        return False

    finally:
        if ib.isConnected():
            ib.disconnect()


# ── Main ───────────────────────────────────────────────────────

def main():
    print()
    print("+" + "=" * 53 + "+")
    print("|    SMART MONEY ROTATION  -  CONNECTION TESTS       |")
    print("+" + "=" * 53 + "+")

    args = sys.argv[1:]
    run_db = True
    run_ibkr = True

    if "--db-only" in args:
        run_ibkr = False
    if "--ibkr-only" in args:
        run_db = False

    pg_ok = test_postgres() if run_db else None
    ibkr_ok = test_ibkr() if run_ibkr else None

    # ── Summary ────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print("   SUMMARY")
    print("=" * 55)
    if pg_ok is not None:
        status = "[OK] PASS" if pg_ok else "[FAIL] FAIL"
        print(f"  PostgreSQL   : {status}")
    if ibkr_ok is not None:
        status = "[OK] PASS" if ibkr_ok else "[FAIL] FAIL"
        print(f"  IBKR         : {status}")
    print("=" * 55)

    # Exit code: 0 = all passed, 1 = something failed
    results = [r for r in (pg_ok, ibkr_ok) if r is not None]
    if not all(results):
        sys.exit(1)


if __name__ == "__main__":
    main()