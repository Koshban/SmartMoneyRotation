us_options.parquet — Schema
═══════════════════════════════════════════════════════════
Column              Parquet Type       Logical Type
───────────────────────────────────────────────────────────
expiry              BYTE_ARRAY         VARCHAR (UTF8)
strike              DOUBLE             DOUBLE
opt_type            BYTE_ARRAY         VARCHAR (UTF8)
bid                 DOUBLE             DOUBLE
ask                 DOUBLE             DOUBLE
last                DOUBLE             DOUBLE
volume              DOUBLE             DOUBLE          ← should be INT
oi                  DOUBLE             DOUBLE          ← should be INT
iv                  DOUBLE             DOUBLE
delta               DOUBLE             DOUBLE
gamma               DOUBLE             DOUBLE
theta               DOUBLE             DOUBLE
vega                DOUBLE             DOUBLE
rho                 DOUBLE             DOUBLE
underlying_price    DOUBLE             DOUBLE
source              BYTE_ARRAY         VARCHAR (UTF8)
═══════════════════════════════════════════════════════════
Notes:
  - volume, oi are DOUBLE in parquet but INTEGER in PostgreSQL
  - date, symbol, dte are NOT in the parquet (added in Python)
  - All columns are OPTIONAL (nullable)