# ARS on Snowflake — Developer Migration Handbook

> Target audience: developers porting the ARS (Auto Replenishment System) MSA, Grid Builder and Listing pipelines from **Azure SQL Server** to **Snowflake**. Read this file first, then work through the process SOPs in order.

| # | Document | What it covers |
|---|---|---|
| 00 | This file | Architecture, connection setup, T-SQL → Snowflake translation cheat-sheet |
| 01 | [Data Model & Masters](01_data_model_and_masters.md) | All source, master, config and output tables with Snowflake DDL |
| 02 | [MSA Stock Calculation](02_msa_stock_calculation.md) | The 9-step warehouse stock pipeline, Snowflake version |
| 03 | [Grid Builder](03_grid_builder.md) | Dynamic pivot-grid generation driven by `ARS_GRID_BUILDER` |
| 04 | [Listing Process](04_listing_process.md) | 8-part listing pipeline + rule-engine allocation (waves × types × rounds) |

---

## 1. Why Snowflake

Current state (Azure SQL):

- Two Azure SQL databases: **Claude** (RBAC / audit) + **Rep_data** (business data).
- Business pipelines (MSA, Grid Builder, Listing) are T-SQL heavy: `PIVOT`, `#temp` tables, `IDENTITY`, `sp_executesql`, `SELECT INTO`, `sys.tables`, `ISNULL`, `TOP N`.
- Pandas side-logic where pivots got too dynamic (MSA Step 5).

Target state (Snowflake):

- **One Snowflake account**, one database (e.g. `ARS_PROD`), schemas by layer:
  - `RAW` — landing from ET_STORE_STOCK / ET_STORE_SALES / SAP extracts.
  - `MASTER` — product, store, size, SLOC masters, `Cont_presets`, `ARS_CHECKLIST`.
  - `STAGE` — intermediate (MSA step outputs, grid staging).
  - `MART` — published tables consumed by API (`ARS_MSA_*`, `ARS_GRID_*`, `ARS_LISTING`, `ARS_LISTING_WORKING`, `ARS_ALLOC_WORKING`).
  - `AUDIT` — audit log, job runs.
- **Stored procedures in Snowflake Scripting (SQL)** or **Python UDFs/sprocs** for step orchestration; keep heavy set-based work in SQL.
- **No `#temp` tables** — use `CREATE TEMPORARY TABLE` (session-scoped) or `CREATE OR REPLACE TRANSIENT TABLE`.
- **No dynamic PIVOT** — use `PIVOT ... ANY` (Snowflake supports dynamic pivots since 2023) or pre-compute and `GROUP BY ... SUM(IIF(...))`.
- Python side (`msa_service.py`, `rule_engine.py`) keeps orchestration; swap `pyodbc` for **`snowflake-connector-python`** + **`snowflake-sqlalchemy`**.

---

## 2. Architecture — before and after

```
BEFORE (SQL Server)                         AFTER (Snowflake)
────────────────────                        ────────────────────
 FastAPI                                     FastAPI
   │ pyodbc                                    │ snowflake-connector-python
   ▼                                           ▼
 Azure SQL (Rep_data)                        Snowflake DB: ARS_PROD
   ├─ ET_STORE_STOCK      (staging)            ├─ RAW.ET_STORE_STOCK
   ├─ ET_STORE_SALES      (staging)            ├─ RAW.ET_STORE_SALES
   ├─ MASTER_*            (masters)            ├─ MASTER.MASTER_*
   ├─ ARS_MSA_*           (mart)               ├─ MART.ARS_MSA_*
   ├─ ARS_GRID_*          (dynamic mart)       ├─ MART.ARS_GRID_*
   ├─ ARS_LISTING / _WORKING                   ├─ MART.ARS_LISTING / _WORKING
   └─ ARS_ALLOC_WORKING                        └─ MART.ARS_ALLOC_WORKING
                                               └─ Warehouses:
                                                    WH_LOAD  (XSMALL, 1 cluster) → ingestion
                                                    WH_CALC  (MEDIUM, autosuspend 60s) → MSA + Grid + Listing
                                                    WH_BI    (XSMALL) → dashboards
```

---

## 3. Connection & auth

### 3.1 Snowflake account setup

```sql
-- One-time setup, run as ACCOUNTADMIN
CREATE DATABASE ARS_PROD;
CREATE SCHEMA ARS_PROD.RAW;
CREATE SCHEMA ARS_PROD.MASTER;
CREATE SCHEMA ARS_PROD.STAGE;
CREATE SCHEMA ARS_PROD.MART;
CREATE SCHEMA ARS_PROD.AUDIT;

CREATE WAREHOUSE WH_CALC  WITH WAREHOUSE_SIZE='MEDIUM'  AUTO_SUSPEND=60  AUTO_RESUME=TRUE;
CREATE WAREHOUSE WH_LOAD  WITH WAREHOUSE_SIZE='XSMALL'  AUTO_SUSPEND=60  AUTO_RESUME=TRUE;
CREATE WAREHOUSE WH_BI    WITH WAREHOUSE_SIZE='XSMALL'  AUTO_SUSPEND=300 AUTO_RESUME=TRUE;

CREATE ROLE ARS_APP;
CREATE ROLE ARS_RO;

GRANT USAGE ON DATABASE ARS_PROD TO ROLE ARS_APP;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ARS_PROD TO ROLE ARS_APP;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ARS_PROD.MART   TO ROLE ARS_APP;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ARS_PROD.STAGE  TO ROLE ARS_APP;
GRANT SELECT ON ALL TABLES IN SCHEMA ARS_PROD.MASTER TO ROLE ARS_APP;
GRANT USAGE ON WAREHOUSE WH_CALC TO ROLE ARS_APP;
GRANT USAGE ON WAREHOUSE WH_LOAD TO ROLE ARS_APP;

CREATE USER ARS_SVC PASSWORD='...' DEFAULT_ROLE=ARS_APP DEFAULT_WAREHOUSE=WH_CALC;
GRANT ROLE ARS_APP TO USER ARS_SVC;
```

### 3.2 Python side

Add to `requirements.txt`:

```
snowflake-connector-python>=3.7.0
snowflake-sqlalchemy>=1.6.0
```

Replace `backend/app/database/session.py` engine creation:

```python
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

data_engine = create_engine(
    URL(
        account=settings.SF_ACCOUNT,        # iafphkw-hh80816
        user=settings.SF_USER,              # ARS_SVC
        password=settings.SF_PASSWORD,
        database="ARS_PROD",
        schema="MART",
        warehouse="WH_CALC",
        role="ARS_APP",
    ),
    pool_pre_ping=True,
    pool_recycle=1800,
    # Snowflake ignores fast_executemany; use write_pandas instead for bulk.
)
```

### 3.3 Bulk load pattern (replaces `fast_executemany`)

```python
from snowflake.connector.pandas_tools import write_pandas

with data_engine.raw_connection() as raw:
    sf_conn = raw.driver_connection  # the underlying SnowflakeConnection
    write_pandas(sf_conn, df, "ARS_MSA_VAR_ART", schema="MART",
                 quote_identifiers=False, auto_create_table=False,
                 overwrite=False, chunk_size=100_000)
```

For very large loads (> 10M rows) use a Snowflake **internal stage + COPY INTO**:

```sql
PUT file://./msa_var_art.csv @%ARS_MSA_VAR_ART;
COPY INTO ARS_MSA_VAR_ART
FROM @%ARS_MSA_VAR_ART
FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
```

---

## 4. T-SQL → Snowflake translation cheat-sheet

| SQL Server idiom | Snowflake equivalent |
|---|---|
| `SELECT TOP 10 …` | `SELECT … LIMIT 10` |
| `ISNULL(x, 0)` | `COALESCE(x, 0)` or `NVL(x, 0)` |
| `IIF(cond, a, b)` | `IFF(cond, a, b)` |
| `GETDATE()` | `CURRENT_TIMESTAMP()` |
| `CAST(x AS NVARCHAR(50))` | `CAST(x AS VARCHAR(50))` or `::VARCHAR` |
| `NVARCHAR(MAX)` | `VARCHAR` (no length cap) |
| `DATETIME` | `TIMESTAMP_NTZ` (default) or `TIMESTAMP_TZ` |
| `IDENTITY(1,1)` | `NUMBER IDENTITY(1,1)` or `AUTOINCREMENT` |
| `SELECT … INTO #tmp FROM …` | `CREATE TEMPORARY TABLE tmp AS SELECT … FROM …` |
| `#tmp` (temp, session scope) | `CREATE TEMPORARY TABLE` — valid for session, auto-dropped |
| `##global_tmp` | Not supported — use `TRANSIENT` table in a session schema |
| `DROP TABLE IF EXISTS X` | Same syntax supported |
| `TRUNCATE TABLE` | Same |
| `UPDATE a SET … FROM a JOIN b …` | `UPDATE a SET … FROM b WHERE a.k = b.k`  *(Snowflake supports the FROM-join form; no alias chain)* |
| `MERGE` | `MERGE INTO tgt USING src ON … WHEN MATCHED …` — same SQL:2008 syntax |
| `sys.tables` / `sys.columns` | `INFORMATION_SCHEMA.TABLES` / `COLUMNS` (or `SHOW TABLES`) |
| `sp_executesql @stmt, @params, @p=...` | `EXECUTE IMMEDIATE :stmt USING (:p)` (Snowflake Scripting) |
| Dynamic `PIVOT (SUM(x) FOR col IN (…))` | Static form same; dynamic form → `PIVOT (SUM(x) FOR col IN (ANY))` (since 2023) |
| `PATINDEX` | `REGEXP_INSTR` / `POSITION` |
| `STRING_AGG(x, ',')` | `LISTAGG(x, ',') WITHIN GROUP (ORDER BY …)` |
| `CHARINDEX(a, b)` | `POSITION(a IN b)` |
| `DATEADD(day, n, d)` | `DATEADD(day, n, d)` — same |
| Clustered PK / clustered index | Snowflake has **no indexes**. Use `CLUSTER BY (…)` for large tables; rely on micro-partition pruning. |
| `CREATE INDEX …` | Remove — use `CLUSTER BY` on the base table if scan-pruning is insufficient. |
| Row-level lock hints (`WITH (UPDLOCK, …)`) | Remove. Snowflake uses multi-version concurrency — no hints needed. |
| `BEGIN TRAN / COMMIT / ROLLBACK` | Same syntax works; auto-commit is default. |
| `RAISERROR` | `RAISE STATEMENT_ERROR` (in scripting) or just raise from Python. |

**Gotchas specific to ARS:**

1. **Identifier case.** Snowflake uppercases unquoted identifiers. Column names like `MAJ_CAT`, `WERKS` are already upper — fine. But anything quoted in the app layer (e.g. `[ACS_D]`) must drop the brackets — Snowflake uses `"ACS_D"` double-quotes and the quotes preserve case.
2. **No `TOP` in subqueries.** Replace `SELECT TOP 1 …` with `SELECT … ORDER BY … LIMIT 1`.
3. **Empty strings vs NULL.** Snowflake treats `''` and `NULL` as distinct (same as SQL Server). No change.
4. **`ROW_NUMBER() OVER(...)` supported identically.** Good — allocation rule engine uses this for `ST_RANK`.
5. **No temp table that outlives a session.** If you previously persisted `#rule_pool` across two service calls in one HTTP request, make sure both calls reuse the **same connection** (keep the session).

---

## 5. Orchestration pattern (Python-driven, SQL-heavy)

The existing ARS code orchestrates in Python and runs set-based SQL for each step. This pattern is ideal for Snowflake — keep it.

```python
# Pseudocode per step — same shape as listing.py _run()
def _run(conn, sql: str, params: dict | None = None) -> int:
    cur = conn.cursor()
    cur.execute(sql, params or {})
    rows = cur.rowcount
    conn.commit()
    return rows
```

Replace `_time_step()` logging (already present) as-is; only the SQL inside changes. Keep **one Snowflake session** for the whole run so temp tables stay alive across steps.

```python
def generate_listing(req):
    with data_engine.raw_connection() as raw:
        sf = raw.driver_connection
        cur = sf.cursor()
        cur.execute("USE WAREHOUSE WH_CALC")
        cur.execute("ALTER SESSION SET QUERY_TAG = 'ars-listing'")
        try:
            part1(sf)
            part2(sf)
            part3_to_7(sf)
            part8_allocation(sf)
        finally:
            cur.close()
```

**Warehouse sizing** (empirical starting points — adjust after first full run):

| Step | Warehouse | Why |
|---|---|---|
| Load raw | `WH_LOAD` XS | Mostly COPY INTO |
| MSA Step 5 pivot | `WH_CALC` M or L | Pivot across many SLOCs |
| Grid Builder Run-All | `WH_CALC` M | Sequential but bursty |
| Listing Part 4a (many grid joins) | `WH_CALC` L | Wide join graph |
| Listing Part 8 allocation | `WH_CALC` M | Many small updates; not scan-heavy |

---

## 6. What stays the same

- Business rules (MSA 9 steps, Grid 6 output columns, Listing 8 parts, Allocation 4 waves × 3 types × N rounds).
- Column names, OPT_TYPE semantics (MIX / RL / TBC / TBL), ALLOC_STATUS values.
- The API surface: FastAPI endpoints, request/response shapes, UI.

## 7. What changes

- Dialect (T-SQL → Snowflake SQL).
- Temp tables (`#x` → `TEMPORARY TABLE x`).
- Dynamic PIVOT strategy.
- Bulk load (executemany → `write_pandas` / `COPY INTO`).
- No indexes; use clustering keys on large tables (`ARS_LISTING`, `ARS_LISTING_WORKING`, `ARS_ALLOC_WORKING`).
- Connection pool (SQLAlchemy pool stays, but pool_recycle should be ≤ 1800s — Snowflake idle tokens expire).
- RBAC — Snowflake roles replace SQL logins; RLS becomes Snowflake **row-access policies**.

## 8. Migration order (recommended)

1. Stand up Snowflake account + warehouses + DB/schemas (§3.1).
2. Recreate master tables in `MASTER` (doc 01) and seed them from Azure SQL via a one-time bulk export.
3. Port **MSA** first (doc 02) — smallest blast radius, validates the full load/compute/publish cycle.
4. Port **Grid Builder** (doc 03) — depends on masters + stock only.
5. Port **Listing** (doc 04) — depends on both and is the biggest single change.
6. Dual-run Azure SQL + Snowflake for 2 cycles; diff `ARS_LISTING_WORKING.ALLOC_QTY` sums by (WERKS, MAJ_CAT). Any row-level diff must be explainable.
7. Cut over; keep Azure SQL read-only for 30 days as fallback.

## 9. Open questions / decisions for the team

- **RBAC/RLS:** Snowflake row-access policies on `MART.ARS_LISTING` keyed by `WERKS` / `RDC` vs application-side filtering. Recommendation: policy-side, matches current RLS semantics.
- **Audit log:** `AUDIT.audit_log` as an append-only table with `STREAMS` for change capture vs shipping to Snowflake from the app. Recommendation: keep app-side writes for now (lowest change).
- **Time travel / retention:** set `DATA_RETENTION_TIME_IN_DAYS=7` on `MART` tables. Allocation mistakes can be reverted via `AT(OFFSET => -…)`.
- **Streams + Tasks for scheduling:** optional — can automate the nightly MSA/Grid refresh via Snowflake Tasks instead of APScheduler, after the cutover is stable.
