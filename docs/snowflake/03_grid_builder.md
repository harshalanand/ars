# Grid Builder — Snowflake SOP

> Port of `backend/app/services/grid_calculations.py` and `backend/app/api/v1/endpoints/grid_builder.py` to Snowflake. Preserves the config-driven, multi-grid behaviour. Prerequisites: [00 README](00_README_Snowflake_Migration.md), [01 Data Model](01_data_model_and_masters.md).

## 0. What Grid Builder does (recap)

Grid Builder produces **summary pivot tables** named `ARS_GRID_<grid_name>`, one per row in `MASTER.ARS_GRID_BUILDER WHERE STATUS='ACTIVE'`. Each grid rolls up stock/sale/MBQ/count per a hierarchy like `(WERKS, MAJ_CAT, FAB)`. The Listing pipeline's Part 4a then joins every grid back onto the listing rows.

Six output metrics per grid:

| Column | Meaning |
|---|---|
| `<prefix>_STK_TTL` | Total stock at the grouping |
| `<prefix>_STR` | Store count at the grouping |
| `<prefix>_CONT` | Contribution % (share of MAJ_CAT total) |
| `<prefix>_MBQ` | Minimum base quantity (target stock) |
| `<prefix>_OPT_CNT` | Distinct option (`GEN_ART × CLR`) count |
| `<prefix>_DISP_Q` | Dispatch quantity shipped in window |

Where `<prefix>` is either the grid's `OUTPUT_PREFIX` column (for secondary grids) or a fixed name like `MJ` / `VAR` for the primary grids.

## 1. Run modes

Two entry points, matching the UI:

1. **Run All** — iterate every `ACTIVE` grid in `SEQ` order. This is the standard weekly run.
2. **Run One** — rebuild a single grid (debug / config-change).

Both call the same per-grid procedure `MART.SP_RUN_GRID(P_GRID_ID)`.

## 2. Per-grid SQL — the template

Snowflake runs this under a single SQL statement per grid, composed dynamically in the stored procedure from the grid's `HIERARCHY_COLUMNS` array. Below is the **template** — a concrete example follows.

```sql
CREATE OR REPLACE TABLE MART.ARS_GRID_<GRID_NAME> AS
WITH
  -- 1. Source stock aggregated to the hierarchy level
  stk AS (
    SELECT
        <hierarchy cols>,
        SUM(STK_Q)                            AS STK_TTL,
        COUNT(DISTINCT WERKS)                 AS STR,
        COUNT(DISTINCT GEN_ART_NUMBER||'|'||CLR) AS OPT_CNT
    FROM (
        SELECT
            s.WERKS, s.GEN_ART_NUMBER, s.CLR, s.VAR_ART, s.SZ,
            p.MAJ_CAT, p.SEG, p.RNG_SEG, p.MACRO_MVGR, p.MICRO_MVGR,
            p.FAB, p.M_VND_CD, p.COL_FAM, p.SSN,
            s.STK_Q
        FROM RAW.ET_STORE_STOCK s
        JOIN MASTER.VW_MASTER_PRODUCT p
          ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER
         AND p.CLR            = s.CLR
        WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
    )
    GROUP BY <hierarchy cols>
  ),

  -- 2. Sales aggregated to the same hierarchy (for DISP_Q & CONT)
  sal AS (
    SELECT
        <hierarchy cols>,
        SUM(SALE_Q) AS DISP_Q
    FROM (
        SELECT s.WERKS, p.MAJ_CAT, p.RNG_SEG, p.MACRO_MVGR, p.MICRO_MVGR,
               p.FAB, p.M_VND_CD, p.CLR, p.COL_FAM, s.GEN_ART_NUMBER,
               s.VAR_ART, s.SZ, s.SALE_Q
        FROM RAW.ET_STORE_SALES s
        JOIN MASTER.VW_MASTER_PRODUCT p
          ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER
         AND p.CLR            = s.CLR
        WHERE s.SALE_DT >= DATEADD(day, -90, CURRENT_DATE)
    )
    GROUP BY <hierarchy cols>
  ),

  -- 3. MAJ_CAT totals for contribution %
  mj AS (
    SELECT WERKS, MAJ_CAT,
           SUM(STK_Q) AS MJ_STK_TTL
    FROM (
        SELECT s.WERKS, p.MAJ_CAT, s.STK_Q
        FROM RAW.ET_STORE_STOCK s
        JOIN MASTER.VW_MASTER_PRODUCT p
          ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER
         AND p.CLR            = s.CLR
        WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
    )
    GROUP BY WERKS, MAJ_CAT
  )

SELECT
    <hierarchy cols>,
    COALESCE(stk.STK_TTL, 0)  AS <PFX>_STK_TTL,
    COALESCE(stk.STR, 0)      AS <PFX>_STR,
    CASE WHEN mj.MJ_STK_TTL > 0
         THEN stk.STK_TTL / mj.MJ_STK_TTL ELSE 0 END AS <PFX>_CONT,
    -- MBQ = contribution × MAJ_CAT target  (see §3 for per-category logic)
    CASE WHEN mj.MJ_STK_TTL > 0
         THEN stk.STK_TTL / mj.MJ_STK_TTL * mj.MJ_STK_TTL ELSE 0 END AS <PFX>_MBQ,
    COALESCE(stk.OPT_CNT, 0)  AS <PFX>_OPT_CNT,
    COALESCE(sal.DISP_Q, 0)   AS <PFX>_DISP_Q,
    CURRENT_TIMESTAMP()        AS BUILT_TS
FROM stk
LEFT JOIN sal USING (<hierarchy cols>)
LEFT JOIN mj  USING (WERKS, MAJ_CAT)
CLUSTER BY (WERKS, MAJ_CAT);
```

`<hierarchy cols>` and `<PFX>` are substituted by the stored procedure from the `HIERARCHY_COLUMNS` / `OUTPUT_PREFIX` columns in `MASTER.ARS_GRID_BUILDER`.

## 3. Contribution & MBQ — per-category logic

The template above uses a simple `stk/MJ_STK_TTL` ratio. In the real pipeline, MBQ can come from a target table or a formula based on `Cont_presets`. Port the exact rule from `grid_calculations.py :: calculate_contribution`:

```sql
-- MBQ = ceil( target_stock_days × average_daily_sale × contribution )
WITH mbq_seed AS (
    SELECT
        <hierarchy cols>,
        stk.STK_TTL,
        mj.MJ_STK_TTL,
        cp.STOCK_PCT,
        cp.EXCESS_X,
        sal.DISP_Q
    FROM stk
    LEFT JOIN mj USING (WERKS, MAJ_CAT)
    LEFT JOIN sal USING (<hierarchy cols>)
    LEFT JOIN MASTER.CONT_PRESETS cp USING (MAJ_CAT)
)
SELECT
    <hierarchy cols>,
    STK_TTL AS <PFX>_STK_TTL,
    CEIL(
        CASE WHEN MJ_STK_TTL > 0
             THEN (STK_TTL / MJ_STK_TTL) * MJ_STK_TTL * STOCK_PCT
             ELSE 0 END
    ) AS <PFX>_MBQ,
    ...
FROM mbq_seed;
```

> Calibrate against the legacy output for one MAJ_CAT before generalising. The current code has subtle caps around EXCESS_X — replicate them faithfully.

## 4. Concrete example — grid `MJ_FAB`

Config row:
```
GRID_NAME        = 'MJ_FAB'
HIERARCHY_COLUMNS = ['WERKS','MAJ_CAT','FAB']
OUTPUT_PREFIX    = 'FAB'
GRID_GROUP       = 'Secondary'
SEQ              = 30
```

Generated statement:

```sql
CREATE OR REPLACE TABLE MART.ARS_GRID_MJ_FAB AS
WITH stk AS (
    SELECT s.WERKS, p.MAJ_CAT, p.FAB,
           SUM(s.STK_Q) AS STK_TTL,
           COUNT(DISTINCT s.WERKS) AS STR,
           COUNT(DISTINCT s.GEN_ART_NUMBER||'|'||s.CLR) AS OPT_CNT
    FROM RAW.ET_STORE_STOCK s
    JOIN MASTER.VW_MASTER_PRODUCT p
      ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
    WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
    GROUP BY 1,2,3
),
sal AS (
    SELECT s.WERKS, p.MAJ_CAT, p.FAB, SUM(s.SALE_Q) AS DISP_Q
    FROM RAW.ET_STORE_SALES s
    JOIN MASTER.VW_MASTER_PRODUCT p
      ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
    WHERE s.SALE_DT >= DATEADD(day, -90, CURRENT_DATE)
    GROUP BY 1,2,3
),
mj AS (
    SELECT s.WERKS, p.MAJ_CAT, SUM(s.STK_Q) AS MJ_STK_TTL
    FROM RAW.ET_STORE_STOCK s
    JOIN MASTER.VW_MASTER_PRODUCT p
      ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
    WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
    GROUP BY 1,2
)
SELECT
    stk.WERKS, stk.MAJ_CAT, stk.FAB,
    stk.STK_TTL                AS FAB_STK_TTL,
    stk.STR                    AS FAB_STR,
    IFF(mj.MJ_STK_TTL > 0,
        stk.STK_TTL / mj.MJ_STK_TTL, 0)   AS FAB_CONT,
    IFF(mj.MJ_STK_TTL > 0,
        CEIL(stk.STK_TTL / mj.MJ_STK_TTL * mj.MJ_STK_TTL
             * COALESCE(cp.STOCK_PCT, 0.6)), 0) AS FAB_MBQ,
    stk.OPT_CNT                AS FAB_OPT_CNT,
    COALESCE(sal.DISP_Q, 0)    AS FAB_DISP_Q,
    CURRENT_TIMESTAMP()        AS BUILT_TS
FROM stk
LEFT JOIN sal ON sal.WERKS=stk.WERKS AND sal.MAJ_CAT=stk.MAJ_CAT AND sal.FAB=stk.FAB
LEFT JOIN mj  ON  mj.WERKS=stk.WERKS AND  mj.MAJ_CAT=stk.MAJ_CAT
LEFT JOIN MASTER.CONT_PRESETS cp ON cp.MAJ_CAT = stk.MAJ_CAT;

-- Add cluster key
ALTER TABLE MART.ARS_GRID_MJ_FAB CLUSTER BY (WERKS, MAJ_CAT);
```

## 5. The driver procedure

```sql
CREATE OR REPLACE PROCEDURE MART.SP_RUN_GRID(P_GRID_ID NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_name    VARCHAR;
    v_hier    ARRAY;
    v_pfx     VARCHAR;
    v_stmt    VARCHAR;
    v_rows    NUMBER;
    v_hier_cs VARCHAR;   -- comma-separated hierarchy columns
BEGIN
    SELECT GRID_NAME, HIERARCHY_COLUMNS, OUTPUT_PREFIX
      INTO :v_name, :v_hier, :v_pfx
    FROM MASTER.ARS_GRID_BUILDER
    WHERE GRID_ID = :P_GRID_ID AND STATUS = 'ACTIVE';

    -- Build the comma-separated hierarchy column list
    SELECT LISTAGG(VALUE::VARCHAR, ', ') WITHIN GROUP (ORDER BY SEQ)
      INTO :v_hier_cs
    FROM TABLE(FLATTEN(INPUT => :v_hier)) WITH OFFSET seq;

    v_stmt := '
        CREATE OR REPLACE TABLE MART.ARS_GRID_' || :v_name || ' AS
        WITH stk AS (
            SELECT ' || :v_hier_cs || ',
                   SUM(s.STK_Q) AS STK_TTL,
                   COUNT(DISTINCT s.WERKS) AS STR,
                   COUNT(DISTINCT s.GEN_ART_NUMBER||''|''||s.CLR) AS OPT_CNT
            FROM RAW.ET_STORE_STOCK s
            JOIN MASTER.VW_MASTER_PRODUCT p
              ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
            WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
            GROUP BY ' || :v_hier_cs || '
        ),
        sal AS (
            SELECT ' || :v_hier_cs || ', SUM(s.SALE_Q) AS DISP_Q
            FROM RAW.ET_STORE_SALES s
            JOIN MASTER.VW_MASTER_PRODUCT p
              ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
            WHERE s.SALE_DT >= DATEADD(day, -90, CURRENT_DATE)
            GROUP BY ' || :v_hier_cs || '
        ),
        mj AS (
            SELECT s.WERKS, p.MAJ_CAT, SUM(s.STK_Q) AS MJ_STK_TTL
            FROM RAW.ET_STORE_STOCK s
            JOIN MASTER.VW_MASTER_PRODUCT p
              ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER AND p.CLR = s.CLR
            WHERE s.SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM RAW.ET_STORE_STOCK)
            GROUP BY s.WERKS, p.MAJ_CAT
        )
        SELECT stk.*, mj.MJ_STK_TTL,
               COALESCE(sal.DISP_Q, 0) AS ' || :v_pfx || '_DISP_Q,
               stk.STK_TTL              AS ' || :v_pfx || '_STK_TTL,
               stk.STR                  AS ' || :v_pfx || '_STR,
               stk.OPT_CNT              AS ' || :v_pfx || '_OPT_CNT,
               IFF(mj.MJ_STK_TTL > 0,
                   stk.STK_TTL / mj.MJ_STK_TTL, 0) AS ' || :v_pfx || '_CONT,
               IFF(mj.MJ_STK_TTL > 0,
                   CEIL(stk.STK_TTL * COALESCE(cp.STOCK_PCT, 0.6)), 0) AS ' || :v_pfx || '_MBQ,
               CURRENT_TIMESTAMP() AS BUILT_TS
        FROM stk
        LEFT JOIN sal USING (' || :v_hier_cs || ')
        LEFT JOIN mj  USING (WERKS, MAJ_CAT)
        LEFT JOIN MASTER.CONT_PRESETS cp USING (MAJ_CAT)
    ';

    EXECUTE IMMEDIATE :v_stmt;

    -- Register in the runtime hierarchy table
    MERGE INTO MASTER.ARS_GRID_HIERARCHY t
    USING (
        SELECT :v_name AS GRID_NAME, VALUE::VARCHAR AS LEVEL_COL,
               :v_pfx  AS OUTPUT_PREFIX
        FROM TABLE(FLATTEN(INPUT => :v_hier))
    ) s
    ON t.GRID_NAME = s.GRID_NAME AND t.LEVEL_COL = s.LEVEL_COL
    WHEN NOT MATCHED THEN INSERT (GRID_NAME, LEVEL_COL, OUTPUT_PREFIX, BUILT_TS)
         VALUES (s.GRID_NAME, s.LEVEL_COL, s.OUTPUT_PREFIX, CURRENT_TIMESTAMP());

    v_stmt := 'SELECT COUNT(*) FROM MART.ARS_GRID_' || :v_name;
    EXECUTE IMMEDIATE :v_stmt;
    SELECT $1 INTO :v_rows FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    RETURN OBJECT_CONSTRUCT('grid_name', :v_name, 'rows', :v_rows);
END;
$$;
```

### Run-all wrapper

```sql
CREATE OR REPLACE PROCEDURE MART.SP_RUN_ALL_GRIDS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c CURSOR FOR
        SELECT GRID_ID FROM MASTER.ARS_GRID_BUILDER
        WHERE STATUS = 'ACTIVE' ORDER BY SEQ;
    results VARIANT := TO_VARIANT(ARRAY_CONSTRUCT());
    r VARIANT;
BEGIN
    FOR g IN c DO
        CALL MART.SP_RUN_GRID(g.GRID_ID) INTO :r;
        results := ARRAY_APPEND(results, :r);
    END FOR;
    RETURN OBJECT_CONSTRUCT('grids', results);
END;
$$;
```

## 6. Python integration

Replace the current `POST /grid-builder/run-all` handler body with:

```python
def run_all_grids():
    with data_engine.raw_connection() as raw:
        sf = raw.driver_connection
        cur = sf.cursor()
        cur.execute("CALL MART.SP_RUN_ALL_GRIDS()")
        result = cur.fetchone()[0]
        sf.commit()
    return result  # {"grids": [{"grid_name":"MJ","rows":...}, ...]}
```

For `POST /grid-builder/grids/{grid_id}/run`:

```python
cur.execute("CALL MART.SP_RUN_GRID(%s)", (grid_id,))
```

## 7. Primary vs secondary — enforcing `grid_group` semantics

Listing Part 7 cares about `grid_group`:

- **Primary** grids feed `PRI_CT%`.
- **Secondary** grids feed `SEC_CT%`.

In Snowflake, keep that logic in the Listing procedure (doc 04) by joining `MASTER.ARS_GRID_HIERARCHY` and `MASTER.ARS_GRID_BUILDER.GRID_GROUP`. No change needed in Grid Builder itself.

## 8. Special case — `ARS_GRID_MJ_VAR_ART`

The variant grid is the seed for Listing Part 1. Its hierarchy is the widest (`WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ`) — everything is a variant row. The same SP runs it but because `OUTPUT_PREFIX = 'VAR'`:

- Output columns: `VAR_STK_TTL, VAR_STR, VAR_CONT, VAR_MBQ, VAR_OPT_CNT, VAR_DISP_Q`.
- Grain is already `VAR_ART × SZ`, so `STR = 1` and `OPT_CNT = 1` per row.

Listing Part 1 later renames `VAR_STK_TTL` → `STK_TTL` and keeps the hierarchy columns as its primary key.

## 9. Dropping a grid cleanly

If you set `STATUS = 'INACTIVE'` on a config row, **the table does not auto-drop.** Clean up explicitly:

```sql
UPDATE MASTER.ARS_GRID_BUILDER SET STATUS='INACTIVE' WHERE GRID_NAME='MJ_M_VND_CD';
DROP TABLE IF EXISTS MART.ARS_GRID_MJ_M_VND_CD;
DELETE FROM MASTER.ARS_GRID_HIERARCHY WHERE GRID_NAME='MJ_M_VND_CD';
```

## 10. Verification

```sql
-- Active grids
SELECT GRID_NAME, GRID_GROUP, STATUS, SEQ, OUTPUT_PREFIX
FROM MASTER.ARS_GRID_BUILDER
ORDER BY SEQ;

-- Row counts for every grid table
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='MART' AND TABLE_NAME LIKE 'ARS_GRID_%'
ORDER BY TABLE_NAME;

-- Freshness
SELECT TABLE_NAME, LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='MART' AND TABLE_NAME LIKE 'ARS_GRID_%'
ORDER BY LAST_ALTERED DESC;

-- Spot check an MJ_FAB row
SELECT * FROM MART.ARS_GRID_MJ_FAB
WHERE WERKS = 'HN14' AND MAJ_CAT = 'M_TEES_HS'
LIMIT 20;

-- Cross-grid sanity: Σ(FAB_STK_TTL) per MAJ_CAT should ≈ MJ_STK_TTL
SELECT mj.WERKS, mj.MAJ_CAT, mj.MJ_STK_TTL,
       SUM(fab.FAB_STK_TTL) AS sum_fab
FROM MART.ARS_GRID_MJ mj
LEFT JOIN MART.ARS_GRID_MJ_FAB fab USING (WERKS, MAJ_CAT)
GROUP BY 1,2,3
HAVING ABS(mj.MJ_STK_TTL - sum_fab) > 0.5
LIMIT 20;
```

## 11. Performance notes

- Every grid scans `RAW.ET_STORE_STOCK` once. If the daily stock feed is > 20M rows, enable **result caching** — consecutive `SP_RUN_ALL_GRIDS` calls within 24 h will reuse the `stk`/`mj`/`sal` CTE results.
- Alternatively, materialize a daily `STAGE.STK_ENRICHED` table with the stock×master join once, and have each grid read from it. Big win on run-all time.
- Cluster keys on every grid table — see `CLUSTER BY (WERKS, MAJ_CAT)` above.

## 12. Common issues

| Symptom | Likely cause | Fix |
|---|---|---|
| One grid fails with "no such column X" | `HIERARCHY_COLUMNS` references a column not in `VW_MASTER_PRODUCT` | Refresh master; fix the JSON |
| All grids run but `*_CONT` and `*_MBQ` are 0 | `CONT_PRESETS` empty for that `MAJ_CAT` | Seed presets or fall back to 0.6 default (already in SQL) |
| `MJ_VAR_ART` table missing | Row status `INACTIVE` or SP errored | Flip to ACTIVE, re-run; variant grid is required by Listing Part 1 |
| Grid table exists but is empty after run | Snapshot date mismatch between STOCK and PENDING | Pin `(SELECT MAX(SNAPSHOT_DT) …)` to the same date in all CTEs |
| `SP_RUN_GRID` raises "object 'ARS_GRID_X' has too many columns" | Duplicate hierarchy column in config | `HIERARCHY_COLUMNS` must have distinct values |

## 13. How to update this doc

When a new output metric is added, when the contribution formula changes, or when a new `GRID_GROUP` is introduced.
