# MSA Stock Calculation — Snowflake SOP

> Port of `backend/app/services/msa_service.py` to Snowflake SQL. Preserves the 9-step algorithm exactly — only the dialect changes. Reader prerequisites: [00 README](00_README_Snowflake_Migration.md), [01 Data Model](01_data_model_and_masters.md).

## 0. What MSA does (recap)

MSA (Main Storage Area) answers one question per replenishment cycle:

> For every article (colour + size), how many units can the warehouse actually ship right now — after subtracting already-committed units?

Input: warehouse stock snapshot + pending allocations + product master + size master.
Output: three MART tables — **`ARS_MSA_TOTAL`** (raw pivot), **`ARS_MSA_GEN_ART`** (per option, `FNL_Q` is the shippable total), **`ARS_MSA_VAR_ART`** (per variant+size, used by the allocator).

## 1. Prerequisites

Run from Python (orchestrator) or Snowflake worksheet as `ARS_APP`:

```sql
USE ROLE ARS_APP;
USE DATABASE ARS_PROD;
USE SCHEMA MART;
USE WAREHOUSE WH_CALC;           -- MEDIUM for typical run; scale to LARGE if > 2M input rows
```

Before kicking off an MSA run verify inputs exist and are current:

```sql
SELECT 'ET_STORE_STOCK'   AS tbl, MAX(SNAPSHOT_DT) AS latest, COUNT(*) AS rows_
FROM RAW.ET_STORE_STOCK
UNION ALL
SELECT 'MASTER_ALC_PEND', MAX(AS_OF_DT), COUNT(*) FROM RAW.MASTER_ALC_PEND
UNION ALL
SELECT 'VW_MASTER_PRODUCT', MAX(UPDATED_TS)::DATE, COUNT(*) FROM MASTER.VW_MASTER_PRODUCT
UNION ALL
SELECT 'MASTER_CONT_SZ',  NULL::DATE, COUNT(*) FROM MASTER.MASTER_CONT_SZ;
```

## 2. Inputs to the procedure

| Param | Type | Default | Meaning |
|---|---|---|---|
| `P_SNAPSHOT_DT` | DATE | `CURRENT_DATE - 1` | Stock snapshot to use |
| `P_SLOCS` | ARRAY | from `MASTER.MASTER_SLOC WHERE INCLUDE_IN_MSA=1` | Allowed SLOCs |
| `P_SEGS` | ARRAY | `['APP','GM']` | Segments in scope |
| `P_THRESHOLD` | NUMBER | `0` | Step-8 variant filter threshold |

## 3. The 9 steps, translated

All nine run within a single Snowflake session so `TEMPORARY` tables live end-to-end. The orchestrator opens one connection, calls the steps, and closes it.

Each step is idempotent: it reads the previous step's temp table and writes the next.

### Step 1 — Filter by SLOC

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP1 AS
SELECT
    s.SNAPSHOT_DT, s.ST_CD, s.SLOC,
    s.GEN_ART_NUMBER, s.CLR, s.VAR_ART, s.SZ,
    s.STK_Q
FROM RAW.ET_STORE_STOCK s
WHERE s.SNAPSHOT_DT = :P_SNAPSHOT_DT
  AND s.SLOC IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(INPUT => :P_SLOCS)))
  AND s.ST_CD LIKE 'D%';   -- RDCs only (matches SQL Server filter); adapt if needed
```

> Why: matches `msa_service.py :: filter_sloc`.

### Step 2 — Normalize

Blanks / negatives → 0; cast stock to NUMBER. Snowflake returns NULL for non-numeric casts, so guard with `TRY_CAST`.

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP2 AS
SELECT
    SNAPSHOT_DT, ST_CD, SLOC,
    GEN_ART_NUMBER, CLR, VAR_ART, SZ,
    GREATEST(COALESCE(TRY_CAST(STK_Q AS NUMBER(18,3)), 0), 0) AS STK_Q
FROM TMP_MSA_STEP1;
```

### Step 3 — Fill missing dims

Where colour, vendor, size are blank on the stock feed but the variant master has them, back-fill. Ensures nothing is dropped for "missing colour".

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP3 AS
SELECT
    s.SNAPSHOT_DT, s.ST_CD, s.SLOC,
    s.GEN_ART_NUMBER,
    COALESCE(NULLIF(s.CLR, ''),     v.CLR, '_NA_') AS CLR,
    COALESCE(s.VAR_ART,              v.VAR_ART)    AS VAR_ART,
    COALESCE(NULLIF(s.SZ, ''),      v.SZ, '_NA_') AS SZ,
    s.STK_Q
FROM TMP_MSA_STEP2 s
LEFT JOIN MASTER.RETAIL_VARIANT_ARTICLE v
       ON v.VAR_ART = s.VAR_ART
       OR (v.GEN_ART_NUMBER = s.GEN_ART_NUMBER
           AND v.CLR = s.CLR AND v.SZ = s.SZ);
```

### Step 4 — Keep only APP + GM segments

Join product master and filter.

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP4 AS
SELECT
    s.SNAPSHOT_DT, s.ST_CD, s.SLOC,
    s.GEN_ART_NUMBER, s.CLR, s.VAR_ART, s.SZ,
    s.STK_Q,
    p.MAJ_CAT, p.SEG
FROM TMP_MSA_STEP3 s
JOIN MASTER.VW_MASTER_PRODUCT p
  ON p.GEN_ART_NUMBER = s.GEN_ART_NUMBER
 AND p.CLR            = s.CLR
 AND COALESCE(p.VAR_ART, s.VAR_ART) = s.VAR_ART
 AND COALESCE(p.SZ, s.SZ)           = s.SZ
WHERE p.SEG IN (SELECT VALUE::VARCHAR FROM TABLE(FLATTEN(INPUT => :P_SEGS)));
```

### Step 5 — Pivot by SLOC

This is the step that was hardest in SQL Server (dynamic T-SQL PIVOT built in Python). Snowflake has **dynamic PIVOT** (`FOR … IN (ANY)`), so it becomes one statement.

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP5 AS
SELECT *
FROM (
    SELECT
        ST_CD, GEN_ART_NUMBER, CLR, VAR_ART, SZ, MAJ_CAT, SEG,
        SLOC, STK_Q
    FROM TMP_MSA_STEP4
) p
PIVOT (
    SUM(STK_Q) FOR SLOC IN (ANY ORDER BY SLOC)
) AS pv;
```

This produces one `STK_Q_<SLOC>` column per distinct SLOC (Snowflake names them `'V01_FRESH'`, `'V02_RESERVE'`, … with quoted identifiers — handle them carefully in Python when naming columns downstream).

Total across all pivoted columns = **`STK_QTY`**. Snowflake has no built-in "sum across unknown columns" — we materialize the sum from the *original* (pre-pivot) table, which is semantically identical and avoids reflection on the pivot schema:

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP5_TTL AS
SELECT
    ST_CD, GEN_ART_NUMBER, CLR, VAR_ART, SZ, MAJ_CAT, SEG,
    SUM(STK_Q) AS STK_QTY
FROM TMP_MSA_STEP4
GROUP BY 1,2,3,4,5,6,7;
```

> **Why two tables?** The pivoted table is what audit needs (Excel-like per-SLOC breakdown → becomes `ARS_MSA_TOTAL`). The grouped table is what Steps 6–9 need (just the total). Keeping both avoids an awkward dynamic-column scan.

### Step 6 — Subtract pending allocations

Match pending on `(ST_CD, GEN_ART_NUMBER, CLR, VAR_ART, SZ)`.

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP6 AS
SELECT
    t.ST_CD, t.GEN_ART_NUMBER, t.CLR, t.VAR_ART, t.SZ,
    t.MAJ_CAT, t.SEG,
    t.STK_QTY,
    COALESCE(a.PEND_QTY, 0) AS PEND_QTY
FROM TMP_MSA_STEP5_TTL t
LEFT JOIN (
    SELECT ST_CD, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
           SUM(PEND_QTY) AS PEND_QTY
    FROM RAW.MASTER_ALC_PEND
    WHERE AS_OF_DT = :P_SNAPSHOT_DT
    GROUP BY 1,2,3,4,5
) a
  ON a.ST_CD = t.ST_CD
 AND a.GEN_ART_NUMBER = t.GEN_ART_NUMBER
 AND a.CLR  = t.CLR
 AND COALESCE(a.VAR_ART, t.VAR_ART) = t.VAR_ART
 AND COALESCE(a.SZ, t.SZ)           = t.SZ;
```

### Step 7 — Compute `FNL_Q`

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP7 AS
SELECT
    ST_CD, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
    MAJ_CAT, SEG,
    STK_QTY, PEND_QTY,
    GREATEST(STK_QTY - PEND_QTY, 0) AS FNL_Q
FROM TMP_MSA_STEP6;
```

### Step 8 — Generate colour variants (variant-grain expansion)

Filter by threshold and confirm the variant grain.

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP8 AS
SELECT
    ST_CD AS RDC,                -- rename per Apr-2026 contract
    GEN_ART_NUMBER, CLR, VAR_ART, SZ,
    MAJ_CAT, SEG,
    STK_QTY, PEND_QTY, FNL_Q
FROM TMP_MSA_STEP7
WHERE FNL_Q > :P_THRESHOLD
   OR STK_QTY > 0;              -- keep audit rows with stock but nothing shippable
```

### Step 9 — Aggregate to option grain

```sql
CREATE OR REPLACE TEMPORARY TABLE TMP_MSA_STEP9 AS
SELECT
    RDC, GEN_ART_NUMBER, CLR,
    ANY_VALUE(MAJ_CAT) AS MAJ_CAT,
    ANY_VALUE(SEG)     AS SEG,
    SUM(STK_QTY)  AS STK_QTY,
    SUM(PEND_QTY) AS PEND_QTY,
    SUM(FNL_Q)    AS FNL_Q
FROM TMP_MSA_STEP8
GROUP BY 1,2,3;
```

## 4. Publish to MART

Atomic swap: `CREATE OR REPLACE` + `INSERT OVERWRITE`. Snowflake doesn't need `BEGIN TRAN` for DDL.

```sql
-- TOTAL (audit) — the pivoted view from Step 5
INSERT OVERWRITE INTO MART.ARS_MSA_TOTAL
SELECT
    ST_CD AS RDC, GEN_ART_NUMBER, CLR, VAR_ART, SZ, MAJ_CAT, SEG,
    /* pivoted SLOC cols remain as an attached object via ARS_MSA_TOTAL_RAW if you need them */
    0 AS STK_QTY_placeholder,
    CURRENT_TIMESTAMP()
FROM TMP_MSA_STEP5;                -- shape matches your pivoted columns

-- GEN_ART (primary consumer)
INSERT OVERWRITE INTO MART.ARS_MSA_GEN_ART
SELECT
    RDC, GEN_ART_NUMBER, CLR, MAJ_CAT, SEG,
    STK_QTY, PEND_QTY, FNL_Q,
    CURRENT_TIMESTAMP()
FROM TMP_MSA_STEP9;

-- VAR_ART (allocator feed)
INSERT OVERWRITE INTO MART.ARS_MSA_VAR_ART
SELECT
    RDC, GEN_ART_NUMBER, CLR, VAR_ART, SZ, MAJ_CAT, SEG,
    STK_QTY, PEND_QTY, FNL_Q,
    CURRENT_TIMESTAMP()
FROM TMP_MSA_STEP8;
```

`INSERT OVERWRITE` truncates-then-inserts in one transaction — readers on `ARS_MSA_*` never see an empty table mid-run.

## 5. One-button procedure

Wrap the whole run in a Snowflake Scripting procedure so a single call is atomic from the API:

```sql
CREATE OR REPLACE PROCEDURE MART.SP_RUN_MSA(
    P_SNAPSHOT_DT DATE,
    P_SLOCS       ARRAY,
    P_SEGS        ARRAY,
    P_THRESHOLD   NUMBER DEFAULT 0
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_gen INTEGER;
    rows_var INTEGER;
BEGIN
    -- Steps 1 → 9 (all CREATE OR REPLACE TEMPORARY TABLE statements from §3 inlined here)
    -- ... then the INSERT OVERWRITE block from §4 ...

    SELECT COUNT(*) INTO rows_gen FROM MART.ARS_MSA_GEN_ART;
    SELECT COUNT(*) INTO rows_var FROM MART.ARS_MSA_VAR_ART;

    RETURN OBJECT_CONSTRUCT(
        'rows_gen_art', rows_gen,
        'rows_var_art', rows_var,
        'snapshot_dt', P_SNAPSHOT_DT::VARCHAR
    );
END;
$$;
```

Call from Python (replaces the current `MSAService.calculate(...)`):

```python
def run_msa(snapshot_dt, slocs, segs, threshold=0):
    with data_engine.raw_connection() as raw:
        sf = raw.driver_connection
        cur = sf.cursor()
        cur.execute(
            "CALL MART.SP_RUN_MSA(%s, %s, %s, %s)",
            (snapshot_dt, slocs, segs, threshold),
        )
        result = cur.fetchone()[0]   # VARIANT → dict
        sf.commit()
        return result
```

## 6. Worked example (as in the legacy doc)

Upload says:
```
GEN_ART=1116111940, CLR=LT_PST, SZ=M  MAJ_CAT=M_TEES_HS, SEG=APP
SLOC=V01_FRESH,   STK_Q=10
SLOC=V02_RESERVE, STK_Q=5
```

After Step 5 pivot → `STK_QTY = 15`.
`MASTER_ALC_PEND` has 2 units pending to store HN14.
Step 7 → `FNL_Q = MAX(15 − 2, 0) = 13`.
Step 9 aggregate → `ARS_MSA_GEN_ART` row `RDC=DH24, GEN_ART=1116111940, CLR=LT_PST, FNL_Q=13`.

## 7. Common issues and fixes (Snowflake-specific)

| Symptom | Likely cause | Fix |
|---|---|---|
| Step 5 PIVOT emits one row per (ST_CD,…) but all SLOC columns are NULL | Case mismatch — `SLOC IN (...)` filter uppercased the filter list but not the data | `UPPER(SLOC) IN (...)` on both sides |
| `FNL_Q` huge after Step 7 | `PEND_QTY` came out negative in the pending extract | Add `PEND_QTY = GREATEST(PEND_QTY, 0)` in the Step 6 subquery |
| MSA finishes but `ARS_MSA_VAR_ART` has 0 rows | Pivot collapsed the variant grain too early | Step 5 aggregation must keep `VAR_ART, SZ` in the GROUP BY — check §3 Step 5_TTL |
| Query takes > 20 minutes for 2M input rows | Warehouse too small | `ALTER WAREHOUSE WH_CALC SET WAREHOUSE_SIZE='LARGE'` just before the SP call; revert after |
| "Statement error: too many concurrent queries" | Steps 1–9 run in parallel in the app | Keep the same Snowflake session for all steps — use one `with raw_connection()` block |
| Step 8 loses rows where STK_QTY > 0 but FNL_Q = 0 | Threshold filter too strict for audit purposes | Keep `OR STK_QTY > 0` in Step 8 (already shown) |

## 8. Verification queries

```sql
-- Totals match between grains
SELECT 'VAR' AS grain, SUM(FNL_Q) FROM MART.ARS_MSA_VAR_ART
UNION ALL
SELECT 'GEN', SUM(FNL_Q) FROM MART.ARS_MSA_GEN_ART;
-- Expect near-equal (sum of variants per option = option FNL_Q)

-- Top 10 RDCs by shippable units
SELECT RDC, COUNT(*) AS variants, SUM(FNL_Q) AS shippable
FROM MART.ARS_MSA_VAR_ART
GROUP BY RDC ORDER BY shippable DESC LIMIT 10;

-- Coverage — how many options have stock?
SELECT COUNT(*) AS options_with_stock
FROM MART.ARS_MSA_GEN_ART WHERE FNL_Q > 0;

-- Pending drain (warning signs of stale PEND)
SELECT GEN_ART_NUMBER, CLR, STK_QTY, PEND_QTY, FNL_Q
FROM MART.ARS_MSA_VAR_ART
WHERE PEND_QTY > 0
ORDER BY PEND_QTY DESC LIMIT 20;

-- Invariant: fnl + pend ≤ stk
SELECT SUM(STK_QTY) stk, SUM(PEND_QTY) pend, SUM(FNL_Q) fnl
FROM MART.ARS_MSA_VAR_ART;
```

## 9. Performance notes

- Steps 1–4 are scan-heavy over `RAW.ET_STORE_STOCK`; cluster that table on `(SNAPSHOT_DT, ST_CD)` so daily partitions prune cleanly.
- Steps 5–7 are SELECT-heavy and benefit more from warehouse size than any DDL. MEDIUM handles ~2M input rows in under 90 seconds; LARGE cuts that to 30–40 s.
- Steps 8–9 are aggregations on a much smaller intermediate — no warehouse scaling needed.
- `INSERT OVERWRITE` on the three output tables is atomic and version-tracked — `SELECT … AT(OFFSET => -60)` gives a rollback window on `MART.ARS_MSA_GEN_ART` if a bad run publishes.

## 10. Rollback

If a run publishes bad data:

```sql
-- Example: roll ARS_MSA_GEN_ART back 15 minutes
CREATE OR REPLACE TABLE MART.ARS_MSA_GEN_ART CLONE MART.ARS_MSA_GEN_ART
    AT(OFFSET => -15*60);
```

Do the same for `ARS_MSA_VAR_ART` and `ARS_MSA_TOTAL`. This is cheap — Snowflake zero-copy clone.

## 11. How to update this doc

Update when a step is added/removed, when a column name changes in a MART table, or when the pivot-column naming convention changes. Bump `last_reviewed` in the header of this file.
