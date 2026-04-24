# Listing Process — Snowflake SOP

> Port of `backend/app/api/v1/endpoints/listing.py :: generate_listing` + `rule_engine.py` (Part 8 allocator) to Snowflake. The 8-part structure stays identical; only T-SQL → Snowflake SQL changes. Prerequisites: [00 README](00_README_Snowflake_Migration.md), [01 Data Model](01_data_model_and_masters.md), [02 MSA](02_msa_stock_calculation.md), [03 Grid Builder](03_grid_builder.md).

## 0. What Listing does (recap)

One button (`Generate`) runs **8 sequential parts**. The pipeline takes freshly prepared stock/sales/MSA/grid inputs and produces:

- `MART.ARS_LISTING` — every `(store × option × variant × size)` row classified as `MIX / RL / TBC / TBL`.
- `MART.ARS_LISTING_WORKING` — rows eligible for allocation (`MSA > 0 AND OPT_REQ_WH ≥ 1`), with grid coverage flags `PRI_CT%`, `SEC_CT%`, `ALLOC_FLAG`.
- `MART.ARS_STORE_RANKING` — per-store ranking used by the allocator.
- `MART.ARS_ALLOC_WORKING` — variant-grain allocation result: `SHIP_QTY`, `HOLD_QTY`, `ALLOC_STATUS`, `SKIP_REASON`.

## 1. Tunables

Matches the current request body. Defaults loaded from `MASTER.CONT_PRESETS` per `MAJ_CAT`; user overrides in request win.

| Param | Default | Meaning |
|---|---|---|
| `STOCK_PCT` | 0.60 | "Adequate stock" threshold → changes OPT_TYPE |
| `EXCESS_X` | 2.0 | Multiplier for article excess |
| `HOLD_D` | 15 | TBL hold buffer days |
| `AGE_D_THRESHOLD` | 15 | New-article age cutoff |
| `REQ_W` / `FILL_W` | 0.4 / 0.6 | Store-ranking weights |
| `ACS_D_FALLBACK` | 18 | Default daily sale when store has no history |
| `MIN_SZ` | 3 | Min filled variants for non-MIX |
| `FALLBACK_WAVE` | FALSE | Allow fallback wave for under-covered options |
| `SIZE_THRESHOLD` | 0.6 | Min size availability in allocator |

## 2. Single Snowflake session — why it matters

All parts run inside **one Snowflake session** so temporary tables and session state survive across steps. The orchestrator opens a connection once, runs parts 1–8, and releases it. Do not open a new connection per part.

```python
def generate_listing(req: GenerateRequest):
    with data_engine.raw_connection() as raw:
        sf = raw.driver_connection
        cur = sf.cursor()
        cur.execute("USE WAREHOUSE WH_CALC")
        cur.execute("ALTER SESSION SET QUERY_TAG='ars-listing'")
        try:
            part1(sf, req)
            part2(sf, req)
            part2_5(sf)
            part3_5a(sf, req); part3_5b(sf); part3_5c(sf); part3_55(sf)
            part3_6(sf, req)      # OPT_TYPE classification
            part3_7(sf, req)      # MIX consolidation
            part4_pre_resolve(sf)
            part4a(sf)            # grid joins
            part4b(sf); part4c(sf, req); part4d(sf, req); part4e(sf)
            part5(sf)
            part6(sf, req)        # store ranking
            part7(sf)             # working table
            part8_allocation(sf, req)
        finally:
            cur.close()
```

## 3. Part 1 — seed from variant grid

```sql
CREATE OR REPLACE TABLE MART.ARS_LISTING AS
SELECT
    v.WERKS, v.MAJ_CAT, v.GEN_ART_NUMBER, v.CLR, v.VAR_ART, v.SZ,
    st.RDC,
    v.VAR_STK_TTL AS STK_TTL,
    0::NUMBER(12,4) AS ACS_D,
    0::NUMBER(12,4) AS ALC_D,
    NULL::NUMBER(10) AS AGE_D,
    0::NUMBER(1)   AS IS_NEW,              -- existing article
    0::NUMBER(18,3) AS MSA_FNL_Q,
    0::NUMBER(10)   AS VAR_COUNT,
    0::NUMBER(10)   AS VAR_FNL_COUNT,
    NULL::VARCHAR(8) AS OPT_TYPE,
    NULL::VARCHAR(8) AS FINAL_OPT_TYPE,
    CURRENT_TIMESTAMP() AS BUILT_TS
FROM MART.ARS_GRID_MJ_VAR_ART v
LEFT JOIN MASTER.MASTER_STORE st ON st.WERKS = v.WERKS;
```

## 4. Part 2 — add MSA-only rows (new launches)

Articles that exist in warehouse stock (MSA > 0) but not at any store yet.

```sql
INSERT INTO MART.ARS_LISTING (WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
                              RDC, STK_TTL, MSA_FNL_Q, IS_NEW)
SELECT
    s.WERKS, m.MAJ_CAT, m.GEN_ART_NUMBER, m.CLR, m.VAR_ART, m.SZ,
    st.RDC, 0, m.FNL_Q, 1
FROM MASTER.MASTER_STORE s
CROSS JOIN MART.ARS_MSA_VAR_ART m
LEFT JOIN MASTER.MASTER_STORE st ON st.WERKS = s.WERKS
LEFT JOIN MART.ARS_LISTING l
       ON l.WERKS = s.WERKS
      AND l.GEN_ART_NUMBER = m.GEN_ART_NUMBER
      AND l.CLR = m.CLR
      AND l.VAR_ART = m.VAR_ART
      AND l.SZ = m.SZ
WHERE l.WERKS IS NULL                           -- not already in listing
  AND m.RDC  = st.RDC                           -- matching servicing warehouse
  AND m.FNL_Q > 0;
```

## 5. Part 2.5 — clustering hint

Snowflake has no indexes, but on very large listings add a cluster key or re-cluster:

```sql
ALTER TABLE MART.ARS_LISTING CLUSTER BY (WERKS, MAJ_CAT);
-- Optional:  ALTER TABLE MART.ARS_LISTING SUSPEND RECLUSTER; RESUME;
```

## 6. Part 3.5a–3.5c — calc-table enrichment

Pull `ACS_D, ALC_D, FOCUS_FLAG` from the cascade table, and auto sale + article age.

```sql
-- 3.5a: store × MAJ_CAT numbers
UPDATE MART.ARS_LISTING l
SET ACS_D = c.ACS_D, ALC_D = c.ALC_D
FROM MART.ARS_CALC_ST_MAJ_CAT c
WHERE c.WERKS = l.WERKS AND c.MAJ_CAT = l.MAJ_CAT;

-- 3.5b: auto sale per option
UPDATE MART.ARS_LISTING l
SET ACS_D = COALESCE(NULLIF(l.ACS_D, 0), s.AUTO_SALE_D, 0)
FROM MASTER.MASTER_GEN_ART_SALE s
WHERE s.GEN_ART_NUMBER = l.GEN_ART_NUMBER AND s.CLR = l.CLR;

-- 3.5c: article age
UPDATE MART.ARS_LISTING l
SET AGE_D = a.AGE_D
FROM MASTER.MASTER_GEN_ART_AGE a
WHERE a.GEN_ART_NUMBER = l.GEN_ART_NUMBER;
```

## 7. Part 3.55 — attach MSA_FNL_Q (variant grain)

```sql
UPDATE MART.ARS_LISTING l
SET MSA_FNL_Q = m.FNL_Q
FROM MART.ARS_MSA_VAR_ART m
WHERE m.RDC = l.RDC
  AND m.GEN_ART_NUMBER = l.GEN_ART_NUMBER
  AND m.CLR  = l.CLR
  AND m.VAR_ART = l.VAR_ART
  AND m.SZ   = l.SZ;

-- Variant counts per option (used by MIX rule)
UPDATE MART.ARS_LISTING l
SET VAR_COUNT     = vc.VAR_COUNT,
    VAR_FNL_COUNT = vc.VAR_FNL_COUNT
FROM (
    SELECT WERKS, GEN_ART_NUMBER, CLR,
           COUNT(*)                                         AS VAR_COUNT,
           COUNT_IF(MSA_FNL_Q > 0 OR STK_TTL > 0)           AS VAR_FNL_COUNT
    FROM MART.ARS_LISTING
    GROUP BY 1,2,3
) vc
WHERE vc.WERKS = l.WERKS
  AND vc.GEN_ART_NUMBER = l.GEN_ART_NUMBER
  AND vc.CLR = l.CLR;
```

## 8. Part 3.6 — OPT_TYPE classification (MIX / RL / TBC / TBL)

Single `UPDATE` with a 10-branch `CASE`. The `MIN_SZ` rule is conditional — skip when `P_MIN_SZ = 0`.

```sql
UPDATE MART.ARS_LISTING l
SET OPT_TYPE = CASE
    -- Rule 1: almost out AND warehouse dry
    WHEN l.STK_TTL < (:P_STOCK_PCT * COALESCE(NULLIF(l.ACS_D, 0), :P_ACS_D_FALLBACK))
         AND l.MSA_FNL_Q = 0                              THEN 'MIX'

    -- Rule 2: too few filled sizes
    WHEN l.VAR_COUNT > 0 AND
         (l.VAR_FNL_COUNT::FLOAT / l.VAR_COUNT) < :P_STOCK_PCT THEN 'MIX'
    WHEN l.VAR_FNL_COUNT < :P_MIN_SZ AND :P_MIN_SZ > 0   THEN 'MIX'

    -- Rule 3: well-stocked
    WHEN l.STK_TTL >= (:P_STOCK_PCT * COALESCE(NULLIF(l.ACS_D, 0), :P_ACS_D_FALLBACK))
                                                         THEN 'RL'

    -- Rule 4: some stock, below target, warehouse has stock
    WHEN l.STK_TTL > 0 AND l.MSA_FNL_Q > 0               THEN 'TBC'

    -- Rule 5: empty store, warehouse has stock
    WHEN l.STK_TTL <= 0 AND l.MSA_FNL_Q > 0              THEN 'TBL'

    -- Safety-net branches
    WHEN l.MSA_FNL_Q = 0 AND l.STK_TTL = 0               THEN 'MIX'
    WHEN l.MSA_FNL_Q = 0 AND l.STK_TTL > 0               THEN 'RL'
    WHEN l.MSA_FNL_Q > 0 AND l.STK_TTL > 0               THEN 'TBC'
    WHEN l.MSA_FNL_Q > 0 AND l.STK_TTL <= 0              THEN 'TBL'
    ELSE 'MIX'
END;
```

Bind params (Python):

```python
cur.execute(sql, {
    "P_STOCK_PCT":       req.stock_pct,
    "P_ACS_D_FALLBACK":  req.acs_d_fallback,
    "P_MIN_SZ":          req.min_size_count,
})
```

## 9. Part 3.7 — MIX consolidation (max 1 row per store × MAJ_CAT)

```sql
-- Sum numeric fields into a single MIX row per (WERKS, MAJ_CAT)
CREATE OR REPLACE TEMPORARY TABLE TMP_MIX_AGG AS
SELECT
    WERKS, MAJ_CAT,
    SUM(STK_TTL)   AS STK_TTL,
    SUM(MSA_FNL_Q) AS MSA_FNL_Q,
    MIN(RDC)       AS RDC
FROM MART.ARS_LISTING
WHERE OPT_TYPE = 'MIX'
GROUP BY WERKS, MAJ_CAT;

DELETE FROM MART.ARS_LISTING WHERE OPT_TYPE = 'MIX';

INSERT INTO MART.ARS_LISTING (WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
                              RDC, STK_TTL, MSA_FNL_Q, OPT_TYPE, IS_NEW)
SELECT WERKS, MAJ_CAT, 0, '_MIX_', 0, '_MIX_',
       RDC, STK_TTL, MSA_FNL_Q, 'MIX', 0
FROM TMP_MIX_AGG;
```

## 10. Part 4 pre-resolve + 4a — grid attribute & metric joins

Pull product attrs from master, then join each active grid.

```sql
-- Pre-resolve product attributes
UPDATE MART.ARS_LISTING l
SET FAB        = p.FAB,
    RNG_SEG    = p.RNG_SEG,
    MACRO_MVGR = p.MACRO_MVGR,
    MICRO_MVGR = p.MICRO_MVGR,
    M_VND_CD   = p.M_VND_CD,
    COL_FAM    = p.COL_FAM,
    SSN        = p.SSN,
    SEG        = p.SEG
FROM MASTER.VW_MASTER_PRODUCT p
WHERE p.GEN_ART_NUMBER = l.GEN_ART_NUMBER AND p.CLR = l.CLR;
```

For Part 4a (the grid joins), iterate active grids in the orchestrator and `UPDATE ... FROM ARS_GRID_<name>`:

```python
grids = cur.execute("""
    SELECT GRID_NAME, HIERARCHY_COLUMNS, OUTPUT_PREFIX, GRID_GROUP
    FROM MASTER.ARS_GRID_BUILDER WHERE STATUS='ACTIVE' ORDER BY SEQ
""").fetchall()

for grid_name, hier, pfx, group_ in grids:
    join_cond = " AND ".join(f"g.{c}=l.{c}" for c in hier)
    set_cols = ", ".join(
        f"{pfx}_{m} = g.{pfx}_{m}" for m in ("STK_TTL","STR","CONT","MBQ","OPT_CNT","DISP_Q")
    )
    cur.execute(f"""
        UPDATE MART.ARS_LISTING l
        SET {set_cols}
        FROM MART.ARS_GRID_{grid_name} g
        WHERE {join_cond}
    """)
```

**Schema note:** `ARS_LISTING` must have those `<PFX>_*` columns. Create them once up-front after Part 1 using `ALTER TABLE ADD COLUMN IF NOT EXISTS` driven by the same grid config.

## 11. Parts 4b – 4e — demand math

```sql
-- 4b: per-option expected sale (from grid flagged USE_FOR_OPT_SALE=1, usually MJ_RNG_SEG)
UPDATE MART.ARS_LISTING l
SET PER_OPT_SALE = CASE WHEN g.RNG_SEG_OPT_CNT > 0
                         THEN g.RNG_SEG_DISP_Q / g.RNG_SEG_OPT_CNT
                         ELSE 0 END
FROM MART.ARS_GRID_MJ_RNG_SEG g
WHERE g.WERKS = l.WERKS AND g.MAJ_CAT = l.MAJ_CAT AND g.RNG_SEG = l.RNG_SEG;

-- 4c: core demand numbers
UPDATE MART.ARS_LISTING l
SET MAX_DAILY_SALE = GREATEST(l.ACS_D, l.PER_OPT_SALE),
    OPT_MBQ        = CEIL(l.MAX_DAILY_SALE * l.ALC_D),
    OPT_REQ        = GREATEST(CEIL(l.MAX_DAILY_SALE * l.ALC_D) - l.STK_TTL, 0),
    OPT_MBQ_WH     = CEIL(l.MAX_DAILY_SALE * (l.ALC_D + :P_HOLD_D)),
    OPT_REQ_WH     = GREATEST(CEIL(l.MAX_DAILY_SALE * (l.ALC_D + :P_HOLD_D)) - l.STK_TTL, 0);

-- 4d: article-level excess (MIX excluded)
UPDATE MART.ARS_LISTING l
SET ART_EXCESS = GREATEST(l.STK_TTL - (:P_EXCESS_X * l.OPT_MBQ), 0)
WHERE l.OPT_TYPE != 'MIX';

-- 4e: per-grid level REQ (demand gap after excess)
-- For each active secondary grid g:
--   <PFX>_REQ = GREATEST(<PFX>_MBQ - <PFX>_STK_TTL, 0)
-- Example for FAB:
UPDATE MART.ARS_LISTING l
SET FAB_REQ = GREATEST(l.FAB_MBQ - l.FAB_STK_TTL, 0);
```

## 12. Part 6 — store ranking

```sql
CREATE OR REPLACE TABLE MART.ARS_STORE_RANKING AS
WITH base AS (
    SELECT
        l.WERKS, l.MAJ_CAT,
        SUM(l.OPT_REQ) AS MJ_REQ,
        SUM(l.STK_TTL) AS MJ_STK_TTL,
        SUM(l.OPT_MBQ) AS MJ_TARGET
    FROM MART.ARS_LISTING l
    WHERE l.OPT_TYPE IN ('RL','TBC','TBL')
    GROUP BY l.WERKS, l.MAJ_CAT
),
scored AS (
    SELECT b.*,
           CASE WHEN b.MJ_TARGET > 0
                THEN 1.0 - (b.MJ_STK_TTL / b.MJ_TARGET)
                ELSE 0 END                           AS FILL_GAP_PCT,
           :P_REQ_W  * b.MJ_REQ                      AS REQ_SCORE,
           :P_FILL_W * 100 * CASE WHEN b.MJ_TARGET > 0
                                    THEN 1.0 - b.MJ_STK_TTL / b.MJ_TARGET
                                    ELSE 0 END       AS FILL_SCORE
    FROM base b
)
SELECT
    WERKS, MAJ_CAT, MJ_REQ, MJ_STK_TTL, FILL_GAP_PCT AS FILL_PCT,
    REQ_SCORE, FILL_SCORE,
    REQ_SCORE + FILL_SCORE AS RANK_SCORE,
    ROW_NUMBER() OVER (PARTITION BY MAJ_CAT
                       ORDER BY REQ_SCORE + FILL_SCORE DESC) AS ST_RANK,
    CURRENT_TIMESTAMP() AS BUILT_TS
FROM scored;
```

## 13. Part 7 — build `ARS_LISTING_WORKING` + coverage flags

```sql
CREATE OR REPLACE TABLE MART.ARS_LISTING_WORKING AS
SELECT l.*,
       /* Hierarchy dimensions carried from MASTER.VW_MASTER_PRODUCT so
          the allocator + UI have full grid context without re-joining.
          Every grid defined in ARS_GRID_BUILDER must find its hierarchy
          column here, else PRI_CT%/SEC_CT% calc skips it silently. */
       p.SEG,
       p.RNG_SEG,
       p.MACRO_MVGR,
       p.MICRO_MVGR,
       p.FAB,
       p.M_VND_CD,
       p.COL_FAM,
       p.SSN,
       /* PRI_CT% = weighted coverage of Primary grids */
       CASE WHEN l.MJ_MBQ > 0 THEN (l.MJ_STK_TTL / l.MJ_MBQ) * 100.0 ELSE 0 END AS PRI_CT_PCT,
       /* SEC_CT% = weighted coverage of Secondary grids (avg) */
       (COALESCE(IFF(l.FAB_MBQ > 0,  l.FAB_STK_TTL/l.FAB_MBQ,  0), 0)
      + COALESCE(IFF(l.CLR_MBQ > 0,  l.CLR_STK_TTL/l.CLR_MBQ,  0), 0)
      + COALESCE(IFF(l.RNG_SEG_MBQ>0,l.RNG_SEG_STK_TTL/l.RNG_SEG_MBQ,0), 0)
      + COALESCE(IFF(l.M_VND_CD_MBQ>0,l.M_VND_CD_STK_TTL/l.M_VND_CD_MBQ,0), 0)
       )/4.0 * 100.0                                                             AS SEC_CT_PCT,
       IFF(l.OPT_TYPE IN ('RL','TBC','TBL') AND l.MSA_FNL_Q > 0
           AND l.OPT_REQ_WH >= 1, 1, 0)                                          AS ALLOC_FLAG,
       0::NUMBER(18,3)                                                           AS ALLOC_QTY,
       sr.ST_RANK                                                                AS ST_RANK
FROM MART.ARS_LISTING l
LEFT JOIN MASTER.VW_MASTER_PRODUCT p
       ON p.GEN_ART_NUMBER = l.GEN_ART_NUMBER
      AND p.CLR            = l.CLR
LEFT JOIN MART.ARS_STORE_RANKING sr
       ON sr.WERKS = l.WERKS AND sr.MAJ_CAT = l.MAJ_CAT
WHERE l.MSA_FNL_Q > 0 AND l.OPT_REQ_WH >= 1;

ALTER TABLE MART.ARS_LISTING_WORKING CLUSTER BY (MAJ_CAT, ST_RANK);
```

**Hierarchy columns added (all nullable, populated via `VW_MASTER_PRODUCT` lookup on `GEN_ART_NUMBER + CLR`):**

| Column | Purpose | Used by grid |
|---|---|---|
| `SEG` | Top-level segment | any grid built on SEG |
| `RNG_SEG` | Range × segment | `RNG_SEG_*` grid |
| `MACRO_MVGR` | Macro merchandise group | `MACRO_MVGR_*` grid |
| `MICRO_MVGR` | Micro merchandise group | `MICRO_MVGR_*` grid |
| `FAB` | Fabric | `FAB_*` grid (secondary) |
| `M_VND_CD` | Master vendor code | `M_VND_CD_*` grid |
| `COL_FAM` | Colour family | `COL_FAM_*` grid |
| `SSN` | Season | `SSN_*` grid |

If a new grid hierarchy is added to `ARS_GRID_BUILDER`, add the matching column to this `SELECT` *before* the grid can be used in `PRI_CT%` / `SEC_CT%` computation.

## 14. Part 8 — Rule-Engine allocation (waves × types × rounds)

Port of `rule_engine.py`. Structure:

- **Waves** (outer loop): `PRI_100`, `PRI_80`, `SEC_100`, `SEC_80` — coverage gates.
- **OPT_TYPES** (middle loop): `RL`, `TBC`, `TBL`.
- **Rounds** (inner loop): 1 … N, where N is per-option `I_ROD` from `ARS_CALC_ST_GEN_ART`.

Each (wave × type × round) iteration does five things: scale demand, size-contribution enforcement, waterfall by `ST_RANK`, commit `SHIP_QTY`/`HOLD_QTY`, refresh the pool.

### 14.1 Step 1–4 — build working set + pool

```sql
-- Step 1: variant-grain working rows (one row per WERKS × VAR_ART × SZ eligible)
CREATE OR REPLACE TABLE MART.ARS_ALLOC_WORKING AS
SELECT
    w.WERKS, w.MAJ_CAT, w.GEN_ART_NUMBER, w.CLR, w.VAR_ART, w.SZ,
    w.RDC, w.OPT_TYPE, w.ST_RANK, w.MSA_FNL_Q, w.STK_TTL,
    cs.SZ_CONT_PCT,
    0::NUMBER(18,3) AS SHIP_QTY,
    0::NUMBER(18,3) AS HOLD_QTY,
    0::NUMBER(18,3) AS POOL_CONSUMED,
    'INELIGIBLE'::VARCHAR(20) AS ALLOC_STATUS,
    NULL::VARCHAR(200) AS SKIP_REASON,
    0::NUMBER(5) AS ALLOC_ROUND,
    NULL::VARCHAR(10) AS WAVE,
    'NORMAL'::VARCHAR(10) AS FOCUS_FLAG
FROM MART.ARS_LISTING_WORKING w
LEFT JOIN MASTER.MASTER_CONT_SZ cs
       ON cs.MAJ_CAT = w.MAJ_CAT AND cs.SZ = w.SZ
WHERE w.ALLOC_FLAG = 1;

-- Step 5: shrinking pool — per variant (RDC × GEN_ART × CLR × VAR_ART × SZ)
CREATE OR REPLACE TEMPORARY TABLE RULE_POOL AS
SELECT RDC, GEN_ART_NUMBER, CLR, VAR_ART, SZ, FNL_Q AS POOL_Q
FROM MART.ARS_MSA_VAR_ART
WHERE FNL_Q > 0;

-- Step 6: flip MIX / unlisted to 0 (they never qualified anyway; belt-and-braces)
UPDATE MART.ARS_ALLOC_WORKING SET ALLOC_STATUS='INELIGIBLE'
WHERE OPT_TYPE NOT IN ('RL','TBC','TBL');
```

### 14.2 Step 7 — the nested loops

Snowflake Scripting can express this directly; alternatively keep the loop in Python and fire set-based SQL per iteration (clearer, matches current code).

```python
WAVES = [
    ("PRI_100", "PRI_CT_PCT", 100.0),
    ("PRI_80",  "PRI_CT_PCT",  80.0),
    ("SEC_100", "SEC_CT_PCT", 100.0),
    ("SEC_80",  "SEC_CT_PCT",  80.0),
]
OPT_TYPE_ORDER = ["RL", "TBC", "TBL"]
MAX_ROUNDS = 6

def part8_allocation(sf, req):
    cur = sf.cursor()
    for wave, col, thresh in WAVES:
        for otype in OPT_TYPE_ORDER:
            for rd in range(1, MAX_ROUNDS + 1):
                # 7a: scale demand for this round — commits happen per round via SQL
                cur.execute(_round_sql(wave, col, thresh, otype, rd,
                                       size_threshold=req.size_threshold,
                                       hold_days=req.hold_d))
                rows_changed = cur.rowcount
                if rows_changed == 0:
                    break   # nothing more to allocate for this (wave, type)
```

`_round_sql(...)` returns one MERGE statement per iteration. The statement:

1. Computes wanted quantity per (WERKS × VAR × SZ) for this round's scale.
2. Joins `RULE_POOL` to clamp to what's available **at variant+size grain**.
3. Applies `SIZE_THRESHOLD` cut — skip if fewer than 60% of the option's sizes have pool left for this store (matches `B0:POOL_DRAINED_AT_SIZE`).
4. Writes `SHIP_QTY` (and `HOLD_QTY` for TBL).
5. Subtracts consumed units from `RULE_POOL`.
6. Orders stores by `ST_RANK` using `ROW_NUMBER() OVER(PARTITION BY variant+size ORDER BY ST_RANK)`.

Sketch of the MERGE (one wave × type × round):

```sql
-- Round R for wave W, opt_type OT, threshold TH on column C (PRI_CT_PCT or SEC_CT_PCT)
WITH candidate AS (
    SELECT w.*,
           CEIL(w.MSA_FNL_Q * w.SZ_CONT_PCT * :round_factor) AS WANT_Q
    FROM MART.ARS_LISTING_WORKING w
    WHERE w.OPT_TYPE = :otype
      AND w.ALLOC_FLAG = 1
      AND w.{col} >= :thresh
      AND NOT EXISTS (   -- not already allocated in prior round
          SELECT 1 FROM MART.ARS_ALLOC_WORKING a
          WHERE a.WERKS = w.WERKS AND a.VAR_ART = w.VAR_ART AND a.SZ = w.SZ
            AND a.ALLOC_STATUS = 'ALLOCATED'
      )
),
waterfall AS (
    /* Rank candidates within each variant+size pool by ST_RANK;
       Snowflake's ROW_NUMBER keeps order stable across ties. */
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY c.RDC, c.GEN_ART_NUMBER, c.CLR, c.VAR_ART, c.SZ
               ORDER BY c.ST_RANK, c.WERKS
           ) AS DRAW_ORDER
    FROM candidate c
),
running AS (
    SELECT w.*,
           p.POOL_Q,
           -- consumed-so-far up to but not including this row
           COALESCE(SUM(w.WANT_Q) OVER (
               PARTITION BY w.RDC, w.GEN_ART_NUMBER, w.CLR, w.VAR_ART, w.SZ
               ORDER BY w.DRAW_ORDER
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ), 0) AS PRIOR_DRAW
    FROM waterfall w
    JOIN RULE_POOL p
      ON p.RDC = w.RDC AND p.GEN_ART_NUMBER = w.GEN_ART_NUMBER
     AND p.CLR = w.CLR AND p.VAR_ART = w.VAR_ART AND p.SZ = w.SZ
),
decision AS (
    SELECT WERKS, VAR_ART, SZ,
           GREATEST(LEAST(WANT_Q, POOL_Q - PRIOR_DRAW), 0) AS DRAW_Q,
           POOL_Q - PRIOR_DRAW                               AS POOL_REMAIN_BEFORE,
           WANT_Q
    FROM running
)
MERGE INTO MART.ARS_ALLOC_WORKING t
USING decision d
  ON t.WERKS = d.WERKS AND t.VAR_ART = d.VAR_ART AND t.SZ = d.SZ
WHEN MATCHED AND d.DRAW_Q > 0 THEN UPDATE SET
    SHIP_QTY      = t.SHIP_QTY + (CASE WHEN t.OPT_TYPE='TBL' THEN d.DRAW_Q * (1 - :hold_ratio) ELSE d.DRAW_Q END),
    HOLD_QTY      = t.HOLD_QTY + (CASE WHEN t.OPT_TYPE='TBL' THEN d.DRAW_Q *  :hold_ratio     ELSE 0 END),
    POOL_CONSUMED = t.POOL_CONSUMED + d.DRAW_Q,
    ALLOC_STATUS  = IFF(d.DRAW_Q >= d.WANT_Q, 'ALLOCATED', 'PARTIAL'),
    ALLOC_ROUND   = :round_no,
    WAVE          = :wave
WHEN MATCHED AND d.DRAW_Q = 0 AND t.ALLOC_STATUS = 'INELIGIBLE' THEN UPDATE SET
    SKIP_REASON = 'B0:POOL_DRAINED_AT_SIZE@' || :wave;

-- Shrink the pool
UPDATE RULE_POOL p
SET POOL_Q = p.POOL_Q - drawn.d
FROM (
    SELECT RDC, GEN_ART_NUMBER, CLR, VAR_ART, SZ, SUM(DRAW_Q) AS d
    FROM decision GROUP BY 1,2,3,4,5
) drawn
WHERE p.RDC = drawn.RDC AND p.GEN_ART_NUMBER = drawn.GEN_ART_NUMBER
  AND p.CLR = drawn.CLR AND p.VAR_ART = drawn.VAR_ART AND p.SZ = drawn.SZ;
```

`:round_factor` starts at `1.0` for round 1 and scales up for later rounds (e.g. `1 + 0.5*(round-1)`), matching the "top up to N days' worth" behaviour.

`:hold_ratio` = `HOLD_D / (ALC_D + HOLD_D)` for TBL rows (split ship vs. hold).

**Size-break guard.** Before committing a round, skip options where fewer than `SIZE_THRESHOLD` of the option's sizes still have pool. The check can be a `HAVING` on the candidate CTE:

```sql
-- In the candidate CTE, per (WERKS, GEN_ART_NUMBER, CLR):
-- keep only if COUNT_IF(POOL_Q > 0) / COUNT(*) >= :size_threshold
```

### 14.3 Step 8 — reflect SHIP_QTY back to `ARS_LISTING_WORKING.ALLOC_QTY`

```sql
UPDATE MART.ARS_LISTING_WORKING w
SET ALLOC_QTY = COALESCE(s.ship_sum, 0)
FROM (
    SELECT WERKS, GEN_ART_NUMBER, CLR, SUM(SHIP_QTY) AS ship_sum
    FROM MART.ARS_ALLOC_WORKING
    WHERE SHIP_QTY > 0
    GROUP BY WERKS, GEN_ART_NUMBER, CLR
) s
WHERE s.WERKS = w.WERKS AND s.GEN_ART_NUMBER = w.GEN_ART_NUMBER AND s.CLR = w.CLR;
```

### 14.4 Step 9 — tally and return

```python
summary = cur.execute("""
    SELECT SUM(SHIP_QTY) ship_total, SUM(HOLD_QTY) hold_total,
           COUNT(*) alloc_rows,
           COUNT_IF(ALLOC_STATUS='ALLOCATED') alloc_ok,
           COUNT_IF(ALLOC_STATUS='PARTIAL')   alloc_partial,
           COUNT_IF(ALLOC_STATUS='SKIPPED')   alloc_skipped
    FROM MART.ARS_ALLOC_WORKING
""").fetchone()
```

Temp tables auto-drop at session end; no cleanup needed.

## 15. End-to-end driver procedure

Wrap everything in a single Snowflake Scripting proc so the Python API call is one statement:

```sql
CREATE OR REPLACE PROCEDURE MART.SP_GENERATE_LISTING(
    P_STOCK_PCT      NUMBER DEFAULT 0.6,
    P_HOLD_D         NUMBER DEFAULT 15,
    P_EXCESS_X       NUMBER DEFAULT 2.0,
    P_ACS_D_FALLBACK NUMBER DEFAULT 18,
    P_MIN_SZ         NUMBER DEFAULT 3,
    P_REQ_W          NUMBER DEFAULT 0.4,
    P_FILL_W         NUMBER DEFAULT 0.6,
    P_SIZE_THRESHOLD NUMBER DEFAULT 0.6,
    P_FALLBACK       BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Part 1 … Part 8 SQL inlined. Between parts use PROCEDURE-level logging:
    -- INSERT INTO AUDIT.JOB_RUNS(JOB_NAME, STATUS, ROWS_AFFECTED) ...

    RETURN OBJECT_CONSTRUCT(
        'rows_listing',  (SELECT COUNT(*) FROM MART.ARS_LISTING),
        'rows_working',  (SELECT COUNT(*) FROM MART.ARS_LISTING_WORKING),
        'rows_alloc',    (SELECT COUNT(*) FROM MART.ARS_ALLOC_WORKING WHERE SHIP_QTY > 0),
        'total_shipped', (SELECT COALESCE(SUM(SHIP_QTY),0) FROM MART.ARS_ALLOC_WORKING),
        'total_held',    (SELECT COALESCE(SUM(HOLD_QTY),0) FROM MART.ARS_ALLOC_WORKING)
    );
END;
$$;
```

Python:

```python
cur.execute("""
    CALL MART.SP_GENERATE_LISTING(
        %(stock)s, %(hold)s, %(excess)s, %(acs)s,
        %(minsz)s, %(reqw)s, %(fillw)s, %(sizeth)s, %(fallback)s
    )
""", {
    "stock": req.stock_pct, "hold": req.hold_d, "excess": req.excess_x,
    "acs":   req.acs_d_fallback, "minsz": req.min_size_count,
    "reqw":  req.req_weight, "fillw": req.fill_weight,
    "sizeth":req.size_threshold, "fallback": req.fallback_wave,
})
```

## 16. Verification

```sql
-- Part 3.6 tag distribution
SELECT OPT_TYPE, COUNT(*) AS rows_, COUNT(DISTINCT WERKS) AS stores_
FROM MART.ARS_LISTING GROUP BY OPT_TYPE ORDER BY rows_ DESC;

-- Working vs full
SELECT 'LISTING'        AS t, COUNT(*) FROM MART.ARS_LISTING
UNION ALL SELECT 'WORKING',   COUNT(*) FROM MART.ARS_LISTING_WORKING
UNION ALL SELECT 'ALLOC_OK',  COUNT(*) FROM MART.ARS_ALLOC_WORKING WHERE SHIP_QTY > 0;

-- Sync: sum(ALLOC_QTY) = sum(SHIP_QTY)
SELECT (SELECT SUM(ALLOC_QTY) FROM MART.ARS_LISTING_WORKING) AS working_sum,
       (SELECT SUM(SHIP_QTY)  FROM MART.ARS_ALLOC_WORKING)   AS alloc_sum;

-- Top-10 receivers
SELECT WERKS, SUM(SHIP_QTY) units, COUNT(DISTINCT MAJ_CAT) cats
FROM MART.ARS_ALLOC_WORKING WHERE SHIP_QTY > 0
GROUP BY WERKS ORDER BY units DESC LIMIT 10;

-- Stores that got nothing despite being in working
SELECT DISTINCT WERKS FROM MART.ARS_LISTING_WORKING
EXCEPT
SELECT DISTINCT WERKS FROM MART.ARS_ALLOC_WORKING WHERE SHIP_QTY > 0;
```

## 17. Common issues on Snowflake

| Symptom | Likely cause | Fix |
|---|---|---|
| Part 3.6 `untagged > 0` | NULL in `STK_TTL`, `ACS_D`, or `MSA_FNL_Q` | Final ELSE already catches to 'MIX'; if count non-zero it means Part 3.55 didn't fill `MSA_FNL_Q` — re-run MSA |
| Part 7 has 0 rows | Either `MSA_FNL_Q=0` everywhere or `OPT_REQ_WH<1` | Check cascade `ACS_D`/`ALC_D`; verify MSA |
| Part 8 completes but `SHIP_QTY` sum is 0 | `ALLOC_FLAG=0` on all rows | Verify `PRI_CT_PCT` calculation in Part 7 — column names on `ARS_LISTING_WORKING` must match grid prefixes |
| Part 8 times out at > 10 min on ~50K rows | Warehouse too small; or too many rounds with empty waterfall | Bump `WH_CALC` to LARGE; ensure each round's SQL returns `rowcount=0` to break early |
| Huge spike in Snowflake credit usage | Every iteration re-opens a warehouse | Keep WH running during Part 8; `AUTO_SUSPEND=60s` is fine but don't end the session prematurely |
| `Numeric value is out of range` | `ceil(MAX_DAILY_SALE * ALC_D)` overflows NUMBER(18,3) | Cast intermediates to NUMBER(38, 6) — Snowflake default precision is 38 |
| Results differ from SQL Server | `ORDER BY` ties in the waterfall tiebreak | The SQL above ties on `ST_RANK, WERKS` to make the order deterministic; confirm your SQL Server version was also deterministic |

## 18. Snowflake-only performance knobs

- **Result cache** — repeated reads of `ARS_LISTING_WORKING` by UI inherit the 24-h result cache automatically. Nothing to do.
- **Query tag** — `ALTER SESSION SET QUERY_TAG = 'ars-listing:' || SYSDATE()` makes cost attribution easy in `ACCOUNT_USAGE.QUERY_HISTORY`.
- **Warehouse scaling** — run Part 8 on LARGE for a full 320-store / 200-MAJCAT weekly run; MEDIUM is fine for incremental single-MAJCAT runs.
- **Time travel** — every published table has a 7-day window. Rollback of a bad Generate is `CREATE OR REPLACE TABLE X CLONE X AT(OFFSET => -<seconds>)`.

## 19. End-to-end runbook

```
USER clicks "Generate"  →  POST /api/v1/listing/generate
FastAPI handler         →  CALL MART.SP_GENERATE_LISTING(...)
Snowflake               →  Parts 1…8 run in one session
AUDIT.JOB_RUNS          →  rows STARTED / SUCCESS written
Response                →  JSON with rows_listing, rows_working, total_shipped, total_held
UI                       →  refresh Working / Full / Alloc tabs
```

## 20. How to update this doc

Update when a Part is added/removed/reordered, when wave or OPT_TYPE order changes, when a new `ALLOC_STATUS` value is introduced, or when the hold-split formula changes.
