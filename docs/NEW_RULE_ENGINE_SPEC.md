# New Rule Engine — Spec & Full Process

**Scope** — everything that happens **after** `ARS_LISTING_WORKING` is built. The listing working table is treated as a frozen input. This spec replaces the combined responsibilities of the old `app.services.rule_engine.py` and `app.services.listing_allocator.py`.

**Old files** — `rule_engine.py` and `listing_allocator.py` are left intact as reference. The listing endpoint will no longer call them (Part 8 of `/listing/generate` is re-wired to the new engine).

**New file** — `backend/app/services/rule_engine_new.py`

**New entry point** — `run_listing_and_allocation(conn, working_table, listed_table, alloc_table, ...)`

---

## 0. Pipeline in one diagram

```
ARS_LISTING_WORKING (one row per OPT = WERKS × MAJ × GEN × CLR, already built)
        │
        ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ STAGE A — LIST ART                                             │
 │   Decide which OPTs are LISTED (eligible for pool).            │
 │   Writes:  ARS_LISTED_OPT                                      │
 │            + LISTED_FLAG / LISTED_REASON / OPT_PRIORITY_RANK   │
 │              back onto ARS_LISTING_WORKING                     │
 └────────────────────────────────────────────────────────────────┘
        │
        ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ STAGE B — EXPLODE TO VAR_ART × SZ                              │
 │   Join listed OPTs with ARS_MSA_VAR_ART and Master_CONT_SZ.    │
 │   Writes:  ARS_ALLOC_WORKING (one row per OPT × VAR_ART × SZ)  │
 └────────────────────────────────────────────────────────────────┘
        │
        ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ STAGE C — ALLOCATE VAR_ART × SZ (pool waterfall)               │
 │   RL → TBC → TBL, I_ROD rounds, rank-banded.                   │
 │   Writes: ALLOC_QTY / SHIP_QTY / HOLD_QTY on ARS_ALLOC_WORKING │
 └────────────────────────────────────────────────────────────────┘
        │
        ▼
 ┌────────────────────────────────────────────────────────────────┐
 │ STAGE D — REFLECT & AUDIT                                      │
 │   Aggregate ALLOC_QTY back to OPT rows on ARS_LISTING_WORKING. │
 │   Stamp ALLOC_STATUS, ALLOC_REMARKS.                           │
 └────────────────────────────────────────────────────────────────┘
```

---

## 1. Inputs

### 1.1 `ARS_LISTING_WORKING` (frozen, already built)

One row per **OPT** — the unique key is `(WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR)`. Relevant fields for the new engine:

| Field | Purpose |
|---|---|
| `WERKS`, `RDC`, `MAJ_CAT`, `GEN_ART_NUMBER`, `CLR` | Identity |
| `GEN_ART_DESC` | Display only |
| `OPT_TYPE` | `RL` / `TBC` / `TBL` / `MIX` / `NL` — primary routing flag |
| `IS_NEW` | `1` if new listing (TBL path) |
| `I_ROD` | Replenishment rounds for this OPT (target number of passes) |
| `OPT_MBQ` | Display MBQ (no hold) |
| `OPT_MBQ_WH` | With-hold MBQ (pool reserve target for TBL) |
| `OPT_REQ`, `OPT_REQ_WH` | Residual demand (MBQ minus current stock) |
| `STK_TTL` | Current store stock for this OPT (OPT-level sum over sizes) |
| `MSA_FNL_Q` | Warehouse supply expected for this OPT (OPT-level) |
| `VAR_COUNT` | # distinct variants in the OPT |
| `VAR_FNL_COUNT` | # variants with FNL_Q > 0 in MSA |
| `ACS_D`, `ALC_D`, `AGE`, `MAX_DAILY_SALE`, `PER_OPT_SALE` | Velocity inputs |
| `LISTING` | `1` = listed upstream, `0` = de-listed |
| `CLR_MIN`, `CLR_MAX` | Colour caps per (store, MAJ) |
| `FOCUS_W_CAP`, `FOCUS_WO_CAP` | Focus-store flags |
| `ST_RANK` | Store rank within MAJ (lower = priority) |
| `MJ_REQ`, `<hier>_REQ` | Residual demand at each primary-grid grain |
| `GH_*`, `H_*`, `PRI_CT%`, `SEC_CT%` | Hierarchy coverage flags |
| `ALLOC_FLAG` | 1 if `PRI_CT% >= 100` (computed during listing build) |
| `HOLD_QTY` | Pre-seeded 0; new engine will overwrite |

### 1.2 `ARS_MSA_VAR_ART` (variant-level pool, frozen)

One row per `(RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, ARTICLE_NUMBER, SZ)`:

| Field | Purpose |
|---|---|
| `RDC`, `MAJ_CAT`, `GEN_ART_NUMBER`, `CLR` | Join keys to listing working |
| `ARTICLE_NUMBER` | variant article (`VAR_ART`) |
| `ARTICLE_DESC` | `VAR_DESC` |
| `SZ` | Size code |
| `MRP`, `PAK_SZ` | Display / rounding inputs |
| `FNL_Q` | **Pool** — warehouse-available qty at this size |
| `STK_QTY`, `PEND_QTY` | Informational |
| `FAB`, `SSN` | Secondary filters |

### 1.3 `Master_CONT_SZ`

Row per `(ST_CD, MAJ_CAT, SZ, CONT)`. Used to split OPT-level MBQ into size-level MBQ. Fallback is uniform `1 / N` where `N = distinct sizes in OPT`.

### 1.4 `Master_ALC_INPUT_ST_MASTER`

Referenced for store/RDC filters already applied upstream. New engine does not re-filter.

---

## 2. Outputs

### 2.1 `ARS_LISTED_OPT` (new)

One row per OPT that survived the listing rules. Columns:

```
WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, GEN_ART_DESC,
OPT_TYPE, IS_NEW, I_ROD,
OPT_MBQ, OPT_REQ, OPT_MBQ_WH, OPT_REQ_WH,
MSA_FNL_Q, VAR_COUNT, VAR_FNL_COUNT,
STK_TTL, ACS_D, AGE, MAX_DAILY_SALE,
LISTING, PRI_CT%, SEC_CT%, ALLOC_FLAG,
FOCUS_W_CAP, FOCUS_WO_CAP,
ST_RANK,
LISTED_FLAG,       -- 1 = listed for allocation, 0 = dropped
LISTED_REASON,     -- free text; comma-separated rule codes
OPT_PRIORITY_RANK, -- integer; lower = allocate first
OPT_PRIORITY_TIER  -- 1=focus-uncapped, 2=focus-capped, 3=regular
```

### 2.2 `ARS_ALLOC_WORKING` (rebuilt — same name, new shape)

One row per `(WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)`. Columns:

```
-- identity
WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, GEN_ART_DESC,
VAR_ART, VAR_DESC, SZ, MRP, PAK_SZ,

-- context from listed OPT
OPT_TYPE, IS_NEW, I_ROD, OPT_PRIORITY_RANK, ST_RANK,
OPT_MBQ, OPT_MBQ_WH, OPT_REQ, OPT_REQ_WH,
MAX_DAILY_SALE, ALLOC_FLAG, PRI_CT%, SEC_CT%,

-- size-level targets
CONT,                        -- size contribution fraction
SZ_MBQ,                      -- OPT_MBQ * CONT (ship target, rounded)
SZ_MBQ_WH,                   -- OPT_MBQ_WH * CONT (pool reserve target, rounded)
SZ_STK,                      -- current store stock at this size (optional enrichment)
SZ_REQ,                      -- max(SZ_MBQ - SZ_STK, 0) — display demand
SZ_REQ_WH,                   -- max(SZ_MBQ_WH - SZ_STK, 0) — pool demand

-- pool
FNL_Q,                       -- pool copy at join time (for audit)
FNL_Q_REM,                   -- live remaining pool (mutated during waterfall)
POOL_CONSUMED,               -- cumulative pool taken by this row

-- outcome
SHIP_QTY,                    -- ships to store
HOLD_QTY,                    -- held at warehouse (TBL only)
ALLOC_QTY,                   -- = SHIP_QTY (for backward-compat with preview)
ROUND_SHIP, ROUND_HOLD,      -- per-round deltas (last round)
ALLOC_WAVE,                  -- e.g. 'RL_R1', 'TBC_R2', 'TBL_R1'
ALLOC_ROUND,                 -- integer round counter
ALLOC_STATUS,                -- PENDING | ALLOCATED | PARTIAL | SKIPPED
SKIP_REASON                  -- short code string
```

### 2.3 Columns added to `ARS_LISTING_WORKING`

(Same idempotent `ALTER TABLE ... ADD` pattern as today — never destructive.)

```
LISTED_FLAG        INT            (mirror of 2.1)
LISTED_REASON      NVARCHAR(500)
OPT_PRIORITY_RANK  INT
OPT_PRIORITY_TIER  INT
ALLOC_QTY          FLOAT          (OPT-level sum from ARS_ALLOC_WORKING)
HOLD_QTY           FLOAT          (OPT-level sum; TBL only)
ALLOC_STATUS       NVARCHAR(50)
ALLOC_REMARKS      NVARCHAR(MAX)
```

---

## 3. Stage A — List the OPTs

**Goal.** Decide, per OPT row in `ARS_LISTING_WORKING`, whether it enters the allocator. Rules are applied in order; the **first failing rule wins** the reason code.

### 3.1 Listing rules (filter gate)

Each rule is a single predicate. The engine concatenates every failing rule into `LISTED_REASON` (not just the first one — the user said "use some rules from there", so all failures are visible and individually toggleable in code).

| Code | Rule | Default | Notes |
|---|---|---|---|
| `R01_LISTING` | `LISTING = 1` | on | Drop de-listed OPTs |
| `R02_NOT_MIX` | `OPT_TYPE <> 'MIX'` | on | MIX has no MSA backing |
| `R03_NOT_NL`  | `OPT_TYPE <> 'NL'` | on | NL = marked for delisting |
| `R04_MSA_POS` | `MSA_FNL_Q > 0` | on | Warehouse has supply |
| `R05_REQ_POS` | `OPT_REQ_WH >= 1` | on | Store needs at least 1 unit |
| `R06_PRI_100` | `PRI_CT% >= 100` OR `ALLOC_FLAG = 1` | on | Primary grids complete |
| `R07_VAR_RATIO_TBL` | For `OPT_TYPE='TBL'`: `VAR_FNL_COUNT / VAR_COUNT >= size_threshold` OR `VAR_FNL_COUNT >= min_size_count` | on | Size coverage for NEW listings only |
| `R08_CLR_CAP` | `CLR_MAX` not yet exceeded for `(WERKS, MAJ_CAT)` after this OPT would be listed | optional | Enforces per-store colour cap; off by default — cap is already respected in listing build |
| `R09_TBL_TRIVIAL_GUARD` | For `TBL`: `MJ_REQ >= TBL_TRIVIAL_NEED_FACTOR × MAX_DAILY_SALE` | on | Prevents filling a store where primary-grid need is <½ day's sale |

Each rule is a single `CASE WHEN ... THEN 'R0X_...'` expression. Listed flag is `1` iff **all enabled rules pass**.

### 3.2 Priority tiering

After a row is `LISTED_FLAG = 1`, it gets a tier + a **global** rank.

```
TIER 1  FOCUS_WO_CAP = 1      (uncapped focus stores)
TIER 2  FOCUS_W_CAP  = 1      (capped focus stores)
TIER 3  everything else
```

TIER is **not** the outer partition. Rank is assigned in one global
`ROW_NUMBER()` so OPT_TYPE is the outermost sort — TIER 1 and TIER 2 sit on
top *inside each opt_type* (user spec, 2026-04-23):

```
ORDER BY
  CASE OPT_TYPE  WHEN 'RL' THEN 1 WHEN 'TBC' THEN 2 WHEN 'TBL' THEN 3 ELSE 4 END,
  OPT_PRIORITY_TIER         ASC,  -- focus rows on top within the opt_type
  ST_RANK                   ASC,  -- store select by ST_RANK within MAJ_CAT
  SEC_CT%                   DESC,
  MAX_DAILY_SALE            DESC,
  OPT_REQ_WH                DESC
```

Effect of this order:
1. All RL rows come first, then TBC, then TBL.
2. Within each opt_type: TIER 1, then TIER 2, then TIER 3.
3. Within each (opt_type, tier): rows walk in **ST_RANK order** — the
   best-ranked store in that MAJ_CAT gets its turn at pool first.
4. SEC_CT% / MAX_DAILY_SALE / OPT_REQ_WH are tie-breakers only.

### 3.3 SQL shape (Stage A)

```sql
-- 1) Prepare columns on listing working
ALTER TABLE [ARS_LISTING_WORKING] ADD [LISTED_FLAG] INT NULL;
ALTER TABLE [ARS_LISTING_WORKING] ADD [LISTED_REASON] NVARCHAR(500) NULL;
ALTER TABLE [ARS_LISTING_WORKING] ADD [OPT_PRIORITY_RANK] INT NULL;
ALTER TABLE [ARS_LISTING_WORKING] ADD [OPT_PRIORITY_TIER] INT NULL;

-- 2) Apply listing rules
UPDATE W SET
    [LISTED_REASON] =
        (CASE WHEN ISNULL(TRY_CAST(LISTING AS INT),1) <> 1       THEN 'R01_LISTING;' ELSE '' END)
      + (CASE WHEN OPT_TYPE = 'MIX'                              THEN 'R02_NOT_MIX;' ELSE '' END)
      + (CASE WHEN OPT_TYPE = 'NL'                               THEN 'R03_NOT_NL;'  ELSE '' END)
      + (CASE WHEN ISNULL(MSA_FNL_Q,0) <= 0                      THEN 'R04_MSA_POS;' ELSE '' END)
      + (CASE WHEN ISNULL(OPT_REQ_WH,0) < 1                      THEN 'R05_REQ_POS;' ELSE '' END)
      + (CASE WHEN ISNULL(TRY_CAST([PRI_CT%] AS FLOAT),0) < 100
                AND ISNULL(ALLOC_FLAG,0) <> 1                    THEN 'R06_PRI_100;' ELSE '' END)
      + (CASE WHEN OPT_TYPE = 'TBL'
                AND VAR_COUNT > 0
                AND (CAST(ISNULL(VAR_FNL_COUNT,0) AS FLOAT) / NULLIF(VAR_COUNT,0)) < :size_threshold
                AND ISNULL(VAR_FNL_COUNT,0) < :min_size_count   THEN 'R07_VAR_RATIO_TBL;' ELSE '' END)
      + (CASE WHEN OPT_TYPE = 'TBL'
                AND ISNULL(MJ_REQ,0) < :tbl_trivial_factor * ISNULL(MAX_DAILY_SALE,0)
                                                                 THEN 'R09_TBL_TRIVIAL;' ELSE '' END),
    [LISTED_FLAG] = CASE WHEN LEN(<same CASE chain>) = 0 THEN 1 ELSE 0 END
FROM [ARS_LISTING_WORKING] W;

-- 3) Assign tier
UPDATE [ARS_LISTING_WORKING]
SET [OPT_PRIORITY_TIER] =
    CASE WHEN FOCUS_WO_CAP = 1 THEN 1
         WHEN FOCUS_W_CAP  = 1 THEN 2
         ELSE 3 END
WHERE LISTED_FLAG = 1;

-- 4) Assign rank inside tier
;WITH R AS (
    SELECT WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR,
           ROW_NUMBER() OVER (
               PARTITION BY OPT_PRIORITY_TIER
               ORDER BY CASE OPT_TYPE WHEN 'RL' THEN 1 WHEN 'TBC' THEN 2 WHEN 'TBL' THEN 3 ELSE 4 END,
                        ISNULL(TRY_CAST([SEC_CT%] AS FLOAT), 0) DESC,
                        ISNULL(MAX_DAILY_SALE, 0) DESC,
                        ISNULL(OPT_REQ_WH, 0) DESC,
                        ISNULL(ST_RANK, 999999) ASC
           ) AS rk
    FROM [ARS_LISTING_WORKING]
    WHERE LISTED_FLAG = 1
)
UPDATE W SET W.OPT_PRIORITY_RANK = R.rk
FROM [ARS_LISTING_WORKING] W
JOIN R ON W.WERKS=R.WERKS AND W.MAJ_CAT=R.MAJ_CAT
      AND W.GEN_ART_NUMBER=R.GEN_ART_NUMBER AND W.CLR=R.CLR;

-- 5) Materialize ARS_LISTED_OPT
IF OBJECT_ID('ARS_LISTED_OPT','U') IS NOT NULL DROP TABLE [ARS_LISTED_OPT];
SELECT <column list from 2.1> INTO [ARS_LISTED_OPT]
FROM [ARS_LISTING_WORKING]
WHERE LISTED_FLAG = 1;
```

### 3.4 Why this structure

- **No partial/retry inside Stage A.** The listing decision is a simple gate; pool dynamics live in Stage C only. Trying to combine the two (as the old engine does with rank-banded revalidation) makes both harder to audit.
- **Reason chain, not short-circuit.** The user asked to "use some rules from there" — having all failing reasons in one string makes it trivial to count how many OPTs fell to each rule and decide which rules to relax.
- **ST_RANK is a tie-breaker, not a gate.** Every LISTED OPT gets a chance at pool; store priority shapes the order only.

---

## 4. Stage B — Explode to VAR_ART × SZ

### 4.1 Join

```sql
IF OBJECT_ID('ARS_ALLOC_WORKING','U') IS NOT NULL DROP TABLE [ARS_ALLOC_WORKING];

SELECT
    L.WERKS, L.RDC, L.MAJ_CAT, L.GEN_ART_NUMBER, L.CLR, L.GEN_ART_DESC,
    V.ARTICLE_NUMBER AS VAR_ART,
    V.ARTICLE_DESC   AS VAR_DESC,
    V.SZ, V.MRP, V.PAK_SZ,

    L.OPT_TYPE, L.IS_NEW, L.I_ROD,
    L.OPT_PRIORITY_RANK, L.OPT_PRIORITY_TIER, L.ST_RANK,
    L.OPT_MBQ, L.OPT_MBQ_WH, L.OPT_REQ, L.OPT_REQ_WH,
    L.MAX_DAILY_SALE, L.ALLOC_FLAG,
    L.[PRI_CT%], L.[SEC_CT%],

    TRY_CAST(V.FNL_Q AS FLOAT)  AS FNL_Q,
    TRY_CAST(V.FNL_Q AS FLOAT)  AS FNL_Q_REM,     -- live counter
    CAST(NULL AS FLOAT)          AS CONT,          -- filled by 4.2
    CAST(NULL AS FLOAT)          AS SZ_MBQ,
    CAST(NULL AS FLOAT)          AS SZ_MBQ_WH,
    CAST(NULL AS FLOAT)          AS SZ_STK,
    CAST(NULL AS FLOAT)          AS SZ_REQ,
    CAST(NULL AS FLOAT)          AS SZ_REQ_WH,

    CAST(0 AS FLOAT) AS POOL_CONSUMED,
    CAST(0 AS FLOAT) AS SHIP_QTY,
    CAST(0 AS FLOAT) AS HOLD_QTY,
    CAST(0 AS FLOAT) AS ALLOC_QTY,
    CAST(0 AS FLOAT) AS ROUND_SHIP,
    CAST(0 AS FLOAT) AS ROUND_HOLD,
    CAST(NULL AS NVARCHAR(20)) AS ALLOC_WAVE,
    CAST(0 AS INT) AS ALLOC_ROUND,
    CAST('PENDING' AS NVARCHAR(50)) AS ALLOC_STATUS,
    CAST(NULL AS NVARCHAR(500))    AS SKIP_REASON
INTO [ARS_ALLOC_WORKING]
FROM [ARS_LISTED_OPT] L
INNER JOIN [ARS_MSA_VAR_ART] V WITH (NOLOCK)
    ON  L.MAJ_CAT        = V.MAJ_CAT
    AND L.GEN_ART_NUMBER = TRY_CAST(V.GEN_ART_NUMBER AS BIGINT)
    AND L.CLR            = V.CLR
    AND L.RDC            = V.RDC
WHERE TRY_CAST(V.FNL_Q AS FLOAT) > 0;
```

### 4.2 Fill `CONT` (size contribution fraction)

Three-step fallback (same as existing engine — kept because this is robust):

1. `Master_CONT_SZ` join on `(ST_CD = WERKS, MAJ_CAT, SZ)`.
2. Where still null, `Master_CONT_SZ` on `(ST_CD = 'CO', MAJ_CAT, SZ)` — the global CO default.
3. Where still null, uniform `1 / N` with `N = distinct sizes per (WERKS, MAJ_CAT)`.

### 4.3 Fill size-level targets

```sql
UPDATE A SET
    [SZ_MBQ]    = ROUND(ISNULL(OPT_MBQ,    0) * ISNULL(CONT, 0), 0),
    [SZ_MBQ_WH] = ROUND(ISNULL(OPT_MBQ_WH, 0) * ISNULL(CONT, 0), 0)
FROM [ARS_ALLOC_WORKING] A;

-- Optional: enrich SZ_STK from a variant grid if available (ARS_GRID_MJ_VAR_ART.STK_TTL)
-- Else leave 0. SZ_REQ/SZ_REQ_WH will default to full SZ_MBQ/SZ_MBQ_WH.
UPDATE A SET
    [SZ_STK]    = ISNULL(SZ_STK, 0),
    [SZ_REQ]    = CASE WHEN [SZ_MBQ]    - ISNULL([SZ_STK],0) > 0 THEN [SZ_MBQ]    - ISNULL([SZ_STK],0) ELSE 0 END,
    [SZ_REQ_WH] = CASE WHEN [SZ_MBQ_WH] - ISNULL([SZ_STK],0) > 0 THEN [SZ_MBQ_WH] - ISNULL([SZ_STK],0) ELSE 0 END
FROM [ARS_ALLOC_WORKING] A;
```

### 4.4 Indexes

```sql
CREATE CLUSTERED INDEX CIX_alloc_walk ON [ARS_ALLOC_WORKING]
  (OPT_TYPE, OPT_PRIORITY_RANK, WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ);

CREATE NONCLUSTERED INDEX IX_alloc_pool ON [ARS_ALLOC_WORKING]
  (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
  INCLUDE (WERKS, SHIP_QTY, HOLD_QTY, FNL_Q_REM);
```

---

## 5. Stage C — Allocate VAR_ART × SZ

### 5.1 Pool semantics

Pool is per `(RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)`. Every store competing for the same variant-size draws from one bucket.

We maintain a temp table `#pool` with `FNL_Q_ORIG` and `FNL_Q_REM`, plus a unique index on the key. `FNL_Q_REM` is decremented atomically as rows are committed.

### 5.2 Walk order

Outer loop — **OPT_TYPE**:
```
for opt_type in ['RL', 'TBC', 'TBL']:   # Rule 1 — fixed order
```

Middle loop — **rounds** (Rule 1 continued):
```
for r in range(1, MAX_I_ROD + 1):       # MAX over the current opt_type's listed OPTs
```

Inner loop — **priority band** (Rule 2 — rank-banded for revalidation):
```
for band_start in range(1, max_rank + 1, BAND_SIZE):
    band_end = band_start + BAND_SIZE - 1
    allocate_band(opt_type, r, band_start, band_end)
    revalidate_grid_remaining(opt_type, band_start, band_end)   # optional
```

`BAND_SIZE = 1` gives strict option-by-option revalidation. Set higher for speed at the cost of tighter grid-overflow protection. Default: `1`.

### 5.3 Round-N target

For round `r`, the size-level targets scale linearly:

```
round_pool_target = SZ_MBQ_WH * r
round_ship_target = SZ_MBQ    * r
```

Per-row outstanding demand at start of round `r`:

```
SZ_POOL_REQ  = max(round_pool_target - POOL_CONSUMED, 0)
SZ_SHIP_REQ  = max(round_ship_target - SHIP_QTY,      0)
```

### 5.4 Batch allocation SQL (one band × opt_type × round)

```sql
;WITH Target AS (
    SELECT
        A.WERKS, A.RDC, A.MAJ_CAT, A.GEN_ART_NUMBER, A.CLR, A.VAR_ART, A.SZ,
        A.OPT_PRIORITY_RANK, A.ST_RANK,
        GREATEST(0.0, :r * A.SZ_MBQ_WH - A.POOL_CONSUMED) AS need_pool,
        GREATEST(0.0, :r * A.SZ_MBQ    - A.SHIP_QTY)      AS need_ship,
        A.IS_NEW
    FROM [ARS_ALLOC_WORKING] A
    WHERE A.OPT_TYPE = :ot
      AND A.OPT_PRIORITY_RANK BETWEEN :bs AND :be
      AND A.ALLOC_STATUS IN ('PENDING', 'PARTIAL')
),
RankedByStore AS (
    SELECT T.*, P.FNL_Q_REM,
           ROW_NUMBER() OVER (
               PARTITION BY T.RDC, T.MAJ_CAT, T.GEN_ART_NUMBER, T.CLR, T.VAR_ART, T.SZ
               ORDER BY T.OPT_PRIORITY_RANK ASC, T.ST_RANK ASC
           ) AS store_order
    FROM Target T
    INNER JOIN #pool P
        ON  P.RDC = T.RDC AND P.MAJ_CAT = T.MAJ_CAT
        AND P.GEN_ART_NUMBER = T.GEN_ART_NUMBER AND P.CLR = T.CLR
        AND P.VAR_ART = T.VAR_ART AND P.SZ = T.SZ
    WHERE T.need_pool > 0 AND P.FNL_Q_REM > 0
),
Cumulative AS (
    SELECT *,
           SUM(need_pool) OVER (
               PARTITION BY RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ
               ORDER BY store_order ROWS UNBOUNDED PRECEDING
           ) AS cum_demand
    FROM RankedByStore
),
Allocated AS (
    SELECT *,
           -- this row gets min(need_pool, whatever pool remains after earlier rows in this band)
           LEAST(
               need_pool,
               GREATEST(0.0, FNL_Q_REM - (cum_demand - need_pool))
           ) AS take_pool
    FROM Cumulative
)
UPDATE A SET
    A.POOL_CONSUMED = A.POOL_CONSUMED + X.take_pool,
    -- TBL: ship = ship-target portion, hold = rest of pool-take.
    -- RL / TBC: whole take ships (OPT_MBQ_WH == OPT_MBQ for these).
    A.ROUND_SHIP = CASE WHEN A.IS_NEW = 1
                        THEN LEAST(X.take_pool, X.need_ship)
                        ELSE X.take_pool END,
    A.ROUND_HOLD = CASE WHEN A.IS_NEW = 1
                        THEN X.take_pool - LEAST(X.take_pool, X.need_ship)
                        ELSE 0 END,
    A.SHIP_QTY   = A.SHIP_QTY   + CASE WHEN A.IS_NEW = 1 THEN LEAST(X.take_pool, X.need_ship) ELSE X.take_pool END,
    A.HOLD_QTY   = A.HOLD_QTY   + CASE WHEN A.IS_NEW = 1 THEN X.take_pool - LEAST(X.take_pool, X.need_ship) ELSE 0 END,
    A.ALLOC_WAVE = CONCAT(:ot, '_R', :r),
    A.ALLOC_ROUND = :r,
    A.ALLOC_STATUS = CASE
        WHEN A.POOL_CONSUMED + X.take_pool >= :r * A.SZ_MBQ_WH THEN 'ALLOCATED'
        ELSE 'PARTIAL' END
FROM [ARS_ALLOC_WORKING] A
INNER JOIN Allocated X
    ON  A.WERKS = X.WERKS AND A.RDC = X.RDC
    AND A.MAJ_CAT = X.MAJ_CAT AND A.GEN_ART_NUMBER = X.GEN_ART_NUMBER
    AND A.CLR = X.CLR AND A.VAR_ART = X.VAR_ART AND A.SZ = X.SZ
WHERE X.take_pool > 0;

-- decrement pool
UPDATE P SET P.FNL_Q_REM = P.FNL_Q_REM - S.taken
FROM #pool P
INNER JOIN (
    SELECT RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ, SUM(take_pool) AS taken
    FROM Allocated
    GROUP BY RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ
) S
ON P.RDC=S.RDC AND P.MAJ_CAT=S.MAJ_CAT
AND P.GEN_ART_NUMBER=S.GEN_ART_NUMBER AND P.CLR=S.CLR
AND P.VAR_ART=S.VAR_ART AND P.SZ=S.SZ;
```

### 5.5 RL / TBC vs TBL split (Rule 3)

- **RL & TBC** — `OPT_MBQ_WH == OPT_MBQ`, so `SZ_MBQ_WH == SZ_MBQ`. All pool taken ships. No hold.
- **TBL** — `OPT_MBQ_WH > OPT_MBQ` by `hold_days × daily_rate`. Pool reserves the bigger number, store only gets the display number, and the delta sits as warehouse buffer.

At end of Stage C:

```
ALLOC_QTY = SHIP_QTY
SKIP_REASON is populated only for rows that stayed PENDING after all rounds
           (reason: 'POOL_EXHAUSTED' or 'STORE_BROKEN')
```

### 5.6 Optional break rules (disabled by default; flip in code to try)

| Code | Rule | Effect |
|---|---|---|
| `SIZE_COVERAGE_BREAK` | If after round 1 fewer than `size_threshold × size_count` sizes received any allocation, mark the whole OPT `SKIPPED` and refund what was already taken to `#pool`. | Off — user said ARS_LISTING_WORKING is already clean, we don't want to re-route at allocation time. |
| `STORE_BROKEN` | If `MJ_REQ_REM < 0.5 × ACS_D` for this store+MAJ, stop allocating further OPTs of this OPT_TYPE to the store. | **On** — see §5.7. |
| `GRID_OVERFLOW` | After each band, recompute `<grid>_REQ_REM` from consumed pool. If a later OPT would push any grid into negative remaining, skip it. | Off — superseded by per-OPT revalidation (§5.7). |

Rules live as feature-flag constants at the top of `rule_engine_new.py` so the user can toggle without touching the allocation SQL.

### 5.7 Per-OPT revalidation (ENABLE_PER_OPT_REVALIDATION)

**Requires `BAND_SIZE = 1`** so each band allocates exactly one OPT. After each band the engine writes the take back onto `ARS_LISTING_WORKING`, recomputes primary-grid coverage, and decides whether the next-ranked OPT still qualifies.

**Shadow columns on `ARS_LISTING_WORKING`** (seeded once at Stage C start):

| Column | Type | Seeded from | Mutated by |
|---|---|---|---|
| `MSA_FNL_Q_REM` | FLOAT | `MSA_FNL_Q` | SHIP + HOLD per OPT |
| `MJ_REQ_REM` | FLOAT | `MJ_REQ` | SHIP at (WERKS, MAJ_CAT) |
| `<hier>_REQ_REM` | FLOAT | `<hier>_REQ` | SHIP at that grid's grain |
| `H_MJ_REM`, `H_<hier>_REM` | INT | `H_MJ`, `H_<hier>` | recomputed each band |
| `PRI_CT_REM` | FLOAT | `PRI_CT%` | recomputed each band |

Originals (`MSA_FNL_Q`, `MJ_REQ`, `PRI_CT%`, `H_MJ`, …) are **never** mutated — they stay as the pre-allocation snapshot.

**After each band — sequence (happens in `_revalidate_after_band`):**

1. `MSA_FNL_Q_REM -= ROUND_SHIP + ROUND_HOLD` per OPT (what this OPT just took).
2. For **every** primary grid discovered in `ARS_GRID_BUILDER`:
   - Aggregate `ROUND_SHIP` at the grid's grain `(WERKS, MAJ_CAT, *extras)` — joined through `ARS_LISTING_WORKING` to pick up the extras (`RNG_SEG`, `MACRO_MVGR`, etc.).
   - `<grid>_REQ_REM = max(<grid>_REQ_REM − grid_take, 0)`.
3. Recompute `H_<grid>_REM`:
   ```
   H_REM = 1 iff (REQ_REM > ACS_SKIP_FACTOR × ACS_D) AND (GH = 1)
   ```
   The `> 0.5 × ACS_D` gate is the key difference from the original `H_col`
   definition. A grid with < half a day's sale worth of residual demand no
   longer counts as "covered" — so it drags `PRI_CT_REM` down and blocks
   the next OPT.
4. Recompute `PRI_CT_REM = round(Σ H_REM / Σ GH × 100, 1)` per row.
5. **Skip rules** — applied to OPTs where `OPT_PRIORITY_RANK > current band_end`
   (i.e., not yet processed), status still `PENDING`/`PARTIAL`:

| Rule | Trigger | Reason code |
|---|---|---|
| MSA exhausted | `MSA_FNL_Q_REM <= 0` | `SKIP_MSA_EXHAUSTED` |
| Primary broken | `PRI_CT_REM < 100` | `SKIP_PRI_BROKEN` |
| Store broken (opt_type-scoped) | `MJ_REQ_REM < 0.5 × ACS_D` for this (WERKS, MAJ_CAT), plus `OPT_TYPE` = current opt_type | `SKIP_STORE_BROKEN` |

6. SKIP status is mirrored onto `ARS_ALLOC_WORKING` for matching rows so later bands' `Target` CTE filters them out (`WHERE ALLOC_STATUS NOT IN ('SKIPPED','INELIGIBLE')`).

**Tuning knobs** (top of `rule_engine_new.py`):

```python
ENABLE_PER_OPT_REVALIDATION = True   # set False to allocate without revalidation
ENABLE_STORE_BROKEN         = True   # set False to let stores keep allocating past MJ_REQ
ACS_SKIP_FACTOR             = 0.5    # threshold used in H_REM gate + store-broken rule
BAND_SIZE                   = 1      # keep at 1 for strict per-OPT revalidation
```

**Why revalidation runs between bands, not before:** the very first OPT in an opt_type always gets its shot because `PRI_CT_REM` is seeded from the already-valid `PRI_CT% = 100`. The threshold (`REQ > 0.5×ACS_D`) only starts biting after allocation has reduced some grid's `REQ_REM` below that line.

---

## 6. Stage D — Reflect & audit

```sql
-- OPT-level aggregation back to listing working
;WITH Agg AS (
    SELECT WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR,
           SUM(SHIP_QTY) AS ship_q,
           SUM(HOLD_QTY) AS hold_q,
           COUNT(*)      AS sz_rows,
           SUM(CASE WHEN SHIP_QTY > 0 THEN 1 ELSE 0 END) AS filled_rows
    FROM [ARS_ALLOC_WORKING]
    GROUP BY WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR
)
UPDATE W SET
    W.ALLOC_QTY      = A.ship_q,
    W.HOLD_QTY       = A.hold_q,
    W.ALLOC_STATUS   = CASE
        WHEN A.ship_q = 0                       THEN 'NOT_ALLOCATED'
        WHEN A.filled_rows < A.sz_rows          THEN 'PARTIAL'
        ELSE 'ALLOCATED' END,
    W.ALLOC_REMARKS  = CONCAT(
        'ship=', A.ship_q, '; hold=', A.hold_q,
        '; sizes=', A.filled_rows, '/', A.sz_rows)
FROM [ARS_LISTING_WORKING] W
INNER JOIN Agg A
    ON W.WERKS=A.WERKS AND W.MAJ_CAT=A.MAJ_CAT
   AND W.GEN_ART_NUMBER=A.GEN_ART_NUMBER AND W.CLR=A.CLR;

-- OPTs that were LISTED but got zero pool
UPDATE W SET
    W.ALLOC_STATUS = 'NOT_ALLOCATED',
    W.ALLOC_REMARKS = 'no pool'
WHERE LISTED_FLAG = 1
  AND W.ALLOC_QTY IS NULL;

-- OPTs dropped at Stage A keep their reason
UPDATE W SET W.ALLOC_STATUS = 'INELIGIBLE'
WHERE LISTED_FLAG = 0;
```

---

## 7. Configurable parameters

Exposed on the `run_listing_and_allocation()` signature — all defaults baked into the function.

| Param | Default | Maps to |
|---|---|---|
| `size_threshold` | `0.6` | R07 / R06 variants |
| `min_size_count` | `3` | R07 |
| `tbl_trivial_factor` | `0.5` | R09 |
| `band_size` | `1` | Stage C 5.2 |
| `enable_store_broken` | `False` | 5.6 |
| `enable_grid_overflow` | `False` | 5.6 |
| `enable_size_coverage_break` | `False` | 5.6 |
| `enable_focus_tiering` | `True` | 3.2 |

---

## 8. What we are *not* doing (on purpose)

1. **No fallback boost** — the old `fallback_boost_mode` (STR / sales_only / full_mbq) is dropped. If a LISTED OPT doesn't get pool in its rounds, it stays `NOT_ALLOCATED`; the user prefers clean signal over synthetic demand. Can be added later as a Stage C.5.
2. **No rank-embedded revalidation against `<grid>_REQ`** — the old engine mutated `<grid>_REQ_REM` on `ARS_LISTING_WORKING` during allocation. The new engine treats the listing working table as immutable except for the status columns in §2.3. Grid-overflow protection, if wanted, moves into a shadow table `#grid_rem` (Stage C extension).
3. **No re-running listing upstream** — Parts 1–7 of `/listing/generate` stay as-is and the listing working table is the frozen contract between upstream and this engine.

---

## 9. Integration — `listing.py` changes

```python
# Part 8 — replace
# OLD:
# from app.services.rule_engine import run_rule_based_allocation
# alloc_result = run_rule_based_allocation(conn=ac, final_table=FINAL_TABLE,
#                                          alloc_table=ALLOC_TABLE,
#                                          size_threshold=req.stock_threshold_pct)

# NEW:
from app.services.rule_engine_new import run_listing_and_allocation
alloc_result = run_listing_and_allocation(
    conn=ac,
    working_table=FINAL_TABLE,       # ARS_LISTING_WORKING
    listed_table="ARS_LISTED_OPT",
    alloc_table=ALLOC_TABLE,         # ARS_ALLOC_WORKING
    size_threshold=req.stock_threshold_pct,
    min_size_count=req.min_size_count,
)
alloc_rows = alloc_result.get("alloc_rows", 0)
```

The old import is left in the file, wrapped in `if False:` (never executes) so the call-site is visible for reference.

---

## 10. Acceptance test — after first run

After one run of `/listing/generate`, the user should be able to validate:

```sql
-- 1. Listing working row counts by status
SELECT ALLOC_STATUS, COUNT(*)
FROM ARS_LISTING_WORKING GROUP BY ALLOC_STATUS;

-- 2. Which listing rules rejected how many OPTs?
SELECT value AS rule_code, COUNT(*)
FROM ARS_LISTING_WORKING
CROSS APPLY STRING_SPLIT(LISTED_REASON, ';')
WHERE LISTED_FLAG = 0 AND value <> ''
GROUP BY value ORDER BY COUNT(*) DESC;

-- 3. Pool utilisation per variant-size
SELECT
    SUM(FNL_Q - FNL_Q_REM) AS shipped_from_pool,
    SUM(FNL_Q) AS total_pool
FROM ARS_ALLOC_WORKING;

-- 4. Opt-level allocation sanity
SELECT OPT_TYPE,
       COUNT(*) AS opts,
       SUM(ALLOC_QTY) AS units,
       SUM(HOLD_QTY) AS held
FROM ARS_LISTING_WORKING
WHERE LISTED_FLAG = 1
GROUP BY OPT_TYPE;
```

Pool utilisation should be `< 100%` only when store demand genuinely exhausts before pool does. If it's `0%` after rollout, Stage C is broken; if it's always `100%`, every OPT is winning pool which usually means Stage A is too loose.
