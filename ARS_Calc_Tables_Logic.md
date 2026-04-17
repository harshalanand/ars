# ARS_CALC_ST_MAJ_CAT & ARS_CALC_ST_ART — Calculation Logic

**Reference document for Pre-Grid Calculation working tables.**
These two tables hold merged master data + computed SAL_D / SAL_PD and feed the Grid Builder's MBQ calculation and the Listing pipeline's DPN/SAL_D/SAL_PD columns.

---

# 1. ARS_CALC_ST_MAJ_CAT — Per Store × MAJ_CAT

## Purpose
Merged + enriched master data at the store × category grain. Feeds:
- Grid Builder MBQ/OPT_CNT calculations
- Listing's DPN, SAL_D, PER_OPT_SALE
- Article-level table's CM_REM_D / NM_REM_D borrowing

## Grain
One row per (`ST_CD`, `MAJ_CAT`).

## Source Tables

| Table | Purpose | Key |
|-------|---------|-----|
| Master_ALC_INPUT_ST_MAJ_CAT | Store-level base settings | ST_CD + MAJ_CAT |
| Master_ALC_INPUT_CO_MAJ_CAT | Company-level overrides | MAJ_CAT |
| Master_ALC_INPUT_ST_MASTER | Store master (INT_DAYS, PRD_DAYS, SL_CVR) | ST_CD |

## Step 1 — Create Working Table

```sql
IF OBJECT_ID('ARS_CALC_ST_MAJ_CAT','U') IS NOT NULL
    DROP TABLE ARS_CALC_ST_MAJ_CAT;

SELECT * INTO ARS_CALC_ST_MAJ_CAT
FROM Master_ALC_INPUT_ST_MAJ_CAT WITH (NOLOCK);
```

Also ensures output columns exist: `SAL_D`, `SAL_PD`, `SALE_COVER_SRC`.

## Step 2 — Merge CO_MAJ_CAT Values

`Master_ALC_INPUT_CO_MAJ_CAT` has no `ST_CD` — values apply to all stores for that MAJ_CAT. Per-column rules:

| Column | Rule | Behavior |
|--------|------|----------|
| LISTING | co_override | CO wins — overwrites all stores |
| I_ROD | co_override | CO wins |
| MANUAL_MBQ | max | MAX(ST, CO) |
| DISP_GR_DGR | max | MAX(ST, CO) |
| LW_ACT_SL_GR_DGR | max | MAX(ST, CO) |
| BGT_SL_GR_DGR | max | MAX(ST, CO) |
| CLR_MIN | max | MAX(ST, CO) |
| CLR_MAX | max | MAX(ST, CO) |
| DPN | max | MAX(ST, CO) |
| CONT | st_first | ST wins; CO only if ST is NULL/0 |

### Rule Implementations

**co_override**
```sql
UPDATE C SET C.[{col}] = CO.[{col}]
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_CO_MAJ_CAT CO
    ON C.[MAJ_CAT] = CO.[MAJ_CAT]
WHERE CO.[{col}] IS NOT NULL;
```

**max**
```sql
UPDATE C SET C.[{col}] =
    CASE WHEN ISNULL(C.[{col}],0) >= ISNULL(CO.[{col}],0) THEN C.[{col}]
         ELSE CO.[{col}] END
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_CO_MAJ_CAT CO
    ON C.[MAJ_CAT] = CO.[MAJ_CAT]
WHERE CO.[{col}] IS NOT NULL AND CO.[{col}] > 0;
```

**st_first**
```sql
UPDATE C SET C.[{col}] = CO.[{col}]
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_CO_MAJ_CAT CO
    ON C.[MAJ_CAT] = CO.[MAJ_CAT]
WHERE (C.[{col}] IS NULL OR TRY_CAST(C.[{col}] AS FLOAT) = 0)
  AND CO.[{col}] IS NOT NULL AND TRY_CAST(CO.[{col}] AS FLOAT) > 0;
```

## Step 3 — Apply Defaults

| Column | Default Rule |
|--------|--------------|
| LISTING | blank / NULL / 'Y' → 1; 'N' → 0; numeric string → keep |
| I_ROD | NULL / 0 → 1 |
| DISP_GR_DGR | NULL / 0 → 1 |
| LW_ACT_SL_GR_DGR | NULL / 0 → 1 |
| BGT_SL_GR_DGR | NULL / 0 → 1 |
| MANUAL_MBQ | ≤ 0 → NULL (so downstream treats it as "no manual override") |

## Step 4 — Calculate SAL_D (Sale-Cover Window)

```
SAL_D = INT_DAYS + PRD_DAYS + SL_CVR
```

`INT_DAYS` and `PRD_DAYS` always come from `Master_ALC_INPUT_ST_MASTER` (joined by ST_CD). `SL_CVR` comes from one of three priority sources (applied in order — each overwrites the previous):

| Priority | SL_CVR Source | Applied When | SALE_COVER_SRC |
|----------|---------------|--------------|----------------|
| Base | ST_MASTER.SL_CVR | Always first | ST_MASTER |
| 2 | CO_MAJ_CAT.SL_CVR | CO value IS NOT NULL AND > 0 | CO_MAJ_CAT |
| 1 (final) | ST_MAJ_CAT.SL_CVR | ST value IS NOT NULL AND > 0 | ST_MAJ_CAT |

`SALE_COVER_SRC` records which source won for audit/debugging.

### Base Query Shape
```sql
UPDATE C SET C.[SALE_COVER_SRC] = 'ST_MASTER',
    C.[SAL_D] = ISNULL(S.[INT_DAYS],0)
              + ISNULL(S.[PRD_DAYS],0)
              + ISNULL(S.[SL_CVR],0)
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_ST_MASTER S ON C.[ST_CD] = S.[ST_CD];
```

The CO and ST priority steps use the same shape but override with higher-priority SL_CVR values.

## Step 5 — Calculate SAL_PD (Per-Day Sale)

```
IF CM_REM_D = 0:
    SAL_PD = 0

ELSE IF CM_REM_D >= SAL_D:
    SAL_PD = CM_SAL_Q / CM_REM_D                       ← Branch 1 (CM daily rate covers window)

ELSE IF SAL_D = 0:
    SAL_PD = 0

ELSE IF NM_REM_D = 0:
    SAL_PD = CM_SAL_Q / CM_REM_D                       ← fallback (no prev-month data)

ELSE:
    SAL_PD = ( CM_SAL_Q + (NM_SAL_Q / NM_REM_D) × (SAL_D − CM_REM_D) ) / SAL_D
                                                       ← Branch 2 (CM actuals + NM extrapolation)
```

### Input Columns (must exist in ARS_CALC_ST_MAJ_CAT)

| Column | Meaning |
|--------|---------|
| CM_SAL_Q | Current month sale quantity |
| CM_REM_D | Current month days with data (elapsed days this period) |
| NM_SAL_Q | Previous/next month sale quantity |
| NM_REM_D | Previous/next month days with data |
| SAL_D | Target window (from Step 4) |

### Worked Example — HB05 / M_TEES_HS

Inputs from the actual DB:
```
CM_SAL_Q = 5400,  CM_REM_D = 25
NM_SAL_Q = 10712, NM_REM_D = 31
SAL_D    = 9
```

Branch check: `CM_REM_D (25) >= SAL_D (9)` → **Branch 1** applies.

```
SAL_PD = CM_SAL_Q / CM_REM_D = 5400 / 25 = 216 units/day
```

**Interpretation:** The store sold at a daily rate of 216 units for M_TEES_HS during the current month, and since we have more CM days than our target window, the CM rate alone is reliable — no need to blend with prev-month data.

### Branch 2 Illustrative Example (If SAL_D Were 37)

```
CM_REM_D (25) < SAL_D (37) → Branch 2

NM daily rate = 10712 / 31 = 345.54
Missing days = SAL_D − CM_REM_D = 37 − 25 = 12

SAL_PD = (5400 + 345.54 × 12) / 37
       = (5400 + 4146.49) / 37
       = 9546.49 / 37
       ≈ 258.01 units/day
```

## Primary Key
`(ST_CD, MAJ_CAT)` — added by `ensure_primary_keys()` if not already present.

## Final Output Columns

| Column | Source |
|--------|--------|
| ST_CD, MAJ_CAT | grouping keys |
| LISTING, I_ROD | merged + default applied |
| MANUAL_MBQ | merged + ≤0 → NULL |
| DPN, DISP_Q, CLR_MIN, CLR_MAX | MAX(ST, CO) |
| DISP_GR_DGR, LW_ACT_SL_GR_DGR, BGT_SL_GR_DGR | MAX(ST, CO) + default 1 |
| CONT | ST first, CO fallback |
| CM_SAL_Q, CM_REM_D, NM_SAL_Q, NM_REM_D | copied from ST source (sales inputs) |
| SAL_D | Step 4 output |
| SAL_PD | Step 5 output |
| SALE_COVER_SRC | SL_CVR source audit (ST_MASTER / CO_MAJ_CAT / ST_MAJ_CAT) |

---

# 2. ARS_CALC_ST_ART — Per Store × Article

## Purpose
Article-level working table mirroring MAJ_CAT flow. Holds article-grain SAL_D and SAL_PD. Downstream uses:
- Feeds `MASTER_GEN_ART_SALE.SAL_PD` as a cross-check (separate bulk calc writes SAL_PD directly on the master)
- Historical reference for article-level settings (FOCUS, CORE, AUTO, HH_ART flags)

## Grain
One row per (`ST_CD`, `MAJ_CAT`, `GEN_ART_NUMBER`, `CLR`).

## Source Tables

| Table | Purpose | Key |
|-------|---------|-----|
| Master_ALC_INPUT_ST_ART | Store-level article settings | ST_CD + GEN_ART |
| MASTER_ALC_INPUT_CO_ART | Company-level article settings (no ST_CD) | 10_DIGIT (= GEN_ART_NUMBER) |
| MASTER_GEN_ART_SALE | Planned sales at option grain | ST_CD + MAJ_CAT + GEN_ART + CLR |
| Master_ALC_INPUT_ST_MASTER | Store master | ST_CD |
| ARS_CALC_ST_MAJ_CAT | Borrowed CM_REM_D / NM_REM_D | ST_CD + MAJ_CAT |

## Article Key Detection
`CO_ART` uses varied column names — auto-detected from these aliases (in order):
```
GEN_ART_NUMBER, 10_DIGIT, ART_NUMBER, ARTICLE_NUMBER
```

## Step A1 — Create Working Table

Primary path (ST_ART has data):
```sql
DROP TABLE ARS_CALC_ST_ART;
SELECT * INTO ARS_CALC_ST_ART
FROM Master_ALC_INPUT_ST_ART WITH (NOLOCK);
```

Fallback (ST_ART is empty — synthesize from CO_ART × stores):
```sql
SELECT ST.[ST_CD],
       CO.[MAJ_CAT],
       TRY_CAST(CO.[10_DIGIT] AS BIGINT) AS [GEN_ART_NUMBER],
       CO.[CLR],
       -- plus all other CO_ART columns
       ...
INTO ARS_CALC_ST_ART
FROM Master_ALC_INPUT_ST_MASTER ST
CROSS JOIN MASTER_ALC_INPUT_CO_ART CO
WHERE ST.[ST_CD] IS NOT NULL;
```

The fallback means every active store receives the company-level article definitions when the store-level ST_ART master is empty.

Ensures `SAL_D`, `SAL_PD`, `SALE_COVER_SRC` columns exist.

## Step A2 — Merge CO_ART Values

`MASTER_ALC_INPUT_CO_ART` has no `ST_CD` — values apply to all stores for that article.

| Column | Rule |
|--------|------|
| LISTING | co_override |
| I_ROD | co_override |
| MANUAL_MBQ | max |
| FOCUS_W_CAP | co_override |
| FOCUS_WO_CAP | co_override |
| CORE | co_override |
| AUTO | co_override |
| HH_ART | co_override |

Same rule implementations as Step 2 of MAJ_CAT calc (co_override / max).

## Step A3 — Calculate SAL_D

Same formula as MAJ_CAT:
```
SAL_D = INT_DAYS + PRD_DAYS + SL_CVR  (from ST_MASTER)
```

Article-level `SAL_D` typically inherits from store level — no article-specific SL_CVR override exists in the current schema.

## Step A4 — Calculate SAL_PD (Article-Grain Per-Day Sale)

Same CASE logic as MAJ_CAT Step 5, but inputs come from different tables:

| Input Field | Source |
|-------------|--------|
| CM_SAL_Q | MASTER_GEN_ART_SALE (joined on ST_CD + GEN_ART_NUMBER + CLR) |
| NM_SAL_Q | MASTER_GEN_ART_SALE (same join) |
| CM_REM_D | ARS_CALC_ST_MAJ_CAT (joined on ST_CD + MAJ_CAT) — borrowed |
| NM_REM_D | ARS_CALC_ST_MAJ_CAT (same join) — borrowed |
| SAL_D | ARS_CALC_ST_ART.SAL_D (from Step A3) |

### Query Shape
```sql
UPDATE C SET C.[SAL_PD] =
    CASE
        WHEN ISNULL(C.[SAL_D],0) = 0 OR MJ.[CM_REM_D] = 0 THEN 0
        WHEN MJ.[CM_REM_D] >= C.[SAL_D] THEN
            CAST(SA.[CM_SAL_Q] AS FLOAT) / MJ.[CM_REM_D]
        WHEN MJ.[NM_REM_D] = 0 THEN
            CAST(SA.[CM_SAL_Q] AS FLOAT) / MJ.[CM_REM_D]
        ELSE
            (CAST(SA.[CM_SAL_Q] AS FLOAT)
             + (CAST(SA.[NM_SAL_Q] AS FLOAT) / MJ.[NM_REM_D])
               * (C.[SAL_D] - MJ.[CM_REM_D])
            ) / C.[SAL_D]
    END
FROM ARS_CALC_ST_ART C
INNER JOIN MASTER_GEN_ART_SALE SA
    ON C.[ST_CD] = SA.[ST_CD]
   AND C.[GEN_ART_NUMBER] = SA.[GEN_ART_NUMBER]
   AND ISNULL(C.[CLR],'') = ISNULL(SA.[CLR],'')
LEFT JOIN ARS_CALC_ST_MAJ_CAT MJ
    ON C.[ST_CD] = MJ.[ST_CD]
   AND C.[MAJ_CAT] = MJ.[MAJ_CAT];
```

### Fallback (if MASTER_GEN_ART_SALE is missing)
Uses `CM_SAL_Q` / `NM_SAL_Q` columns directly from `ARS_CALC_ST_ART` if they exist there (legacy data carried from ST_ART).

## Final Output Columns

| Column | Source |
|--------|--------|
| ST_CD, MAJ_CAT, GEN_ART_NUMBER, CLR | grouping keys |
| LISTING, I_ROD | merged |
| MANUAL_MBQ | merged |
| FOCUS_W_CAP, FOCUS_WO_CAP | merged (CO override) |
| CORE, AUTO, HH_ART | merged (CO override) |
| SAL_D | Step A3 output |
| SAL_PD | Step A4 output |
| SALE_COVER_SRC | SL_CVR source audit |

---

# 3. PIPELINE ORDER

The orchestrator `calculate_per_day_sale(conn)` runs both table builds in this sequence:

```
 1.  ensure_primary_keys()              → add PK to master tables if missing
 2.  _step_create_calc()                → build ARS_CALC_ST_MAJ_CAT
 3.  _step_merge_co_values()            → merge CO_MAJ_CAT
 4.  _step_defaults()                   → apply column defaults
 5.  _step_sal_d()                      → MAJ_CAT SAL_D
 6.  _step_sal_pd()                     → MAJ_CAT SAL_PD

 7.  _step_master_sale_sal_pd()         → write SAL_PD directly on MASTER_GEN_ART_SALE
                                          (~21L rows — full option coverage for Listing)

 8.  _step_create_calc_art()            → build ARS_CALC_ST_ART
 9.  _step_merge_co_art()               → merge CO_ART
10.  _step_art_sal_d()                  → article SAL_D
11.  _step_art_sal_pd()                 → article SAL_PD (joins ART_SALE + MAJ_CAT REM_D)
```

## Why This Order Matters

- Steps 2–6 must complete before step 7 — `MASTER_GEN_ART_SALE.SAL_PD` calc joins `ARS_CALC_ST_MAJ_CAT` for REM_D and SAL_D.
- Steps 2–6 must complete before step 11 — article-level SAL_PD borrows REM_D from MAJ_CAT calc.
- Step 7 runs **in place on the master table** (adds a SAL_PD column), separate from `ARS_CALC_ST_ART` which is a derived working table. Both coexist because the master has ~21L rows while the working calc typically has only ~176K.

---

# 4. KEY COMPARISON TABLE

| Aspect | ARS_CALC_ST_MAJ_CAT | ARS_CALC_ST_ART |
|--------|---------------------|-----------------|
| Grain | ST_CD × MAJ_CAT | ST_CD × MAJ_CAT × GEN_ART × CLR |
| Typical row count | ~250K | ~176K |
| Primary purpose | Category-level base for Grid Builder + Listing | Article-level fallback + FOCUS/CORE flags |
| Core formula | SAL_D + SAL_PD per category | SAL_D + SAL_PD per option |
| CM_REM_D source | Calculated from master data | **Borrowed from MAJ_CAT calc** (joined by ST + MAJ_CAT) |
| Merge target | CO_MAJ_CAT | CO_ART |
| SL_CVR priority | ST_MAJ_CAT > CO_MAJ_CAT > ST_MASTER | ST_MASTER only |
| Sales input | CM_SAL_Q / NM_SAL_Q columns on itself | JOIN MASTER_GEN_ART_SALE |
| Downstream | Grid Builder MBQ, Listing Part 3.5 | Historical reference (SAL_PD on master is preferred) |

---

# 5. TROUBLESHOOTING

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| SAL_D = 0 for all rows | ST_MASTER missing or INT_DAYS/PRD_DAYS/SL_CVR blank | Upload ST_MASTER; verify all three columns populated |
| SAL_PD = 0 for most rows | CM_REM_D = 0 (no current-month data) | Check month-end / upload timing |
| SAL_PD = 0 only for some rows | Missing CM_SAL_Q for those store/category combos | Load sales data for affected categories |
| Article SAL_PD missing | MASTER_GEN_ART_SALE has no row for that option | Normal for options with no planned sale — fallback to MAJ_CAT |
| "SALE_COVER_SRC = ST_MASTER" everywhere | No ST_MAJ_CAT or CO_MAJ_CAT overrides — using store base | Normal if no category-specific overrides exist |
| CO_override didn't apply | CO value is NULL for that column | Check CO_MAJ_CAT upload — blank cells stay NULL |
| PK creation fails | Duplicate (ST_CD, MAJ_CAT) in Master_ALC_INPUT_ST_MAJ_CAT | Clean master table — each store×MAJ_CAT must be unique |

---

# 6. FILE LOCATIONS

All logic lives in `backend/app/services/grid_calculations.py`:

| Function | Lines | Purpose |
|----------|-------|---------|
| `ensure_primary_keys()` | 90-107 | Add PKs if missing |
| `_step_create_calc()` | 113-130 | Copy ST_MAJ_CAT → ARS_CALC_ST_MAJ_CAT |
| `_step_merge_co_values()` | 162-220 | Merge CO_MAJ_CAT with rule engine |
| `_step_defaults()` | 226-273 | Apply column defaults |
| `_step_sal_d()` | 279-336 | Calculate SAL_D with 3-priority SL_CVR |
| `_step_sal_pd()` | 341-370 | Calculate SAL_PD |
| `_step_master_sale_sal_pd()` | ~780 | Write SAL_PD to MASTER_GEN_ART_SALE |
| `_step_create_calc_art()` | 422-494 | Build ARS_CALC_ST_ART (with CO_ART fallback) |
| `_step_merge_co_art()` | 497-570 | Merge CO_ART values |
| `_step_art_sal_d()` | 572-603 | Article-level SAL_D |
| `_step_art_sal_pd()` | 605-714 | Article-level SAL_PD (ART_SALE + MAJ_CAT joins) |
| `calculate_per_day_sale()` | 723+ | Main orchestrator — runs all steps in order |

---

*ARS v2.0 Calc Tables Logic Reference — 15 April 2026*
