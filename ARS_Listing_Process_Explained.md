# ARS Listing Process — Complete Guide v3

> **Purpose:** Every major and minor step of the ARS Listing + Allocation pipeline explained in plain language with worked examples. Nothing is skipped.

---

## 1. What is the Listing Process?

> "Which products should we send to which stores, and how many pieces of each size?"

V2 Retail has 320+ stores and thousands of products. The Listing Process scans warehouse stock (MSA), store stock (Grid), and business rules to produce a precise allocation plan.

**Output Tables:**
| Table | Grain | Purpose |
|-------|-------|---------|
| `ARS_LISTING` | Store × Option | Full picture (grid + MSA), all options |
| `ARS_LISTING_WORKING` | Store × Option | Filtered eligible subset + ALLOC_FLAG + ALLOC_QTY |
| `ARS_ALLOC_WORKING` | Store × Option × Size | Size-level allocation quantities |
| `ARS_STORE_RANKING` | Store × MAJ_CAT | Store priority ranking |

---

## 2. Running Example

```
Store:     DELHI-101 (tagged to RDC = WH-NORTH)
MAJ_CAT:   LEGGING  (Accessories Density ACS_D = 5, Allocation Days ALC_D = 14)
Product A: GEN_ART = 50001, CLR = BLUE  (existing, AGE = 30 days, STK = 10)
Product B: GEN_ART = 50001, CLR = BLACK (existing, AGE = 30, STK = 3, low stock)
Product C: GEN_ART = 50002, CLR = RED   (new from MSA, AGE = 5, STK = 0)
```

---

## 3. Configurable Variables (UI Settings)

These are set in the UI before clicking Generate. All are saved to `AppSettings` for next session.

| Variable | Default | Meaning |
|----------|---------|---------|
| `stock_threshold_pct` | 0.6 (60%) | OPT_TYPE: RL when STK >= X% of ACS_D |
| `excess_multiplier` | 2.0 | Excess if STK > X × OPT_MBQ |
| `hold_days` | 0 | Extra days for IS_NEW=1 (transit buffer) |
| `age_threshold` | 15 | Articles with AGE < X use PER_OPT_SALE boost |
| `req_weight` | 0.4 | Store ranking: weight for requirement |
| `fill_weight` | 0.6 | Store ranking: weight for fill rate |
| `mix_mode` | st_maj_rng | MIX aggregation level |
| `enable_fallback` | false | Enable grid demotion + MBQ boost |
| `fallback_boost_mode` | static | "str" = dynamic STR-based, "static" = fixed % |
| `static_growth_pct` | 130 | Static boost multiplier (130 = +30%) |
| `str_tiers` | 30:150,45:130,60:120,90:110 | STR days:boost% pairs |

**RDC Modes:**
- `all` — All stores, all RDCs
- `own` — Stores see only their tagged RDC's options
- `cross` — Take options FROM one RDC, send TO stores of another

**Run Modes:**
- `listing` — Generate listing only (assumes calc tables + grids already exist)
- `full` — Pre-grid calculations → Grid build → Listing (end-to-end)

---

## 4. Full Pipeline (run_mode = "full")

When "Full Pipeline" is selected, these extra steps run BEFORE listing:

### 4A. Pre-Grid Calculations (`grid_calculations.py`)
Creates `ARS_CALC_ST_MAJ_CAT` and `ARS_CALC_ST_ART` with:
- **ACS_D** (Accessories Density): base display stock per store × MAJ_CAT
- **ALC_D** (Allocation Days): coverage period = INT_DAYS + PRD_DAYS + SL_CVR
- **SAL_PD** (Sale Per Day): daily sale forecast
- **DISP_Q, DISP_GR_DGR, BGT_SL_GR_DGR**: display and budget parameters
- **LISTING, I_ROD, CLR_MIN, CLR_MAX**: control flags
- **MANUAL_DENSITY**: article-level ACS_D override

### 4B. Grid Build (`grid_builder.py`)
Runs all active grids in parallel. Each grid:
1. Pivots ET_STORE_STOCK data by SLOC
2. Calculates **STK_TTL** = sum of KPI='STK' SLOCs (actual stock)
3. Calculates **STR** = sum of KPI='SAL' SLOCs (7-day sales)
4. Calculates **STR_DAYS** = STK_TTL / (STR / 7) — days of cover
5. Enriches with ACS_D, ALC_D, SAL_PD, CONT from calc tables
6. Calculates **MBQ** = (SAL_PD × BGT_SL_GR_DGR) × ALC_D + (DISP_Q × DISP_GR_DGR)
7. Calculates **OPT_CNT** = ROUND(DISP_Q × DISP_GR_DGR × CONT / ACS_D, 0)

---

## 5. Phase 1: Collect Grid Data (Existing Stock + Sales)

**Source:** `ARS_GRID_MJ_GEN_ART` (gen-art level grid)

**What happens:** Read every store's existing stock and sales data from the grid table.

**Two key totals:**
- **STK_TTL** = Sum of stock SLOCs only (e.g., 0001 + 0002 + 0004 + ...). Sale columns excluded.
- **STR** = Sum of sale SLOCs only (e.g., L-7 DAYS SALE-Q). Stock columns excluded.

### Example:

| WERKS | MAJ_CAT | GEN_ART | CLR | SLOC 0001 | SLOC 0002 | L-7 SALE | STK_TTL | STR | IS_NEW |
|-------|---------|---------|-----|-----------|-----------|----------|---------|-----|--------|
| DELHI-101 | LEGGING | 50001 | BLUE | 8 | 2 | 14 | **10** | **14** | 0 |
| DELHI-101 | LEGGING | 50001 | BLACK | 2 | 1 | 7 | **3** | **7** | 0 |

**SQL:**
```sql
INSERT INTO ARS_LISTING
SELECT G.[WERKS], S.[RDC], G.[MAJ_CAT], G.[GEN_ART_NUMBER], G.[CLR],
       [all_stock_cols], SUM(stock_slocs) AS STK_TTL, SUM(sale_slocs) AS STR,
       0 AS IS_NEW, NULL AS OPT_TYPE
FROM ARS_GRID_MJ_GEN_ART G
INNER JOIN (stores) S ON G.[WERKS] = S.[ST_CD]
```

---

## 6. Phase 2: Add MSA Missing Options (New Products)

**Source:** `ARS_MSA_GEN_ART` (warehouse recommendations)

**What happens:** For each store, check if MSA has products NOT in the grid. Add them with STK_TTL=0, STR=0, IS_NEW=1.

### Example:

MSA has product 50002-RED in WH-NORTH. DELHI-101 doesn't have it.

| WERKS | MAJ_CAT | GEN_ART | CLR | STK_TTL | STR | IS_NEW |
|-------|---------|---------|-----|---------|-----|--------|
| DELHI-101 | LEGGING | 50002 | RED | **0** | **0** | **1** |

**SQL:**
```sql
INSERT INTO ARS_LISTING
SELECT S.[ST_CD], S.[RDC], M.[MAJ_CAT], M.[GEN_ART_NUMBER], M.[CLR],
       0 (all stock), 0 AS STK_TTL, 0 AS STR, 1 AS IS_NEW, NULL AS OPT_TYPE
FROM MSA_options M
INNER JOIN (stores) S ON 1=1 [RDC join if own mode]
WHERE NOT EXISTS (already in ARS_LISTING)
```

---

## 7. Phase 2.5: Create Indexes

If listing has >= 5000 rows, create indexes on (WERKS, MAJ_CAT) and (GEN_ART_NUMBER) to speed up Part 4 grid joins.

---

## 8. Phase 3.5: Enrich ACS_D + ALC_D

**Source:** `ARS_CALC_ST_MAJ_CAT` (store × category grain)

| Column | Source Column | Meaning |
|--------|-------------|---------|
| ACS_D | ACS_D (or legacy DPN) | Base display stock (Accessories Density) |
| ALC_D | ALC_D (or legacy SAL_D) | Allocation coverage days |

**Backward compatible:** Checks for new names first (`ACS_D`), falls back to legacy (`DPN`) if not found.

### Example:
| WERKS | MAJ_CAT | ACS_D | ALC_D |
|-------|---------|-------|-------|
| DELHI-101 | LEGGING | 5 | 14 |

Meaning: Store needs 5 pieces as base display + 14 days of sales coverage.

---

## 9. Phase 3.5a: Enrich Control Flags (Two-Level Cascade)

### Level 1: From ARS_CALC_ST_MAJ_CAT (store × MAJ_CAT)
- **LISTING** (1=approved, 0=blocked)
- **I_ROD** (replenishment rounds)
- **CLR_MIN** (min colors in normal allocation)
- **CLR_MAX** (max colors in fallback allocation)

### Level 2: From ARS_CALC_ST_ART (store × article — OVERRIDES Level 1)
- **LISTING** (article-level override)
- **I_ROD** (article-level override)
- **FOCUS_W_CAP** (Focus With Capping: top priority, with requirement check)
- **FOCUS_WO_CAP** (Focus Without Capping: force-allocate, skip all checks)

### CLR_MIN / CLR_MAX Example:
```
GEN_ART 50001 (Legging) has 15 colors in warehouse.
CLR_MIN = 3  → Normal: allocate top 3 colors by demand
CLR_MAX = 8  → Fallback: allow up to 8 colors
Colors 9-15  → Never allocated regardless of mode
```

### FOCUS Example:
| Product | FOCUS_W_CAP | FOCUS_WO_CAP | Behavior |
|---------|------------|-------------|----------|
| 50001-BLUE | 0 | 0 | Normal priority, standard rules |
| 50003-PINK | 1 | 0 | Top priority but only if REQ > 0 |
| 50004-PROMO | 0 | 1 | Force-allocate, skip requirement check |

---

## 10. Phase 3.5b: Enrich AUTO_GEN_ART_SALE

**Source:** `MASTER_GEN_ART_SALE.SAL_PD`
**Grain:** Store × MAJ_CAT × GEN_ART_NUMBER × CLR
**Purpose:** Per-article daily sale rate used in OPT_MBQ when AGE >= threshold.

---

## 11. Phase 3.5c: Enrich AGE

**Source:** `MASTER_GEN_ART_AGE`
**Grain:** Store × MAJ_CAT × GEN_ART_NUMBER × CLR
**Purpose:** Days since product was first listed. Controls whether PER_OPT_SALE is included in OPT_MBQ rate.

| Product | AGE | Effect |
|---------|-----|--------|
| 50001-BLUE | 30 | >= 15 → excludes PER_OPT_SALE from rate |
| 50002-RED | 5 | < 15 → includes PER_OPT_SALE (new product boost) |

---

## 12. Phase 3.55: Pre-populate MSA_FNL_Q + Variant Counts

**Source:** `ARS_MSA_GEN_ART` (for FNL_Q) + `ARS_MSA_VAR_ART` (for variant counts)

| Column | Calculation | Example (50001-BLUE) |
|--------|-------------|---------------------|
| MSA_FNL_Q | SUM(FNL_Q) per option from MSA | 200 |
| VAR_COUNT | COUNT(DISTINCT variants) from MSA_VAR_ART | 5 (S,M,L,XL,XXL) |
| VAR_FNL_COUNT | COUNT(variants WHERE FNL_Q > 0) | 4 (XXL has no stock) |

**Why early?** Needed by Part 3.6 for OPT_TYPE classification (MIX(a) checks MSA_FNL_Q, MIX(b) checks VAR ratio).

---

## 13. Phase 3.6: OPT_TYPE Classification (4-Way + Fallback)

**This is the most important classification step.** OPT_TYPE determines allocation priority and behavior.

### Decision Logic (first match wins, top to bottom):

```
 1. MIX(a): ACS_D > 0 AND STK < 60% × ACS_D AND MSA_FNL_Q = 0
            → Low stock, warehouse can't help → MIX (excluded)

 2. MIX(b): VAR_COUNT > 0 AND VAR_FNL_COUNT / VAR_COUNT < 60%
            → Poor color fill in warehouse → MIX (excluded)

 3. RL:     ACS_D > 0 AND STK >= 60% × ACS_D
            → Adequate stock → Repeated Listed (replenish)

 4. TBC:    ACS_D > 0 AND 0 < STK < 60% × ACS_D AND MSA_FNL_Q > 0
            → Low stock, warehouse has more → To Be Check

 5. TBL:    STK <= 0 AND MSA_FNL_Q > 0
            → Zero stock, warehouse has it → To Be Listed (new)

 FALLBACK (when ACS_D is NULL or 0 — no calc data):
 6. MSA=0, STK=0  → MIX
 7. MSA=0, STK>0  → RL
 8. MSA>0, STK>0  → TBC
 9. MSA>0, STK<=0 → TBL
10. ELSE           → MIX
```

### Example:

| Product | ACS_D | STK_TTL | 60%×ACS_D | MSA_FNL_Q | VAR Ratio | OPT_TYPE | Why |
|---------|-------|---------|-----------|-----------|-----------|----------|-----|
| 50001-BLUE | 5 | 10 | 3 | 200 | 80% | **RL** | 10 >= 3 |
| 50001-BLACK | 5 | 3 | 3 | 200 | 80% | **RL** | 3 >= 3 (barely) |
| 50002-RED | 5 | 0 | 3 | 150 | 80% | **TBL** | STK=0, MSA>0 |
| 50001-GREY | 5 | 2 | 3 | 0 | - | **MIX** | STK<3, MSA=0 |

---

## 14. Phase 3.7: MIX Aggregation

**Rule:** ALL MIX rows for (WERKS, MAJ_CAT) merge into exactly 1 MIX row. Max 1 MIX per store × category.

**Aggregated MIX row:**
- GEN_ART_NUMBER = 0, CLR = 'MIX', GEN_ART_DESC = 'MIX'
- ACS_D / ALC_D: from ARS_CALC_ST_MAJ_CAT (MAX)
- All numeric columns: SUM
- IS_NEW = 0

Original MIX rows are deleted, aggregated row is inserted.

---

## 15. Phase 4a: Grid Column Joins

For each active grid (MJ, MJ_RNG_SEG, MJ_MACRO_MVGR, etc.):

**Columns added per grid (prefixed):**
- `{PREFIX}_STK_TTL` — stock for this grid's hierarchy
- `{PREFIX}_CONT` — contribution factor
- `{PREFIX}_MBQ` — minimum buy quantity for this grid
- `{PREFIX}_OPT_CNT` — option count
- `{PREFIX}_DISP_Q` — display quantity

**Master product columns** (from `vw_master_product`) are pre-resolved to avoid repeated joins to the 5M-row table.

---

## 16. Phase 4b: PER_OPT_SALE

From the grid marked `use_for_opt_sale=1`:

```
PER_OPT_SALE = ((MBQ - DISP_Q) / DISP_Q × ACS_D) / ALC_D
```

If DISP_Q=0 or ALC_D=0, PER_OPT_SALE=0.

### Example:
```
MBQ = 50, DISP_Q = 20, ACS_D = 5, ALC_D = 14
PER_OPT_SALE = ((50 - 20) / 20 × 5) / 14 = (1.5 × 5) / 14 = 0.536 per day
```

---

## 17. Phase 4c: OPT_MBQ + OPT_REQ (Core Formula)

### 17.1 MANUAL_DENSITY Override
If `ARS_CALC_ST_ART.MANUAL_DENSITY > 0`, it overrides ACS_D for that option. Checked from ARS_CALC_ST_ART first, then Master_ALC_INPUT_ST_ART as fallback.

### 17.2 Rate Selection (AGE-based)
```
L7_daily = (L-7 DAYS SALE-Q) / 7
auto_sale = AUTO_GEN_ART_SALE
per_opt = PER_OPT_SALE

IF AGE < 15 (or IS_NEW=1 → AGE forced to 0):
    rate = MAX(per_opt, L7_daily, auto_sale)    ← includes PER_OPT_SALE
ELSE (AGE >= 15):
    rate = MAX(L7_daily, auto_sale)             ← excludes PER_OPT_SALE
```

### 17.3 Formulas
```
OPT_MBQ    = ACS_D + rate × ALC_D
OPT_REQ    = MAX(0, OPT_MBQ - STK_TTL)
OPT_MBQ_WH = ACS_D + rate × (ALC_D + HOLD_DAYS if IS_NEW=1 else 0)
OPT_REQ_WH = MAX(0, OPT_MBQ_WH - STK_TTL)
MAX_DAILY_SALE = MAX(L7_daily, auto_sale)
```

### Example — Product A (50001-BLUE, AGE=30):
```
L7_daily = 14/7 = 2.0,  auto_sale = 0.8
rate = MAX(2.0, 0.8) = 2.0  (PER_OPT_SALE excluded, AGE >= 15)
OPT_MBQ = 5 + 2.0 × 14 = 33
OPT_REQ = MAX(0, 33 - 10) = 23
```

### Example — Product C (50002-RED, AGE=5, IS_NEW=1):
```
L7_daily = 0,  auto_sale = 0.8,  per_opt = 1.5
rate = MAX(1.5, 0, 0.8) = 1.5  (PER_OPT_SALE included, AGE < 15)
OPT_MBQ = 5 + 1.5 × 14 = 26
OPT_REQ = MAX(0, 26 - 0) = 26
OPT_MBQ_WH = 5 + 1.5 × (14 + 0) = 26  (hold_days=0)
OPT_REQ_WH = 26
```

---

## 18. Phase 4d: Excess Stock

```
EXCESS_STK = MAX(0, STK_TTL - 2 × OPT_MBQ)     [MIX rows excluded]
```

If store has more than 2x what it needs, the extra is "excess." Deducted at grid level in Phase 4e.

Example: 50001-BLUE: MAX(0, 10 - 2×33) = MAX(0, -56) = 0 (not excess)

---

## 19. Phase 4e: Per-Grid REQ with Excess Deduction

For each grid, aggregate ART_EXCESS by hierarchy columns, then:
```
{PREFIX}_REQ = MAX(0, {PREFIX}_MBQ - ({PREFIX}_STK_TTL - aggregated_excess))
```

---

## 20. Phase 6: Store Ranking

**Purpose:** When warehouse stock is limited, who gets served first?

### Formula (per MAJ_CAT):
```
FILL_RATE = MJ_STK_TTL / MJ_MBQ
REQ_RANK = ROW_NUMBER(ORDER BY MJ_REQ ASC)
FILL_RANK = ROW_NUMBER(ORDER BY FILL_RATE DESC)
W_SCORE = REQ_RANK × 0.4 + FILL_RANK × 0.6
ST_RANK = ROW_NUMBER(ORDER BY W_SCORE DESC)     ← Rank 1 = first served
```

### Example (3 stores for LEGGING):
| Store | MJ_REQ | FILL_RATE | W_SCORE | ST_RANK |
|-------|--------|-----------|---------|---------|
| DELHI-101 | 200 | 0.167 | 2.2 | **1** (first) |
| MUMBAI-55 | 150 | 0.333 | 2.0 | **2** |
| PUNE-23 | 80 | 0.667 | 1.8 | **3** (last) |

---

## 21. Phase 7: Create Working Table + Grid Coverage

### 21.1 Filter (ARS_LISTING → ARS_LISTING_WORKING)
Only rows passing ALL these conditions enter:
```
MSA_FNL_Q > 0                                           — warehouse has stock
OPT_REQ_WH >= 1                                         — store needs at least 1 piece
VAR_FNL_COUNT / VAR_COUNT >= 60% (or VAR_COUNT=0)       — color availability
LISTING = 1                                              — product approved
```

### 21.2 GH_ Columns (Raw Hierarchy Flags)
For each grid, `GH_{GRID}` = 1 if the hierarchy applies to this MAJ_CAT (from `ARS_GRID_HIERARCHY`), else 0.

Special: `GH_MJ` = always 1 (base grid always applies).

### 21.3 H_ Columns (Demand × Hierarchy)
```
H_{GRID} = (1 if {GRID}_REQ > 0 OR OPT_REQ_WH > 0) × GH_{GRID}
```
**Key fix:** `OPT_REQ_WH > 0` is checked because an option can need stock even when the aggregate grid-level REQ is zero (store has enough stock overall but this specific option needs replenishment).

### 21.4 PRI_CT% and ALLOC_FLAG
```
PRI_CT% = SUM(all H_Primary) / SUM(all GH_Primary) × 100
SEC_CT% = SUM(all H_Secondary) / SUM(all GH_Secondary) × 100
ALLOC_FLAG = 1  if PRI_CT% >= 100  (eligible)
ALLOC_FLAG = 0  if PRI_CT% < 100   (needs fallback)
```

### Example:
| Grid | Group | GH (applies?) | H (demand?) | Covered? |
|------|-------|---------------|-------------|----------|
| MJ | Primary | 1 | 1 (OPT_REQ_WH=26 > 0) | YES |
| RNG_SEG | Primary | 1 | 1 (RNG_SEG_REQ > 0) | YES |
| MACRO_MVGR | Primary | 1 | 1 (OPT_REQ_WH > 0) | YES |

PRI_CT% = (1+1+1)/(1+1+1) × 100 = **100%** → ALLOC_FLAG = **1**

---

## 22. Phase 8: Allocation (9 Internal Steps)

### Step 1: Create ARS_ALLOC_WORKING
Join working table with `ARS_MSA_VAR_ART` to get **size-level** rows.
Only rows where ALLOC_FLAG=1 AND FNL_Q>0. Carries: FOCUS_W_CAP, FOCUS_WO_CAP, CLR_MIN, CLR_MAX.

### Step 2: Enrich Size Data
- **STK_TTL** (per variant from `ARS_GRID_MJ_VAR_ART`)
- **CONT** (size contribution from `Master_CONT_SZ`, fallback: Store → CO → auto 1/count)
- **SZ_MBQ** = OPT_MBQ × CONT
- **SZ_REQ** = MAX(0, SZ_MBQ - STK_TTL)

### Example (50002-RED, OPT_MBQ=26):
| SZ | CONT | SZ_MBQ | STK_TTL | SZ_REQ |
|----|------|--------|---------|--------|
| S | 0.20 | 5.2 | 0 | **5** |
| M | 0.35 | 9.1 | 0 | **9** |
| L | 0.30 | 7.8 | 0 | **8** |
| XL | 0.15 | 3.9 | 0 | **4** |

### Step 3: Add Tracking + Audit Columns
Adds to alloc table: ALLOC_QTY, ALLOC_ROUND, SKIP_FLAG, ROUND_ALLOC, ALLOC_STATUS, SKIP_REASON, ALLOC_TYPE, ALLOC_BATCH_ID, FINAL_OPT_TYPE, OPT_TYPE_REASON, FOCUS_FLAG, CLR_CAP_MODE, STR_BOOST_PCT.

Generates `ALLOC_BATCH_ID` = `LST_YYYYMMDD_HHMMSS` (unique per run).

### Step 4: Create Pool Tracker
```
Pool = MAX(FNL_Q) per (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
Tracks: FNL_Q_ORIG (starting) and FNL_Q_REM (remaining after allocation)
```

### Step 5: Mark Initial Eligibility (E1–E5)
| Check | Rule | Remark if Fail |
|-------|------|---------------|
| E3 | OPT_TYPE ≠ 'MIX' | E3:OPT_TYPE=MIX |
| E1 | LISTING = 1 | E1:LISTING!=1 |
| E2 | ALLOC_FLAG = 1 | E2:ALLOC_FLAG=0(PRI_CT%=X) |
| E4 | MSA_FNL_Q > 0 | E4:MSA_FNL_Q=0 |
| E5 | OPT_REQ_WH >= 1 **OR FOCUS_WO_CAP=1** | E5:OPT_REQ_WH=X<1 |

FOCUS_WO_CAP=1 **bypasses E5** — force-allocated regardless of requirement.

### Step 6: Primary Allocation (RL → TBC → TBL)

**Priority within each OPT_TYPE:**
1. FOCUS_WO_CAP = 1 (force, first)
2. FOCUS_W_CAP = 1 (priority, second)
3. Normal by ST_RANK (1, 2, 3...)

**I_ROD Rounds:**
- Round 1: SZ_REQ = OPT_MBQ × 1 × CONT - STK
- Round N: SZ_REQ = OPT_MBQ × N × CONT - STK - already_allocated

**Waterfall Example (Size M, Pool = 40):**
```
Store DELHI-101 (Rank 1): needs 9  → gets 9,  pool = 31  ✓
Store MUMBAI-55 (Rank 2): needs 12 → gets 12, pool = 19  ✓
Store PUNE-23   (Rank 3): needs 8  → gets 8,  pool = 11  ✓
Store JAIPUR-7  (Rank 4): needs 15 → gets 11, pool = 0   ◐ PARTIAL
Store AGRA-12   (Rank 5): needs 10 → gets 0,  pool = 0   ✗ NOT SERVED
```

**Size Break Rules (DIFFERENT for RL/TBC vs TBL):**

| OPT_TYPE | Size Break Check | Why |
|----------|-----------------|-----|
| **RL** | **SKIP** — no size break | Already listed. Replenish whatever sizes available. |
| **TBC** | **SKIP** — no size break | Already on shelf. Partial replenishment > none. |
| **TBL** | **APPLY** — 60% threshold | New listing needs proper size coverage for display. |

**Post-allocation sync (after each round):**
- MSA_FNL_Q updated (remaining pool)
- OPT_REQ_WH recalculated (remaining demand)
- VAR_FNL_COUNT updated (variants still in stock)

### Step 7: Fallback Allocation (Optional)

Runs when `enable_fallback = true`. For options with ALLOC_FLAG=0:

**Grid Demotion Loop:**
```
For each Primary grid (seq DESC, skip seq=1):
  1. Demote grid → Secondary
  2. Recalculate PRI_CT% and ALLOC_FLAG
  3. Find newly eligible options (ALLOC_FLAG now = 1)
  4. Insert them into alloc table
  5. Enrich (STK_TTL, CONT, SZ_MBQ, SZ_REQ)
  6. Apply STR/Static MBQ boost (see below)
  7. Allocate newly eligible only (only_new=True)
  8. Repeat with next grid
Restore all demoted grids to Primary
```

**STR-Based MBQ Boost (during fallback):**

STR_DAYS = STK_TTL / (STR / 7) = "how many days until stockout?"

| STR Days | Boost | Meaning | MBQ 26 → |
|----------|-------|---------|----------|
| < 30 | +50% | Critical, < 1 month | 39 |
| < 45 | +30% | Low, 1-1.5 months | 34 |
| < 60 | +20% | Moderate, ~2 months | 31 |
| < 90 | +10% | Comfortable | 29 |
| >= 90 | 0% | Plenty | 26 |
| STR=0 | static% | Can't calculate → fallback to 130% | 34 |

Tiers are configurable via `str_tiers` setting (format: `days:pct,days:pct,...`).

**Boost applies to:**
```
OPT_MBQ    → OPT_MBQ × boost%
OPT_MBQ_WH → OPT_MBQ_WH × boost%
SZ_MBQ     → recalculated from boosted OPT_MBQ × CONT
SZ_REQ     → recalculated: MAX(0, SZ_MBQ - STK_TTL)
```

### Step 8: Reflect to Working + Final OPT_TYPE

**Reflect ALLOC_QTY:**
```
SUM(ALLOC_QTY) per (WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR) → ARS_LISTING_WORKING.ALLOC_QTY
```

**ALLOC_STATUS on working table:**
| Status | Condition |
|--------|-----------|
| ALLOCATED | ALLOC_QTY >= OPT_MBQ |
| PARTIAL | ALLOC_QTY > 0 but < OPT_MBQ |
| INELIGIBLE | Failed E1-E5 |
| NOT_PROCESSED | Was PENDING, never reached |

**FINAL_OPT_TYPE (OPT_TYPE stays unchanged):**
| OPT_TYPE | Condition | FINAL_OPT_TYPE | OPT_TYPE_REASON |
|----------|-----------|---------------|-----------------|
| RL | Always | RL | RL:stays_RL |
| TBC | Allocated | RL | TBC->RL:allocated_Xpcs |
| TBC | Not allocated | MIX | TBC->MIX:not_allocated |
| TBL | Allocated | NL (New Listed) | TBL->NL:new_listed_Xpcs |
| TBL | Not allocated | TBL | TBL:not_allocated |
| MIX | Always | MIX | - |

### Step 9: Cleanup
Drop temp tables (#alloc_pool, ARS_ALLOC_BREAK_RANKS).

---

## 23. Audit Trail

Every allocation run records these fields for full traceability:

| Field | Example | Purpose |
|-------|---------|---------|
| ALLOC_BATCH_ID | LST_20260419_171900 | Unique per run |
| ALLOC_TYPE | PRIMARY / FALLBACK | Which pass |
| OPT_TYPE | RL / TBC / TBL | Initial classification (never changed) |
| FINAL_OPT_TYPE | RL / NL / MIX | Post-allocation result |
| OPT_TYPE_REASON | TBC->RL:allocated_15pcs | Why it converted |
| ALLOC_STATUS | ALLOCATED / PARTIAL / SKIPPED | Outcome |
| SKIP_REASON | B3:SZ_AVAIL<60%,BREAK@RANK=4 | Why skipped |
| ALLOC_REMARKS | RL R1:QTY=15; RL R2:QTY=8 | Step-by-step trail |
| FOCUS_FLAG | NORMAL / W_CAP / WO_CAP | Focus priority used |
| CLR_CAP_MODE | NORMAL / FALLBACK | Color capping mode |
| STR_BOOST_PCT | 150 | Boost % applied (100 = no boost) |

---

## 24. Complete Flow Diagram

```
INPUT TABLES:
  ARS_GRID_MJ_GEN_ART  → store stock + sales (STK_TTL, STR)
  ARS_MSA_GEN_ART       → warehouse options (FNL_Q)
  ARS_MSA_VAR_ART       → warehouse sizes (per variant)
  ARS_CALC_ST_MAJ_CAT   → ACS_D, ALC_D, LISTING, I_ROD, CLR_MIN/MAX
  ARS_CALC_ST_ART       → FOCUS flags, article overrides
  Master_CONT_SZ        → size contribution %
  ARS_GRID_BUILDER      → grid definitions (Primary/Secondary)
  ARS_GRID_HIERARCHY    → hierarchy flags per MAJ_CAT
  Master_ALC_INPUT_ST_MASTER → store-RDC mapping

PROCESS:
  Phase 1:  Grid data → STK_TTL + STR, IS_NEW=0
  Phase 2:  MSA missing → STK=0, STR=0, IS_NEW=1
  Phase 2.5: Indexes (if >= 5000 rows)
  Phase 3.5: Enrich ACS_D, ALC_D
  Phase 3.5a: Enrich LISTING, I_ROD, CLR_MIN/MAX, FOCUS flags
  Phase 3.5b: Enrich AUTO_GEN_ART_SALE
  Phase 3.5c: Enrich AGE
  Phase 3.55: Pre-populate MSA_FNL_Q, VAR_COUNT, VAR_FNL_COUNT
  Phase 3.6: Tag OPT_TYPE (MIX/RL/TBC/TBL)
  Phase 3.7: MIX aggregation (1 per store × MAJ_CAT)
  Phase 4a: Grid column joins (MBQ, STK_TTL, CONT per grid)
  Phase 4b: PER_OPT_SALE from designated grid
  Phase 4c: OPT_MBQ = ACS_D + rate × ALC_D, OPT_REQ, OPT_MBQ_WH, OPT_REQ_WH
  Phase 4d: EXCESS_STK = MAX(0, STK - 2×OPT_MBQ)
  Phase 4e: Per-grid REQ with excess deduction
  Phase 6:  Store ranking (ST_RANK per MAJ_CAT)
  Phase 7:  Working table + GH_/H_/PRI_CT%/ALLOC_FLAG
  Phase 8:  Allocation engine:
    Step 1: Create alloc working (size-level join)
    Step 2: Enrich CONT, SZ_MBQ, SZ_REQ
    Step 3: Tracking + audit columns
    Step 4: Pool tracker (FNL_Q_REM)
    Step 5: Eligibility (E1-E5, FOCUS bypass)
    Step 6: Primary allocation (RL→TBC→TBL, FOCUS priority, I_ROD rounds)
            RL/TBC: no size break | TBL: 60% size break
    Step 7: Fallback (grid demotion + STR/Static MBQ boost)
    Step 8: Reflect ALLOC_QTY + FINAL_OPT_TYPE
    Step 9: Cleanup

OUTPUT:
  ARS_LISTING         → all options
  ARS_LISTING_WORKING → eligible + ALLOC_STATUS + FINAL_OPT_TYPE
  ARS_ALLOC_WORKING   → size-level allocations + audit trail
  ARS_STORE_RANKING   → store priorities
```

---

## 25. Glossary

| Term | Full Name | Meaning |
|------|-----------|---------|
| ACS_D | Accessories Density | Base display stock (replaces DPN) |
| ALC_D | Allocation Days | Days of supply to send (replaces SAL_D) |
| STR | Sales Turn Rate | Sum of L-7 sale SLOCs from grid |
| STR_DAYS | Days of Stock Cover | STK_TTL / (STR/7) |
| OPT_MBQ | Option Min Buy Qty | ACS_D + rate × ALC_D |
| OPT_REQ | Option Requirement | MAX(0, OPT_MBQ - STK_TTL) |
| OPT_REQ_WH | Requirement With Hold | Includes hold_days for IS_NEW=1 |
| STK_TTL | Stock Total | Sum of stock SLOCs (KPI=STK) |
| MSA_FNL_Q | MSA Final Quantity | Warehouse stock per option |
| CONT | Contribution | Size share (e.g., M = 35%) |
| SZ_MBQ | Size MBQ | OPT_MBQ × CONT |
| SZ_REQ | Size Requirement | MAX(0, SZ_MBQ - STK_TTL) |
| I_ROD | Rounds | Allocation passes count |
| PRI_CT% | Primary Coverage | SUM(H_pri)/SUM(GH_pri) × 100 |
| ALLOC_FLAG | Allocation Flag | 1 if PRI_CT% >= 100 |
| ST_RANK | Store Rank | 1 = first served |
| CLR_MIN | Color Minimum | Normal allocation color cap |
| CLR_MAX | Color Maximum | Fallback allocation color cap |
| FOCUS_W_CAP | Focus With Capping | Top priority + REQ check |
| FOCUS_WO_CAP | Focus Without Cap | Force-allocate, skip checks |
| RL | Repeated Listed | Existing product, replenished |
| NL | New Listed | TBL successfully allocated |
| TBC | To Be Check | Low stock, warehouse check |
| TBL | To Be Listed | New to store, warehouse has it |
| MIX | Mixed/Excluded | Excluded from allocation |

---

## 26. End-to-End Trace: Product 50002-RED at DELHI-101

```
Phase 1:  Not in grid → skipped (IS_NEW will be 1)
Phase 2:  MSA adds it: STK=0, STR=0, IS_NEW=1
Phase 3.5: ACS_D=5, ALC_D=14
Phase 3.5a: LISTING=1, I_ROD=2, CLR_MIN=3, CLR_MAX=8, FOCUS=NORMAL
Phase 3.5c: AGE=5 (< 15 → new product rate)
Phase 3.55: MSA_FNL_Q=150, VAR_COUNT=5, VAR_FNL_COUNT=4
Phase 3.6: STK=0, MSA>0 → OPT_TYPE = TBL
Phase 4b: PER_OPT_SALE = 1.5
Phase 4c: rate=MAX(1.5, 0, 0.8)=1.5 → OPT_MBQ = 5 + 1.5×14 = 26
          OPT_REQ = 26, OPT_MBQ_WH = 26, OPT_REQ_WH = 26
Phase 6:  DELHI-101 = ST_RANK 1 for LEGGING
Phase 7:  Passes all filters → ARS_LISTING_WORKING
          PRI_CT%=100% → ALLOC_FLAG=1
Step 1:   Joined with MSA_VAR_ART → 4 size rows (S,M,L,XL)
Step 2:   CONT: S=0.20, M=0.35, L=0.30, XL=0.15
          SZ_MBQ: 5, 9, 8, 4 → SZ_REQ: 5, 9, 8, 4
Step 5:   E1-E5 all pass → PENDING
Step 6:   TBL pass, Rank 1 → Gets 26 pcs (S=5, M=9, L=8, XL=4)
          Size break: 4/4 = 100% >= 60% → OK
Step 8:   ALLOC_QTY=26, ALLOC_STATUS=ALLOCATED
          FINAL_OPT_TYPE=NL, REASON=TBL->NL:new_listed_26pcs
```

**Result: DELHI-101 receives 26 pieces of 50002-RED (5×S, 9×M, 8×L, 4×XL) as a New Listing (NL).**

---

*v3 — Complete with all major and minor steps, ACS_D/ALC_D, STR, Focus priority, color capping, differential size break, STR-based fallback boost, audit trail. April 2026.*
