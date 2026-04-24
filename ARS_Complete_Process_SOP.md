# ARS v2.0 — Complete Process & SOP
## MSA Stock Calculation | Grid Builder | Listing Generation

**Detailed step-by-step process guide with formulas, examples, SQL code, and SOPs**

---

# 1. MSA STOCK CALCULATION

## 1.1 What Is MSA?

MSA stands for **Main Storage Area** — the central warehouse/RDC where stock physically sits before being allocated to stores.

The MSA Stock Calculation answers: *"For each MSA (warehouse), how much stock is actually available for allocation after deducting pending shipments?"*

It transforms raw warehouse stock data into a clean, aggregated recommendation list. Only articles with sufficient available stock (above a configurable threshold) make it into the recommendation — everything else is filtered out.

**Business Meaning:** If an article's available stock at the MSA is below 25 units (default threshold), it's not worth recommending because the quantity is too small to distribute meaningfully across stores.

## 1.2 Input Tables

| Table | What It Contains | Example Row |
|-------|-----------------|-------------|
| VW_ET_MSA_STK_WITH_MASTER | Stock per store/SLOC/article joined with product master | ST_CD=DH24, SLOC=0001, ARTICLE_NUMBER=1116113204, STK_Q=500, MAJ_CAT=M_TEES_HS, CLR=BLK, SZ=M |
| MASTER_ALC_PEND | Pending allocations (stock committed but not yet shipped) | RDC=DH24, ARTICLE_NUMBER=1116113204, MOA=NORMAL, QTY=150 |

## 1.3 Output Tables

| Table | What It Contains | Who Uses It |
|-------|-----------------|-------------|
| ARS_MSA_TOTAL | Full pivot — every article with all SLOC columns | Debugging / detailed analysis |
| ARS_MSA_GEN_ART | Aggregated recommendations (1 row per RDC/MAJ_CAT/GEN_ART/CLR) | **Listing pipeline** (main input) |
| ARS_MSA_VAR_ART | Color variants that passed threshold | **Listing pipeline** (VAR_COUNT, allocation variant expansion) |

## 1.4 The 9-Step Algorithm — Explained

### Step 1 — Filter by SLOC

**What:** Keep only stock from selected storage locations.

**Why:** Different SLOCs represent different types of stock (sellable, damaged, intransit). You typically only want sellable stock SLOCs.

**Code logic:**
```python
if slocs_selected and "SLOC" in df.columns:
    df = df[df["SLOC"].isin(slocs_selected)]
# If no SLOCs selected → use all SLOCs (no filter)
```

**Example:** You select SLOCs [0001, 0002, 0004]. Only rows with these 3 SLOCs are kept. Stock from SLOC=0099 (damaged goods) is excluded.

### Step 2 — Numeric Safety

**What:** Convert STK_Q (stock quantity) from any format to a clean number.

**Why:** Source data sometimes has text ("N/A"), blanks, or special characters in numeric fields. Without this step, calculations would crash.

**Code logic:**
```python
df["STK_Q"] = pd.to_numeric(df["STK_Q"], errors="coerce").fillna(0)
# "coerce" = if it can't be converted, make it NaN
# .fillna(0) = replace NaN with 0
```

**Example:**
| Before | After |
|--------|-------|
| 500 | 500 |
| "N/A" | 0 |
| NULL | 0 |
| 123.45 | 123.45 |

### Step 3 — Default Fill

**What:** Replace missing values in dimension columns with safe defaults.

**Why:** When we GROUP BY or PIVOT, NULL values cause problems — SQL treats NULL != NULL, creating phantom groups. Filling with defaults ensures clean grouping.

**Defaults applied:**

| Column | Default | Why This Default |
|--------|---------|-----------------|
| CLR (Color) | "A" | "A" = generic/default color code |
| SZ (Size) | "A" | "A" = generic/one-size |
| M_VND_CD (Vendor Code) | 0 | 0 = unknown vendor |
| M_VND_NM (Vendor Name) | "NA" | Standard "not available" |
| MACRO_MVGR, MICRO_MVGR, FAB, SSN | "NA" | Standard "not available" |

**Example:** An article with CLR=NULL becomes CLR="A" so it groups cleanly with other single-color articles.

### Step 4 — Segment Filter

**What:** Keep only Apparel (APP) and General Merchandise (GM) segments.

**Why:** ARS is designed for apparel/GM allocation. Other segments (e.g., electronics, food) have different replenishment logic and are excluded.

**Code logic:**
```python
if "SEG" in df.columns:
    df = df[df["SEG"].isin(["APP", "GM"])]
```

### Step 4B — Category RLS Filter

**What:** If the logged-in user has Row-Level Security restrictions, only process their allowed MAJ_CATs.

**Why:** A planner responsible for "M_TEES" should only see/process tees, not all categories.

**Example:** User "planner_north" is assigned MAJ_CATs ["M_TEES_HS", "M_SHIRTS"]. Only these 2 categories are processed. Admin users see everything.

### Step 5 — Pivot by SLOC

**What:** Transform the data from "long" format (one row per article/SLOC) to "wide" format (one row per article, one column per SLOC).

**Why:** We need to see all SLOCs side-by-side for each article to calculate total stock and eventually deduct pending.

**Before (long format — 6 rows for 1 article, 2 SLOCs, 3 sizes):**

| ST_CD | SLOC | GEN_ART | CLR | SZ | STK_Q |
|-------|------|---------|-----|----|-------|
| DH24 | 0001 | 1116113204 | BLK | S | 200 |
| DH24 | 0001 | 1116113204 | BLK | M | 300 |
| DH24 | 0001 | 1116113204 | BLK | L | 100 |
| DH24 | 0002 | 1116113204 | BLK | S | 100 |
| DH24 | 0002 | 1116113204 | BLK | M | 150 |
| DH24 | 0002 | 1116113204 | BLK | L | 50 |

**After (wide format — 3 rows, SLOCs as columns):**

| ST_CD | GEN_ART | CLR | SZ | 0001 | 0002 | STK_QTY |
|-------|---------|-----|----|------|------|---------|
| DH24 | 1116113204 | BLK | S | 200 | 100 | **300** |
| DH24 | 1116113204 | BLK | M | 300 | 150 | **450** |
| DH24 | 1116113204 | BLK | L | 100 | 50 | **150** |

**STK_QTY** = 0001 + 0002 = total stock across all SLOCs for that size.

**SQL equivalent:**
```sql
SELECT ST_CD, GEN_ART_NUMBER, CLR, SZ,
    ISNULL([0001], 0) AS [0001],
    ISNULL([0002], 0) AS [0002],
    ISNULL([0001], 0) + ISNULL([0002], 0) AS STK_QTY
FROM source_data
PIVOT (SUM(STK_Q) FOR SLOC IN ([0001], [0002])) pvt
```

### Step 6 — Merge Pending Allocations

**What:** Join the stock data with pending allocation data and calculate how much is already committed.

**Why:** If 1000 units are in the warehouse but 200 are already allocated to other shipments, only 800 are truly available.

**Pending data (from MASTER_ALC_PEND):**

| RDC | ARTICLE_NUMBER | MOA | QTY |
|-----|---------------|-----|-----|
| DH24 | 1116113204 | NORMAL | 150 |
| DH24 | 1116113204 | EXPRESS | 50 |

**After pivot:** PEND_QTY = 150 + 50 = **200**

**After merge:** Each stock row now has an additional PEND_QTY column. If no pending allocation exists for an article, PEND_QTY = 0.

### Step 7 — Calculate Final Quantity

**The core formula:**
```
FNL_Q = MAX(STK_QTY - PEND_QTY, 0)
```

**Why MAX with 0?** Pending allocations can theoretically exceed stock (over-allocation). We cap at 0 — never show negative availability.

**Worked example for each size:**

| SZ | STK_QTY | PEND_QTY | FNL_Q = MAX(STK - PEND, 0) |
|----|---------|----------|---------------------------|
| S | 300 | 67 (distributed) | MAX(233, 0) = **233** |
| M | 450 | 100 | MAX(350, 0) = **350** |
| L | 150 | 33 | MAX(117, 0) = **117** |

(PEND_QTY distributed proportionally across sizes in the actual implementation.)

### Step 8 — Generate Color Variants (Threshold Filter)

**What:** Group articles by (ST_CD, MAJ_CAT, GEN_ART_NUMBER, CLR) and only keep groups with sufficient total available stock.

**Why:** No point recommending an article-color to stores if the RDC only has 10 units total — not enough to meaningfully distribute.

**Threshold:** Default = **25 units** (configurable 0-100).

**How it works:**

| ST_CD | MAJ_CAT | GEN_ART | CLR | Total FNL_Q (all sizes) | Threshold=25 | Keep? |
|-------|---------|---------|-----|------------------------|--------------|-------|
| DH24 | M_TEES_HS | 1116113204 | BLK | 700 | 700 > 25 | YES |
| DH24 | M_TEES_HS | 1116113204 | RED | 15 | 15 < 25 | **NO** |
| DH24 | M_TEES_HS | 1116112695 | GRN | 800 | 800 > 25 | YES |

**Important:** The threshold is applied at the **color group level**, not individual sizes. If the color has 700 total across all sizes, ALL sizes for that color are kept — even if size XS only has 2 units.

### Step 9 — Aggregate to Hierarchy

**What:** Remove size granularity — collapse all sizes into one row per (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR). SUM all numeric columns.

**Why:** The listing pipeline works at the option level (MAJ_CAT × GEN_ART × CLR), not size level. Size-level allocation happens later in Part 8.

**Before (3 rows for 3 sizes):**

| ST_CD | GEN_ART | CLR | SZ | 0001 | 0002 | STK_QTY | PEND_QTY | FNL_Q |
|-------|---------|-----|----|------|------|---------|----------|-------|
| DH24 | 1116113204 | BLK | S | 200 | 100 | 300 | 67 | 233 |
| DH24 | 1116113204 | BLK | M | 300 | 150 | 450 | 100 | 350 |
| DH24 | 1116113204 | BLK | L | 100 | 50 | 150 | 33 | 117 |

**After (1 row, sizes summed):**

| RDC | MAJ_CAT | GEN_ART | CLR | 0001 | 0002 | STK_QTY | PEND_QTY | FNL_Q |
|-----|---------|---------|-----|------|------|---------|----------|-------|
| DH24 | M_TEES_HS | 1116113204 | BLK | 600 | 300 | 900 | 200 | **700** |

Note: ST_CD is renamed to **RDC** in the output.

## 1.5 Complete Worked Example — End to End

**Scenario:** RDC DH24, Article 1116113204 (M_TEES_HS), Color BLK, 3 sizes (S/M/L), 2 SLOCs (0001/0002), pending allocation of 200 units.

| Step | Action | Result |
|------|--------|--------|
| 1 | Filter SLOCs [0001, 0002] | 6 rows kept |
| 2 | Numeric safety | STK_Q all clean numbers |
| 3 | Default fill | CLR="BLK" (already set), SZ=S/M/L (already set) |
| 4 | Segment filter | SEG=APP → kept |
| 5 | Pivot | 3 rows (one per size), STK_QTY = 300/450/150 |
| 6 | Merge pending | PEND_QTY = 200 total |
| 7 | FNL_Q | 233 + 350 + 117 = 700 total |
| 8 | Threshold check | 700 > 25 → KEEP |
| 9 | Aggregate | 1 row: RDC=DH24, FNL_Q=700 |

**Final output in ARS_MSA_GEN_ART:** DH24 / M_TEES_HS / 1116113204 / BLK / FNL_Q=700

This article-color is now recommended for allocation to stores served by RDC DH24.

## 1.6 SOP — Running MSA Stock Calculation

### Before You Run
- [ ] Data Checklist → VW_ET_MSA_STK_WITH_MASTER refreshed today
- [ ] Data Checklist → MASTER_ALC_PEND refreshed today
- [ ] Store SLOC Validation → no "New" SLOCs pending

### Steps
1. Open **Data Preparation → MSA Stock Calculation**
2. Select SLOCs (leave empty for all active SLOCs)
3. Set threshold (default 25 — increase to be more selective, decrease to include more options)
4. Click **Run**
5. Monitor progress (9 steps, typically 1-3 minutes)
6. **Verify:** ARS_MSA_GEN_ART should have 10K-20K+ rows for a full run
7. **Spot check:** Open ARS_MSA_GEN_ART → verify FNL_Q > 0 for rows, check a known article

### Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| 0 rows output | Source view empty or all filtered | Check VW_ET_MSA_STK_WITH_MASTER has data; check SLOC/SEG filters |
| Very few rows | Threshold too high | Lower threshold from 25 to 10 or 5 |
| FNL_Q all 0 | PEND_QTY > STK_QTY for everything | Check MASTER_ALC_PEND — is it stale? Are allocations over-committed? |
| Takes > 10 min | Large dataset (millions of rows) | Normal for full run; use SLOC filter to reduce scope |

---

# 2. PRE-GRID CALCULATIONS (Build Calc Tables)

## 2.1 What Are Calc Tables?

Before the Grid Builder can compute MBQ (how much to stock) and OPT_CNT (how many options), it needs several intermediate values:
- **ALC_D** — how many days of forward stock cover is the target?
- **SAL_PD** — how many units per day does this store/category sell?
- **CONT** — what percentage of the category does this sub-segment represent?
- **DISP_Q** — how many display units per fixture?

These values come from **multiple master tables** with different priority rules (store-level overrides company-level). The Build Calc Tables step resolves all these priorities and writes the final merged result into two working tables:

| Working Table | Grain | Typical Rows |
|--------------|-------|-------------|
| ARS_CALC_ST_MAJ_CAT | Store × Major Category | ~250K-400K |
| ARS_CALC_ST_ART | Store × Article (× Color) | ~175K |

## 2.2 Column Name Reference

The system recently renamed these columns. Both old and new names are supported:

| New Name | Old Name | Meaning |
|----------|----------|---------|
| **ACS_D** | DPN | Average Consumption per Store / Display-Per-Norm (floor stock units) |
| **ALC_D** | SAL_D | Allocation Lifecycle Days (sale-cover window) |
| **SAL_PD** | SAL_PD | Per-Day Sale rate (unchanged) |
| **MANUAL_DENSITY** | MANUAL_MBQ | Article-level ACS_D override |

## 2.3 MAJ_CAT Level Pipeline — Step by Step

### Step 1 — Create Working Table

**What:** Copy `Master_ALC_INPUT_CO_MAJ_CAT` (company-level data) as the base, cross-joined with all active stores from `Master_ALC_INPUT_ST_MASTER`. Then overlay `Master_ALC_INPUT_ST_MAJ_CAT` (store-level data) on top.

**Why this order?** CO (company) provides defaults that apply to **all stores**. ST (store) provides overrides for **specific stores**. Store data wins where it exists.

**Example:**
```
CO says: MAJ_CAT=M_TEES_HS has ACS_D=20, CONT=0.5
ST says: Store HB05 / M_TEES_HS has ACS_D=24 (different from CO)
Result: HB05/M_TEES_HS → ACS_D=24 (ST wins), CONT=0.5 (CO carried forward)
```

**SQL (optimized — single UPDATE):**
```sql
-- Step 1: CO base × all stores
SELECT ST.[ST_CD], CO.* INTO ARS_CALC_ST_MAJ_CAT
FROM Master_ALC_INPUT_ST_MASTER ST
CROSS JOIN Master_ALC_INPUT_CO_MAJ_CAT CO;

-- Step 2: Overlay ST values where non-null (1 pass, all columns)
UPDATE C SET
    C.[CM_REM_D] = CASE WHEN S.[CM_REM_D] IS NOT NULL AND S.[CM_REM_D]<>'' THEN S.[CM_REM_D] ELSE C.[CM_REM_D] END,
    C.[CM_SAL_Q] = CASE WHEN S.[CM_SAL_Q] IS NOT NULL AND S.[CM_SAL_Q]<>'' THEN S.[CM_SAL_Q] ELSE C.[CM_SAL_Q] END,
    -- ... all 16 columns in one UPDATE ...
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_ST_MAJ_CAT S
    ON C.[ST_CD] = S.[ST_CD] AND C.[MAJ_CAT] = S.[MAJ_CAT]
```

### Step 3 — Apply Defaults

**What:** Ensure critical columns have usable values.

**Why:** Growth rates of NULL or 0 would zero out MBQ. Default to 1.0 (no growth/no shrinkage).

**SQL (single UPDATE):**
```sql
UPDATE ARS_CALC_ST_MAJ_CAT SET
    [LISTING] = CASE WHEN [LISTING] IS NULL OR [LISTING]='' THEN 1
                     WHEN UPPER([LISTING])='Y' THEN 1
                     WHEN UPPER([LISTING])='N' THEN 0 ELSE [LISTING] END,
    [I_ROD] = CASE WHEN [I_ROD] IS NULL OR [I_ROD]=0 THEN 1 ELSE [I_ROD] END,
    [DISP_GR_DGR] = CASE WHEN [DISP_GR_DGR] IS NULL OR [DISP_GR_DGR]=0 THEN 1 ELSE [DISP_GR_DGR] END,
    [LW_ACT_SL_GR_DGR] = CASE WHEN ... THEN 1 ELSE ... END,
    [BGT_SL_GR_DGR] = CASE WHEN ... THEN 1 ELSE ... END
```

### Step 4 — Calculate ALC_D (Allocation Lifecycle Days)

**Formula:**
```
ALC_D = INT_DAYS + PRD_DAYS + SL_CVR
```

**What each component means:**
- **INT_DAYS** (Internal Days): buffer days for internal processing (picking, packing)
- **PRD_DAYS** (Period Days): transit time from RDC to store
- **SL_CVR** (Sale Cover): how many extra days of stock to maintain at the store

**SL_CVR priority (applied in order, each overwrites previous if > 0):**

| Priority | Source | When Used |
|----------|--------|-----------|
| Base | ST_MASTER.SL_CVR | Always applied first |
| Override | CO_MAJ_CAT.SL_CVR | If CO has a category-specific value > 0 |
| Final | ST_MAJ_CAT.SL_CVR | If store has its own override > 0 (highest priority) |

**SQL:**
```sql
-- Base: ST_MASTER (applies to all)
UPDATE C SET C.[SALE_COVER_SRC]='ST_MASTER',
    C.[ALC_D] = ISNULL(S.[INT_DAYS],0) + ISNULL(S.[PRD_DAYS],0) + ISNULL(S.[SL_CVR],0)
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_ST_MASTER S ON C.[ST_CD] = S.[ST_CD];

-- Priority 2: CO_MAJ_CAT override (if SL_CVR > 0)
UPDATE C SET C.[SALE_COVER_SRC]='CO_MAJ_CAT',
    C.[ALC_D] = ISNULL(S.[INT_DAYS],0) + ISNULL(S.[PRD_DAYS],0) + ISNULL(CO.[SL_CVR],0)
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_ST_MASTER S ON C.[ST_CD] = S.[ST_CD]
INNER JOIN Master_ALC_INPUT_CO_MAJ_CAT CO ON C.[MAJ_CAT] = CO.[MAJ_CAT]
WHERE CO.[SL_CVR] IS NOT NULL AND CO.[SL_CVR] > 0;

-- Priority 1: ST_MAJ_CAT own SL_CVR (highest)
UPDATE C SET C.[SALE_COVER_SRC]='ST_MAJ_CAT',
    C.[ALC_D] = ISNULL(S.[INT_DAYS],0) + ISNULL(S.[PRD_DAYS],0) + ISNULL(C.[SL_CVR],0)
FROM ARS_CALC_ST_MAJ_CAT C
INNER JOIN Master_ALC_INPUT_ST_MASTER S ON C.[ST_CD] = S.[ST_CD]
WHERE C.[SL_CVR] IS NOT NULL AND C.[SL_CVR] > 0;
```

**Worked Example — Store HB05:**
```
ST_MASTER: INT_DAYS=4, PRD_DAYS=3, SL_CVR=2    → ALC_D = 4+3+2 = 9, SRC=ST_MASTER
CO_MAJ_CAT for M_TEES_HS: SL_CVR=NULL           → no override
ST_MAJ_CAT for HB05/M_TEES_HS: SL_CVR=NULL      → no override
Final: ALC_D = 9 days, SALE_COVER_SRC = ST_MASTER
```

### Step 5 — Calculate SAL_PD (Per-Day Sale Rate)

**What:** Calculate how many units per day this store sells in this category.

**Why:** SAL_PD feeds the MBQ formula — it's the demand signal.

**The 5-branch formula:**
```
IF CM_REM_D = 0:
    SAL_PD = 0                              ← no data at all

ELSE IF CM_REM_D >= ALC_D:
    SAL_PD = CM_SAL_Q / CM_REM_D            ← current month has enough days

ELSE IF ALC_D = 0:
    SAL_PD = 0                              ← no target window

ELSE IF NM_REM_D = 0:
    SAL_PD = CM_SAL_Q / CM_REM_D            ← fallback (no prev month data)

ELSE:
    SAL_PD = (CM_SAL_Q + (NM_SAL_Q / NM_REM_D) × (ALC_D - CM_REM_D)) / ALC_D
                                             ← blend CM actuals + NM extrapolation
```

**Input columns:**

| Column | Meaning | Example |
|--------|---------|---------|
| CM_SAL_Q | Current month sale quantity | 5,400 units |
| CM_REM_D | Current month days with data | 25 days |
| NM_SAL_Q | Previous/next month sale quantity | 10,712 units |
| NM_REM_D | Previous/next month days | 31 days |
| ALC_D | Target window (from Step 4) | 9 days |

**Worked Example — HB05 / M_TEES_HS (Branch 1):**
```
CM_SAL_Q=5400, CM_REM_D=25, ALC_D=9

Check: CM_REM_D (25) >= ALC_D (9)? YES → Branch 1

SAL_PD = CM_SAL_Q / CM_REM_D = 5400 / 25 = 216 units/day

Interpretation: We have 25 days of current-month data, and our target window is only 9 days.
The CM data alone is sufficient — no need to blend with previous month.
```

**Worked Example — Hypothetical (Branch 2, ALC_D=37):**
```
CM_SAL_Q=5400, CM_REM_D=25, NM_SAL_Q=10712, NM_REM_D=31, ALC_D=37

Check: CM_REM_D (25) < ALC_D (37)? YES → need to blend

NM daily rate = 10712 / 31 = 345.54 units/day
Missing days = ALC_D - CM_REM_D = 37 - 25 = 12 days

SAL_PD = (5400 + 345.54 × 12) / 37
       = (5400 + 4146.49) / 37
       = 9546.49 / 37 = 258.01 units/day

Interpretation: CM covers 25 of the 37 target days. We extrapolate the remaining 12 days
using the prev-month daily rate, then average across the full 37-day window.
```

**SQL:**
```sql
UPDATE ARS_CALC_ST_MAJ_CAT SET [SAL_PD] =
    CASE
        WHEN ISNULL([CM_REM_D],0) = 0 THEN 0
        WHEN [CM_REM_D] >= ISNULL([ALC_D],0) THEN
            CAST([CM_SAL_Q] AS FLOAT) / [CM_REM_D]
        WHEN ISNULL([ALC_D],0) = 0 THEN 0
        WHEN ISNULL([NM_REM_D],0) = 0 THEN
            CAST([CM_SAL_Q] AS FLOAT) / [CM_REM_D]
        ELSE
            (CAST([CM_SAL_Q] AS FLOAT)
             + (CAST([NM_SAL_Q] AS FLOAT) / [NM_REM_D])
               * ([ALC_D] - [CM_REM_D])
            ) / [ALC_D]
    END
```

### Step M — MASTER_GEN_ART_SALE SAL_PD (Full Option Coverage)

**What:** Apply the same SAL_PD formula directly on MASTER_GEN_ART_SALE (the planned-sales table with ~2.17M rows).

**Why:** ARS_CALC_ST_ART only covers ~176K options. MASTER_GEN_ART_SALE covers the full universe (~21 lakh rows). The listing needs per-option daily-sale rates for OPT_MBQ calculation.

**Inputs:**
- CM_SAL_Q, NM_SAL_Q → from MASTER_GEN_ART_SALE itself (each row has planned sales)
- CM_REM_D, NM_REM_D, ALC_D → borrowed from ARS_CALC_ST_MAJ_CAT (joined on ST_CD + MAJ_CAT)

**SQL:**
```sql
UPDATE S SET S.[SAL_PD] = [same CASE formula as Step 5]
FROM MASTER_GEN_ART_SALE S
INNER JOIN ARS_CALC_ST_MAJ_CAT MJ
    ON S.[ST_CD] = MJ.[ST_CD] AND S.[MAJ_CAT] = MJ.[MAJ_CAT]
```

**Performance:** Added index `IX_MASTER_GEN_ART_SALE_ST_MAJ` on (ST_CD, MAJ_CAT) to speed up the 2.17M-row UPDATE.

---

# 3. GRID BUILDER

## 3.1 What Is a Grid?

A "grid" is a **pivoted stock table** at a specific hierarchy level. Each grid answers: *"For each store × category × [hierarchy dimension], how much stock is there per SLOC, and how much should there be (MBQ)?"*

**Example grids in ARS:**

| Grid | Hierarchy | Output Table | Rows |
|------|-----------|-------------|------|
| MJ | WERKS, MAJ_CAT | ARS_GRID_MJ | 123K |
| MJ_RNG_SEG | WERKS, MAJ_CAT, RNG_SEG | ARS_GRID_MJ_RNG_SEG | 282K |
| MJ_CLR | WERKS, MAJ_CAT, CLR | ARS_GRID_MJ_CLR | 1.39M |
| MJ_GEN_ART | WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR | ARS_GRID_MJ_GEN_ART | 4.48M |

The finer the hierarchy, the more rows — but each row gives a more specific stock picture.

## 3.2 Grid Definition Table (ARS_GRID_BUILDER)

| Field | Type | Purpose | Example |
|-------|------|---------|---------|
| grid_name | NVARCHAR | Short identifier | MJ_RNG_SEG |
| hierarchy_columns | JSON array | Grouping dimensions | ["WERKS","MAJ_CAT","RNG_SEG"] |
| kpi_filter | NVARCHAR | Only use SLOCs with this KPI | STK |
| output_table | NVARCHAR | Result table name | ARS_GRID_MJ_RNG_SEG |
| status | NVARCHAR | Active or Inactive | Active |
| pivot_only | BIT | If 1: skip MBQ/OPT_CNT calc | 0 |
| use_for_opt_sale | BIT | If 1: feeds PER_OPT_SALE | 0 |
| grid_group | NVARCHAR | Primary or Secondary | Primary |
| weightage | FLOAT | Priority weight | 1.0 |
| seq | INT | Execution order | 2 |

## 3.3 The 7 Steps — Explained with Code

### Step 1 — Get Active SLOCs

**What:** Query which storage locations are active and what KPI they're tagged with.

**SQL:**
```sql
SELECT DISTINCT STK.SLOC, S.KPI
FROM ET_STORE_STOCK STK WITH (NOLOCK)
INNER JOIN ARS_STORE_SLOC_SETTINGS S WITH (NOLOCK) ON STK.SLOC = S.SLOC
WHERE UPPER(S.STATUS) = 'ACTIVE'
  AND UPPER(S.KPI) = 'STK'    -- if kpi_filter='STK' on this grid
ORDER BY STK.SLOC
```

**Result:** List of SLOC codes like `[0001, 0002, 0004, 0005, 0006, 0099, DH24_PRD_QTY, DH24_STO_QTY_Q]`

### Step 2 — Pivot Stock Data

**What:** Transform ET_STORE_STOCK (28M+ rows) from long format into a wide pivot with one column per SLOC.

**SQL (simplified):**
```sql
SELECT [WERKS], [MAJ_CAT], [RNG_SEG],
    ISNULL([0001],0) AS [0001],
    ISNULL([0002],0) AS [0002],
    ISNULL([0004],0) AS [0004],
    -- ... one per active SLOC ...
    ISNULL([0001],0) + ISNULL([0002],0) + ISNULL([0004],0) + ... AS STK_TTL
FROM (
    SELECT STK.[WERKS],
           ISNULL(LTRIM(RTRIM(MP.[MAJ_CAT])), 'NA') AS [MAJ_CAT],
           ISNULL(LTRIM(RTRIM(MP.[RNG_SEG])), 'NA') AS [RNG_SEG],
           STK.SLOC,
           STK.PARTICULARS_VALUE
    FROM ET_STORE_STOCK STK WITH (NOLOCK)
    LEFT JOIN vw_master_product MP ON STK.MATNR = MP.ARTICLE_NUMBER
    INNER JOIN ARS_STORE_SLOC_SETTINGS S ON STK.SLOC = S.SLOC
    WHERE UPPER(S.STATUS) = 'ACTIVE'
      AND STK.WERKS IS NOT NULL AND STK.WERKS <> ''
) src
PIVOT (SUM(PARTICULARS_VALUE) FOR SLOC IN ([0001], [0002], [0004], ...)) pvt
```

**STK_TTL** = sum of ONLY the STK-type SLOCs (sale-type SLOCs like L-7 DAYS SALE-Q are excluded).

### Step 3 — Post-Pivot Lookups

Three lookups in sequence:

**Lookup 1 — LISTING Filter:**
```sql
-- Join Master_ALC_INPUT_ST_MASTER
-- DELETE rows where store's LISTING != 1 (unlisted stores removed)
DELETE FROM [output_table]
WHERE [WERKS] NOT IN (
    SELECT ST_CD FROM Master_ALC_INPUT_ST_MASTER WHERE LISTING = 1
)
```

**Lookup 2 — Calculation Data:**
```sql
UPDATE O SET
    O.[DISP_Q] = C.[DISP_Q], O.[ACS_D] = C.[ACS_D], O.[ALC_D] = C.[ALC_D],
    O.[SAL_PD] = C.[SAL_PD], O.[DISP_GR_DGR] = C.[DISP_GR_DGR],
    O.[BGT_SL_GR_DGR] = C.[BGT_SL_GR_DGR], O.[CONT] = C.[CONT]
FROM [output_table] O
INNER JOIN ARS_CALC_ST_MAJ_CAT C
    ON O.[WERKS] = C.[ST_CD] AND O.[MAJ_CAT] = C.[MAJ_CAT]
```

**Lookup 3 — Dynamic CONT (contribution %):**
```sql
-- Table name derived from last hierarchy column
-- Grid: MJ_RNG_SEG → last column = RNG_SEG → table = Master_CONT_RNG_SEG
UPDATE O SET O.[CONT] = L.[CONT]
FROM [output_table] O
INNER JOIN [Master_CONT_RNG_SEG] L
    ON O.[WERKS] = L.[ST_CD]
   AND O.[MAJ_CAT] = L.[MAJ_CAT]
   AND O.[RNG_SEG] = L.[RNG_SEG]

-- Fallback if CONT still NULL: try CO level
UPDATE O SET O.[CONT] = L.[CONT]
FROM [output_table] O
INNER JOIN [Master_CONT_RNG_SEG] L
    ON L.[ST_CD] = 'CO'
   AND O.[MAJ_CAT] = L.[MAJ_CAT]
   AND O.[RNG_SEG] = L.[RNG_SEG]
WHERE O.[CONT] IS NULL

-- Final fallback: even distribution = 1/COUNT
UPDATE O SET O.[CONT] = 1.0 / G.cnt
FROM [output_table] O
INNER JOIN (SELECT WERKS, MAJ_CAT, COUNT(*) AS cnt
            FROM [output_table] GROUP BY WERKS, MAJ_CAT) G
    ON O.WERKS = G.WERKS AND O.MAJ_CAT = G.MAJ_CAT
WHERE O.[CONT] IS NULL
```

### Step 4 — Calculate MBQ (Minimum Base Quantity)

**What MBQ means:** *"How many units should this hierarchy combination have in stock?"*

**Stage 1 — Raw MBQ (before contribution):**
```
MBQ_RAW = (SAL_PD × BGT_SL_GR_DGR) × ALC_D + (DISP_Q × DISP_GR_DGR)
```

| Component | Meaning | Typical Value |
|-----------|---------|--------------|
| SAL_PD × BGT_SL_GR_DGR | Daily sales adjusted for growth/decline | 216 × 1.0 = 216 |
| × ALC_D | ... for N days forward | × 9 = 1,944 |
| DISP_Q × DISP_GR_DGR | Display capacity adjusted for growth | 50 × 1.0 = 50 |
| **MBQ_RAW** | Total target stock before contribution | **1,994** |

**Stage 2 — Apply Contribution:**
```
MBQ = ROUND(MBQ_RAW × CONT, 0)
```

**Why CONT?** A category might have multiple sub-segments (RNG_SEG = E, P, V). If segment E is 50% of the category, its MBQ should be 50% of the total → CONT = 0.5.

**Worked Example:**
```
SAL_PD=216, BGT_SL_GR_DGR=1.0, ALC_D=9 → sales component = 216 × 1.0 × 9 = 1,944
DISP_Q=50, DISP_GR_DGR=1.0              → display component = 50 × 1.0 = 50
MBQ_RAW = 1,994
CONT = 0.5

MBQ = ROUND(1994 × 0.5, 0) = 997 units
```

**SQL:**
```sql
-- Stage 1: Raw MBQ
UPDATE [output_table] SET [MBQ] =
    (ISNULL(TRY_CAST([SAL_PD] AS FLOAT), 0)
     * CASE WHEN ISNULL(TRY_CAST([BGT_SL_GR_DGR] AS FLOAT), 0) = 0 THEN 1
            ELSE TRY_CAST([BGT_SL_GR_DGR] AS FLOAT) END)
    * ISNULL(TRY_CAST([ALC_D] AS FLOAT), 0)
    + (ISNULL(TRY_CAST([DISP_Q] AS FLOAT), 0)
       * CASE WHEN ISNULL(TRY_CAST([DISP_GR_DGR] AS FLOAT), 0) = 0 THEN 1
              ELSE TRY_CAST([DISP_GR_DGR] AS FLOAT) END);

-- Stage 2: Apply CONT
UPDATE [output_table] SET [MBQ] =
    CASE WHEN ISNULL(TRY_CAST([CONT] AS FLOAT), 0) = 0 THEN 0
         ELSE ROUND([MBQ] * TRY_CAST([CONT] AS FLOAT), 0) END;
```

### Step 5 — Calculate OPT_CNT (Option Count)

**Formula:**
```
OPT_CNT = ROUND(DISP_Q × DISP_GR_DGR × CONT / ACS_D, 0)
```

**Business meaning:** *"How many distinct options (color-sizes) should this hierarchy display?"*

**Example:** `ROUND(50 × 1.0 × 0.5 / 7, 0) = ROUND(3.57, 0) = 4 options`

### Step 6 — Multiply DISP_Q by CONT

```
DISP_Q = ROUND(DISP_Q × CONT, 0)
```

**Critical:** This runs AFTER Steps 4-5 because those use the raw DISP_Q. After this step, DISP_Q reflects the contribution-adjusted display capacity.

### Step 7 — Create Primary Key

Fill NULLs, deduplicate (keep highest STK_TTL), create PK constraint on hierarchy columns.

---

# 4. LISTING GENERATION

## 4.1 What Is the Listing?

The Listing (`ARS_LISTING`) is the **master output table** that merges grid stock data with MSA recommendations. It's the final data preparation output — what planners use to decide which articles to ship to which stores.

**Key outputs per row:**
- **STK_TTL** — current stock at the store
- **OPT_MBQ** — target stock level
- **OPT_REQ** — how much to ship = MAX(0, target - current)
- **OPT_TYPE** — classification (RL/TBL/TBC/MIX)
- **IS_NEW** — 1 if MSA recommends it but store doesn't stock it yet

## 4.2 Complete Pipeline (Parts 1-8)

### Part 1 — Grid Data INSERT (IS_NEW=0)

**What:** Load all existing store × article combinations from the grid table.

**Key details:**
- IS_NEW = 0 (these options already exist in the store's inventory)
- STK_TTL = sum of stock-type SLOC columns (sale columns like L-7 DAYS SALE-Q are excluded from the sum but carried as separate columns)
- Filtered by: active stores (LISTING=1), selected stores/MAJ_CATs

**SQL:**
```sql
INSERT INTO ARS_LISTING (WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR,
    [0001], [0002], ..., [L-7 DAYS SALE-Q], STK_TTL, IS_NEW, OPT_TYPE)
SELECT G.[WERKS], S.[RDC], G.[MAJ_CAT], TRY_CAST(G.[GEN_ART_NUMBER] AS BIGINT), G.[CLR],
    ISNULL(G.[0001],0), ISNULL(G.[0002],0), ..., ISNULL(G.[L-7 DAYS SALE-Q],0),
    ISNULL(G.[0001],0) + ISNULL(G.[0002],0) + ... AS STK_TTL,   -- stock SLOCs only
    0 AS IS_NEW, NULL AS OPT_TYPE
FROM [ARS_GRID_MJ_GEN_ART] G
INNER JOIN (stores) S ON G.[WERKS] = S.[ST_CD]
```

### Part 2 — MSA Missing Options INSERT (IS_NEW=1)

**What:** Add MSA-recommended articles that are NOT already in the listing.

**Business meaning:** The store doesn't currently stock this article, but the MSA system says the RDC has enough stock and recommends listing it.

**Key details:**
- IS_NEW = 1
- All stock columns = 0 (store has zero stock of this option)
- STK_TTL = 0
- NOT EXISTS check ensures no duplicates with grid data

**RDC modes:**

| Mode | Behavior | When to Use |
|------|----------|-------------|
| All | Every MSA option for every store | Broadest — testing/analysis |
| Own | Each store gets only its own RDC's options | **Most common** — production use |
| Cross | FROM source RDC(s), TO target RDC stores | Cross-docking scenarios |

### Part 3.5 — Populate Base Attributes

**What:** Enrich each listing row with demand/supply parameters from calc tables.

| Column | Source | Join Key | Business Meaning |
|--------|--------|----------|-----------------|
| ACS_D | ARS_CALC_ST_MAJ_CAT (or DPN fallback) | WERKS+MAJ_CAT | Floor stock / display-per-norm |
| ALC_D | ARS_CALC_ST_MAJ_CAT (or SAL_D fallback) | WERKS+MAJ_CAT | Sale-cover window in days |
| LISTING | ARS_CALC_ST_MAJ_CAT → ARS_CALC_ST_ART | Cascade | Is this option listed? (1=yes) |
| I_ROD | Same cascade | Same | I_ROD allocation rounds |
| CLR_MIN, CLR_MAX | ARS_CALC_ST_MAJ_CAT | WERKS+MAJ_CAT | Min/max colors for this category |
| FOCUS_W_CAP, FOCUS_WO_CAP | ARS_CALC_ST_ART | +GEN_ART+CLR | Focus article flags |
| AUTO_GEN_ART_SALE | MASTER_GEN_ART_SALE.SAL_PD | +GEN_ART+CLR | Planned per-day-sale at option level |
| AGE | MASTER_GEN_ART_AGE | ST_CD+MAJ_CAT+GEN_ART+CLR | Option age in days |

### Part 3.55 — MSA_FNL_Q + VAR_COUNT

**What:** Pre-populate MSA availability for OPT_TYPE classification.

| Column | Source | Formula |
|--------|--------|---------|
| MSA_FNL_Q | ARS_MSA_GEN_ART | SUM(FNL_Q) per MAJ_CAT+GEN_ART+CLR |
| VAR_COUNT | ARS_MSA_VAR_ART | COUNT of variant rows |
| VAR_FNL_COUNT | ARS_MSA_VAR_ART | COUNT WHERE FNL_Q > 0 |

### Part 3.6 — OPT_TYPE Classification

**What:** Tag each option with its supply status. This determines allocation priority and handling.

**Priority order (first match wins):**

```sql
SET [OPT_TYPE] = CASE
    -- 1. MIX(a): low stock + no MSA backup
    WHEN ACS_D > 0 AND STK_TTL < 0.6 × ACS_D AND MSA_FNL_Q = 0  THEN 'MIX'

    -- 2. MIX(b): poor color fill (existing rows only)
    WHEN IS_NEW = 0 AND VAR_COUNT > 0
     AND VAR_FNL_COUNT / VAR_COUNT < 0.6                          THEN 'MIX'

    -- 3. RL: adequate stock
    WHEN ACS_D > 0 AND STK_TTL >= 0.6 × ACS_D                    THEN 'RL'

    -- 4. TBC: low stock but MSA available
    WHEN ACS_D > 0 AND STK_TTL > 0 AND STK_TTL < 0.6 × ACS_D
     AND MSA_FNL_Q > 0                                            THEN 'TBC'

    -- 5. TBL: zero stock + MSA available
    WHEN STK_TTL <= 0 AND MSA_FNL_Q > 0                           THEN 'TBL'

    -- FALLBACK (when ACS_D is NULL/0):
    WHEN MSA_FNL_Q = 0 AND STK_TTL = 0   THEN 'MIX'
    WHEN MSA_FNL_Q = 0 AND STK_TTL > 0   THEN 'RL'
    WHEN MSA_FNL_Q > 0 AND STK_TTL > 0   THEN 'TBC'
    WHEN MSA_FNL_Q > 0 AND STK_TTL <= 0  THEN 'TBL'
END
```

**Example classification for 5 options:**

| WERKS | GEN_ART | STK_TTL | ACS_D | 60%×ACS_D | MSA_FNL_Q | VAR ratio | OPT_TYPE | Why |
|-------|---------|---------|-------|-----------|-----------|-----------|----------|-----|
| HB05 | 1116113204 | 19 | 22 | 13.2 | 198 | 5/6=83% | **RL** | 19 >= 13.2 |
| HB05 | 1116112688 | 4 | 22 | 13.2 | 3053 | 6/6=100% | **TBC** | 4 < 13.2, MSA>0 |
| HB05 | 1116113731 | 0 | 22 | 13.2 | 7495 | 6/6=100% | **TBL** | STK=0, MSA>0 |
| HB05 | 1116112950 | 3 | 22 | 13.2 | 0 | — | **MIX(a)** | STK < 13.2, MSA=0 |
| HB05 | 1116112842 | 25 | 22 | 13.2 | 100 | 1/6=17% | **MIX(b)** | VAR ratio < 60% |

### Part 3.7 — MIX Aggregation

**What:** Collapse all MIX-tagged rows into fewer lines.

**Why:** MIX options are not individually actionable (no MSA backup). Keeping hundreds of MIX rows per store clutters the listing. Aggregation summarizes them into 1 line.

| mix_mode | Result |
|----------|--------|
| st_maj_rng (default) | 1 MIX line per (WERKS, MAJ_CAT, RNG_SEG) |
| st_maj | 1 MIX line per (WERKS, MAJ_CAT) |
| each | No aggregation — keep every MIX row |

### Part 4a — Grid Column Joins

For each active non-pivot grid, add prefixed columns to the listing:

**Example:** Grid MJ_RNG_SEG adds:
- `RNG_SEG_STK_TTL` — grid-level stock total
- `RNG_SEG_CONT` — contribution at this hierarchy
- `RNG_SEG_MBQ` — grid-level MBQ target
- `RNG_SEG_OPT_CNT` — grid-level option count
- `RNG_SEG_DISP_Q` — grid-level display quantity

### Part 4b — PER_OPT_SALE

**Formula:**
```
PER_OPT_SALE = ((MBQ - DISP_Q) / DISP_Q × ACS_D) / ALC_D
```

**Business meaning:** Estimated daily sale rate per option, derived from how much the MBQ exceeds display capacity.

### Part 4c — OPT_MBQ (The Target Stock Level)

**Rate selection (age-based):**
```
IF AGE < 15 (or IS_NEW=1):
    rate = MAX(PER_OPT_SALE, L-7 DAYS SALE/7, AUTO_GEN_ART_SALE)
ELSE:
    rate = MAX(L-7 DAYS SALE/7, AUTO_GEN_ART_SALE)
```

**Why the age check?** New articles (< 15 days old) have unreliable L-7 history. Adding PER_OPT_SALE as a third candidate ensures they get a reasonable target even with zero recent sales.

**ACS_D override:** If MANUAL_DENSITY > 0 at article level, it overrides ACS_D for that option before the formula runs.

**Formulas:**
```
OPT_MBQ    = ACS_D + rate × ALC_D
OPT_REQ    = MAX(0, OPT_MBQ - STK_TTL)
OPT_MBQ_WH = ACS_D + rate × (ALC_D + hold_days)   [IS_NEW=1 only]
OPT_REQ_WH = MAX(0, OPT_MBQ_WH - STK_TTL)
MAX_DAILY_SALE = MAX(L-7/7, AUTO_GEN_ART_SALE)
```

**Worked Example — Established article (HB05, AGE=40):**
```
ACS_D=24, L-7 DAYS SALE-Q=320 → L-7/7=45.71, AUTO=12.0, ALC_D=9, STK_TTL=2167

AGE=40 >= 15 → rate = MAX(45.71, 12.0) = 45.71
OPT_MBQ = 24 + 45.71 × 9 = 24 + 411.43 = 435
OPT_REQ = MAX(0, 435 - 2167) = 0   ← stock exceeds target, no shipment needed
```

**Worked Example — New article (AGE=5, IS_NEW=1):**
```
ACS_D=24, L-7=0 (no history), AUTO=0, PER_OPT_SALE=0.82, ALC_D=9, hold_days=15

AGE=5 < 15 → rate = MAX(0.82, 0, 0) = 0.82
OPT_MBQ    = 24 + 0.82 × 9 = 31
OPT_MBQ_WH = 24 + 0.82 × (9 + 15) = 24 + 19.68 = 44   ← extra buffer for new article
OPT_REQ_WH = MAX(0, 44 - 0) = 44   ← ship 44 units
```

### Part 4d — ART_EXCESS

```
ART_EXCESS = MAX(0, STK_TTL - 2 × OPT_MBQ)
```

**Business meaning:** If a store has more than 2× the target stock, the excess can be redistributed. MIX rows always = 0.

### Part 4e — Per-Grid REQ with Excess Deduction

For each grid, aggregate ART_EXCESS by that grid's hierarchy, then recalculate REQ:
```
{prefix}_REQ = MAX(0, {prefix}_MBQ - ({prefix}_STK_TTL - aggregated_excess))
```

### Part 6 — Store Ranking

**What:** Rank stores within each MAJ_CAT by demand + fill rate for allocation priority.

```
FILL_RATE = MJ_STK_TTL / MJ_MBQ   (how well stocked the store is)
REQ_RANK  = ROW_NUMBER() ORDER BY MJ_REQ ASC    (lower demand = higher rank)
FILL_RANK = ROW_NUMBER() ORDER BY FILL_RATE DESC (better filled = higher rank)
W_SCORE   = REQ_RANK × 0.4 + FILL_RANK × 0.6
ST_RANK   = ROW_NUMBER() ORDER BY W_SCORE DESC  (highest score = highest priority)
```

**Interpretation:** Stores with high demand AND poor fill rate get the highest ST_RANK — they're allocated first.

### Part 7 — Working Table + Hierarchy + ALLOC_FLAG

**Filter:** ARS_LISTING → ARS_LISTING_WORKING where:
- MSA_FNL_Q > 0 (warehouse has stock)
- OPT_REQ_WH >= 1 (store needs at least 1 unit)
- VAR_FNL_COUNT/VAR_COUNT >= 60% (color availability adequate)
- LISTING = 1 (option is listed)

**Hierarchy columns:** GH_{name} (grid presence 0/1), H_{name} (REQ>0 × hierarchy)
**PRI_CT%** = SUM(primary H_) / SUM(primary GH_) × 100
**ALLOC_FLAG** = 1 if PRI_CT% >= 100 (all primary grids have demand)

### Part 8 — Multi-Level Allocation (The Core Allocation Engine)

This is the most critical part of the entire system. It decides **exactly how many units of each size to ship to each store** — respecting warehouse limits, store priority, and size availability.

**Think of it like this:** You have a warehouse (MSA) with limited stock. 346 stores are asking for stock. You can't give everyone what they want. So you prioritize: who gets stock first? How much? What if a size runs out?

#### The Waterfall Concept

Allocation flows like a waterfall in 3 priority waves:

```
WAVE 1: RL (Regular Listed) — stores that already stock the article
  → They sell it, they know it, replenish them first
  → Priority: ST_RANK (best stores first)

WAVE 2: TBC (To Be Check) — stores with low stock + MSA available
  → Check-worthy, allocate if stock allows
  → Priority: ST_RANK

WAVE 3: TBL (To Be Listed) — stores with zero stock (new listing)
  → New introduction. Validate size coverage first.
  → Priority: ST_RANK + size break validation
```

Each wave runs I_ROD rounds. Round 1 = 1x target. Round 2 = 2x target. And so on.

#### Step 8.1 — Create ARS_ALLOC_WORKING (Expand to Size Level)

The working table has 1 row per option (MAJ_CAT × GEN_ART × CLR). Allocation happens at **size level**. So we expand each option into all available sizes from ARS_MSA_VAR_ART.

**Example:** 1 working row → 3 alloc rows

| Working (option) | → | Alloc (per size) |
|---|---|---|
| HB05/M_TEES/1116113204/BLK/OPT_MBQ=435 | → | HB05/BLK/**S**/FNL_Q=200 |
| | → | HB05/BLK/**M**/FNL_Q=350 |
| | → | HB05/BLK/**L**/FNL_Q=150 |

#### Step 8.2 — Enrich: STK_TTL + CONT + SZ_MBQ + SZ_REQ

**CONT (Size Contribution):** 3-level fallback (store → company → auto = 1/count).

**SZ_MBQ and SZ_REQ:**
```
SZ_MBQ = OPT_MBQ × CONT       (target for THIS SIZE)
SZ_REQ = MAX(0, SZ_MBQ - STK_TTL)  (how many to ship)
```

**Example:** OPT_MBQ=435, CONT_M=0.40, STK_M=50 → SZ_MBQ=174, SZ_REQ=124

#### Step 8.3 — Tracking Columns + Step 8.4 — Pool Tracker

**Pool** tracks remaining warehouse stock per variant-size. As stores get stock, pool depletes.

#### Step 8.5 — Eligibility Checks (E1-E5)

| Check | Rule | If Failed |
|-------|------|-----------|
| E3 | OPT_TYPE != MIX | INELIGIBLE |
| E1 | LISTING = 1 | INELIGIBLE |
| E2 | ALLOC_FLAG = 1 (PRI_CT% >= 100%) | INELIGIBLE |
| E4 | MSA_FNL_Q > 0 | INELIGIBLE |
| E5 | OPT_REQ_WH >= 1 (or FOCUS_WO_CAP = 1) | INELIGIBLE |

#### Step 8.6 — The Allocation Loop

FOR EACH OPT_TYPE IN [RL, TBC, TBL]:
  FOR EACH I_ROD ROUND:

**A. Scale demand:** Round N → SZ_MBQ = OPT_MBQ × N × CONT

**B. Waterfall allocate (1 SQL for ALL stores + ALL sizes):**
```
Priority: FOCUS_WO_CAP first → FOCUS_W_CAP → then ST_RANK

For each pool (RDC × GEN_ART × CLR × SZ):
  Store rank 1: wants 124 → pool=350 → gets 124. Pool=226
  Store rank 2: wants 132 → pool=226 → gets 132. Pool=94
  Store rank 3: wants 130 → pool=94  → gets 94 (partial). Pool=0
  Store rank 4: wants 120 → pool=0   → gets 0. Pool=0
```

**C. Deduct from pool**

**D. Size break validation (TBL only):**
```
If available_sizes / total_sizes < 60% at rank N:
  → BREAK at rank N
  → Stores rank N+ get allocation REVERSED
  → Pool restored for those stores
  → SKIP_FLAG = 1
```

**E. Commit:** ALLOC_QTY += ROUND_ALLOC

**F. Post-sync:** Update MSA_FNL_Q, OPT_REQ_WH, VAR_FNL_COUNT

#### Step 8.7 — Worked Example (4 stores, size M, pool=350)

| Store | ST_RANK | SZ_MBQ | STK | SZ_REQ | Pool Before | Gets | Pool After | Status |
|-------|---------|--------|-----|--------|-------------|------|------------|--------|
| HB05 | 1 | 174 | 50 | 124 | 350 | 124 | 226 | ALLOCATED |
| HD22 | 2 | 152 | 20 | 132 | 226 | 132 | 94 | ALLOCATED |
| HG11 | 3 | 140 | 10 | 130 | 94 | 94 | 0 | PARTIAL |
| HW18 | 4 | 120 | 0 | 120 | 0 | 0 | 0 | — (pool empty) |

#### Step 8.8 — Fallback (Optional)

If enable_fallback=True: demote grid → recalc PRI_CT% → boost OPT_MBQ (130%) → re-run.

#### Part 8 Flowchart

```
START
  │
  ▼
Create ARS_ALLOC_WORKING (expand → variant × size)
  │
  ▼
Enrich: STK_TTL, CONT (ST→CO→auto), SZ_MBQ, SZ_REQ
  │
  ▼
Create Pool (#alloc_pool — FNL_Q_REM per variant-size)
  │
  ▼
Mark Eligibility (E1-E5 → PENDING or INELIGIBLE)
  │
  ▼
┌──────────────────────────────────────────────────────┐
│  FOR EACH OPT_TYPE IN [RL, TBC, TBL]:               │
│    FOR EACH I_ROD ROUND (1..max):                    │
│      A. Scale demand (×N)                            │
│      B. Waterfall allocate (by ST_RANK)              │
│      C. Deduct pool                                  │
│      D. Size break validation (TBL only)             │
│         → BREAK? Restore pool, SKIP stores           │
│      E. Commit ALLOC_QTY                             │
│      F. Post-sync (MSA_FNL_Q, OPT_REQ_WH)           │
│    END ROUND                                         │
│  END OPT_TYPE                                        │
└──────────────────────────────────────────────────────┘
  │
  ▼
[Optional] Fallback (demote grid → boost → re-allocate)
  │
  ▼
Reflect ALLOC_QTY → ARS_LISTING_WORKING
Set FINAL_OPT_TYPE (ALLOCATED / PARTIAL / SKIP)
  │
  ▼
DONE → alloc_rows, skipped, ineligible, duration
```

## 4.3 Configurable Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| stock_threshold_pct | 0.6 | OPT_TYPE boundary (60%) and VAR fill threshold |
| excess_multiplier | 2.0 | Excess = STK > X × OPT_MBQ |
| hold_days | 0 | Extra cover days for IS_NEW=1 |
| age_threshold | 15 | AGE < X → use PER_OPT_SALE |
| req_weight | 0.4 | Store ranking: demand weight |
| fill_weight | 0.6 | Store ranking: fill rate weight |
| mix_mode | st_maj_rng | MIX aggregation level |
| enable_fallback | false | Grid demotion + boost |
| fallback_boost_mode | static | "str" or "static" boost |
| static_growth_pct | 130.0 | Static boost multiplier |
| str_tiers | 30:150,45:130,60:120,90:110 | STR boost tiers |

## 4.4 Step Timings (All 18 Steps Logged)

| Step | What | Typical Time |
|------|------|-------------|
| Part 1 | Grid data INSERT | 2-5s |
| Part 2 | MSA missing INSERT | 3-8s |
| Part 2.5 | Indexes | 1-3s |
| Part 3.5a | LISTING/I_ROD/CLR/FOCUS | 2-4s |
| Part 3.5 | ACS_D/ALC_D/AUTO/AGE | 3-6s |
| Part 3.55 | MSA_FNL_Q + VAR_COUNT | 2-5s |
| Part 3.6 | OPT_TYPE classification | 1-2s |
| Part 3.7 | MIX aggregation | 1-3s |
| Part 4 pre | MP → listing columns | 3-8s |
| Part 4a | Grid column joins | 10-30s (per grid ~2-5s) |
| Part 4b | PER_OPT_SALE | 1-2s |
| Part 4c | OPT_MBQ + OPT_REQ | 2-5s |
| Part 4d | ART_EXCESS | 1-2s |
| Part 4e | Per-grid REQ | 2-5s |
| Part 5 | Final indexes | 1-2s |
| Part 6 | Store Ranking | 2-5s |
| Part 7 | Working + Hierarchy | 5-15s |
| Part 8 | Allocation | 10-30s |

## 4.5 SOP — Running Listing

### Before You Run
- [ ] Data Checklist → all master tables current
- [ ] MSA Stock Calculation → completed
- [ ] Grid Builder → all active grids built
- [ ] MASTER_GEN_ART_AGE → populated

### Steps
1. Open **Data Preparation → Listing**
2. Select **Run Mode:** Listing Only (fast) or Full Pipeline (MSA+Grid+Listing)
3. Select **RDC Mode:** Own (most common)
4. Filter Stores/MAJ_CATs if needed
5. Set **MIX Rows** mode and **Variables**
6. Click **Generate**
7. Review **Summary:** MIX/TBL/TBC/RL counts + ALLOC QTY by RDC and OPT_TYPE
8. Toggle **Working** / **Full Listing** / **Alloc** to inspect
9. **Export** for downstream use

---

# FORMULA QUICK REFERENCE

| Formula | Expression | Where |
|---------|-----------|-------|
| FNL_Q | MAX(STK_QTY - PEND_QTY, 0) | MSA Step 7 |
| ALC_D | INT_DAYS + PRD_DAYS + SL_CVR | Calc Step 4 |
| SAL_PD | CM_SAL_Q/CM_REM_D or (CM+NM blend)/ALC_D | Calc Step 5 |
| MBQ | ((SAL_PD × BGT_GR) × ALC_D + DISP_Q × DISP_GR) × CONT | Grid Step 4 |
| OPT_CNT | ROUND(DISP_Q × DISP_GR × CONT / ACS_D, 0) | Grid Step 5 |
| DISP_Q (final) | ROUND(DISP_Q × CONT, 0) | Grid Step 6 |
| PER_OPT_SALE | ((MBQ - DISP_Q) / DISP_Q × ACS_D) / ALC_D | Listing 4b |
| OPT_MBQ | ACS_D + MAX(rates) × ALC_D | Listing 4c |
| OPT_REQ | MAX(0, OPT_MBQ - STK_TTL) | Listing 4c |
| ART_EXCESS | MAX(0, STK - excess_mult × OPT_MBQ) | Listing 4d |
| W_SCORE | REQ_RANK × 0.4 + FILL_RANK × 0.6 | Listing Part 6 |
| PRI_CT% | SUM(primary H_) / SUM(primary GH_) × 100 | Listing Part 7 |

---

*ARS v2.0 Complete Process & SOP — Updated 19 April 2026*
*Column names: ACS_D (was DPN), ALC_D (was SAL_D), SAL_PD unchanged*
