# ARS v2.0 — Process Workflow & Formulas

## Document Purpose
This document explains the three core data preparation processes in ARS: **MSA Stock Calculation**, **Grid Builder**, and **Listing Generation** — including all formulas, steps, input/output tables, and business rules.

---

# 1. MSA STOCK CALCULATION

## What It Does
Calculates **available stock per store/article** by combining warehouse stock data with pending allocations, then determines which articles have enough stock to recommend.

## Input Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| VW_ET_MSA_STK_WITH_MASTER | Stock + master product view | ST_CD, SLOC, STK_Q, ARTICLE_NUMBER, MAJ_CAT, GEN_ART_NUMBER, CLR, SEG |
| MASTER_ALC_PEND | Pending allocations | RDC, ARTICLE_NUMBER, MOA, QTY |

## 9-Step Algorithm

### Step 1 — Filter by SLOC
Filter stock data to only the selected SLOCs (storage locations). If no SLOCs specified, use all.

### Step 2 — Numeric Safety
Convert STK_Q (stock quantity) to numeric. Fill any missing/NaN values with 0.

### Step 3 — Default Fill
Fill missing dimension values to prevent GROUP BY issues:

| Column | Default |
|--------|---------|
| CLR (Color) | A |
| SZ (Size) | A |
| M_VND_CD (Vendor Code) | 0 |
| M_VND_NM (Vendor Name) | NA |
| MACRO_MVGR, MICRO_MVGR, FAB, SSN | NA |

### Step 4 — Segment Filter
Keep only rows where SEG (segment) is **APP** (Apparel) or **GM** (General Merchandise).

### Step 5 — Pivot by SLOC
Transform from long format to wide format:
- **Rows**: Each unique article/store combination
- **Columns**: One column per SLOC (e.g., 0001, 0002, 0004...)
- **Values**: SUM of stock quantities
- **STK_QTY** = Sum of all SLOC columns (total stock)

### Step 6 — Merge Pending Allocations
Join with MASTER_ALC_PEND to get pending allocation quantities:
- Pivot pending data by MOA (method of allocation)
- **PEND_QTY** = Sum of all pending MOA columns
- Left join on (ST_CD = RDC) and ARTICLE_NUMBER

### Step 7 — Calculate Final Quantity
```
FNL_Q = MAX(STK_QTY - PEND_QTY, 0)
```
*Final available quantity = stock minus pending allocations (minimum 0).*

### Step 8 — Generate Color Variants
Group by (ST_CD, MAJ_CAT, GEN_ART_NUMBER, CLR) and filter: only keep groups where **FNL_Q > threshold** (default 25 units).

### Step 9 — Aggregate to Hierarchy
Group by hierarchy columns (excluding ARTICLE_NUMBER, SZ) and SUM all numeric columns. Rename ST_CD to **RDC** in output.

## Output Tables

| Table | Description | Grain |
|-------|-------------|-------|
| **ARS_MSA_TOTAL** | Full pivot with all SLOCs + FNL_Q | Per article/store/size/color |
| **ARS_MSA_GEN_ART** | Aggregated by gen-art + color (recommended articles) | Per RDC/MAJ_CAT/GEN_ART/CLR |
| **ARS_MSA_VAR_ART** | Color variants (detailed before aggregation) | Per RDC/MAJ_CAT/GEN_ART/CLR (with threshold filter) |

---

# 2. PRE-GRID CALCULATIONS

## What It Does
Prepares **per-day sale rates** (SAL_PD) and **sale day counts** (SAL_D) at two levels — MAJ_CAT and Article — by merging store-level and company-level master data. These feed into the Grid Builder's MBQ/OPT_CNT formulas.

## Input Tables

| Table | Description | Level |
|-------|-------------|-------|
| Master_ALC_INPUT_ST_MAJ_CAT | Store-level settings per MAJ_CAT | Store x MAJ_CAT |
| Master_ALC_INPUT_CO_MAJ_CAT | Company-level settings per MAJ_CAT | MAJ_CAT |
| Master_ALC_INPUT_ST_MASTER | Store master (days, coverage) | Store |
| Master_ALC_INPUT_ST_ART | Store-level article settings | Store x Article |
| MASTER_ALC_INPUT_CO_ART | Company-level article settings | Article |
| MASTER_GEN_ART_SALE | Article-level planned sales (CM_SAL_Q, NM_SAL_Q, + SAL_PD computed in Step M) | Store x MAJ_CAT x GEN_ART x CLR |
| MASTER_GEN_ART_AGE | Option age in days at store level | ST_CD x MAJ_CAT x GEN_ART x CLR |

## MAJ_CAT Level Pipeline

### Step 1 — Create Working Table
Copy `Master_ALC_INPUT_ST_MAJ_CAT` into `ARS_CALC_ST_MAJ_CAT`.

### Step 2 — Merge Company Values
For each column, apply merge rules between store (ST) and company (CO):

| Column | Rule | Meaning |
|--------|------|---------|
| LISTING | co_override | Company value always wins |
| I_ROD | co_override | Company value always wins |
| DPN | max | Take the higher of ST or CO |
| DISP_Q | max | Take the higher of ST or CO |
| DISP_GR_DGR | max | Take the higher of ST or CO |
| BGT_SL_GR_DGR | max | Take the higher of ST or CO |
| CLR_MIN, CLR_MAX | max | Take the higher of ST or CO |
| MANUAL_MBQ | max | Take the higher of ST or CO |
| CONT | st_first | Store wins; CO only if store is blank |

### Step 3 — Apply Defaults
| Column | Default | Rule |
|--------|---------|------|
| LISTING | 1 | Blank or 'Y' becomes 1; 'N' becomes 0 |
| I_ROD | 1 | NULL or 0 becomes 1 |
| DISP_GR_DGR | 1 | NULL or 0 becomes 1 |
| LW_ACT_SL_GR_DGR | 1 | NULL or 0 becomes 1 |
| BGT_SL_GR_DGR | 1 | NULL or 0 becomes 1 |
| MANUAL_MBQ | NULL | Values <= 0 set to NULL |

### Step 4 — Calculate SAL_D (Total Sale Days)
Three priority sources (highest wins):

```
Priority 1: ST_MAJ_CAT own SL_CVR (if > 0)
  SAL_D = INT_DAYS + PRD_DAYS + ST_SL_CVR

Priority 2: CO_MAJ_CAT SL_CVR (if > 0)
  SAL_D = INT_DAYS + PRD_DAYS + CO_SL_CVR

Priority 3: ST_MASTER SL_CVR (base/default)
  SAL_D = INT_DAYS + PRD_DAYS + ST_MASTER_SL_CVR
```

Where:
- **INT_DAYS**: Internal days (from ST_MASTER)
- **PRD_DAYS**: Period days (from ST_MASTER)
- **SL_CVR**: Sale coverage days (from the priority source)

### Step 5 — Calculate SAL_PD (Per-Day Sale)

```
IF CM_REM_D = 0:
    SAL_PD = 0

ELSE IF CM_REM_D >= SAL_D:
    SAL_PD = CM_SAL_Q / CM_REM_D
    (Current month daily rate — enough days to cover the window)

ELSE IF SAL_D = 0:
    SAL_PD = 0

ELSE IF NM_REM_D = 0:
    SAL_PD = CM_SAL_Q / CM_REM_D
    (Fallback — no previous month data available)

ELSE:
    SAL_PD = (CM_SAL_Q + (NM_SAL_Q / NM_REM_D) x (SAL_D - CM_REM_D)) / SAL_D
    (Blend: CM actuals + NM-rate extrapolation for remaining days)
```

Where:
- **CM_SAL_Q**: Current month sale quantity
- **CM_REM_D**: Current month remaining days (days with data so far)
- **NM_SAL_Q**: Next/previous month sale quantity
- **NM_REM_D**: Next/previous month remaining days

**Example** (ST_CD=HB05, MAJ_CAT=M_TEES_HS): CM_SAL_Q=5400, CM_REM_D=25, SAL_D=9 → CM_REM_D(25) ≥ SAL_D(9) → Branch 1 → SAL_PD = 5400/25 = **216 units/day**.

## Step M — SAL_PD on MASTER_GEN_ART_SALE (Full Option Coverage)

`MASTER_GEN_ART_SALE` carries the complete planned-sale universe (~21 lakh rows), far more than `ARS_CALC_ST_ART` (~176K). SAL_PD is computed **in place** on this master table using the same formula as MAJ_CAT:

- **CM_SAL_Q, NM_SAL_Q** from MASTER_GEN_ART_SALE (the row itself)
- **CM_REM_D, NM_REM_D, SAL_D** from ARS_CALC_ST_MAJ_CAT (joined on ST_CD + MAJ_CAT)

This column feeds `ARS_LISTING.AUTO_GEN_ART_SALE` in the listing pipeline.

## Article Level Pipeline
Mirrors MAJ_CAT level but at article grain. Uses `MASTER_GEN_ART_SALE` for sale quantities and `Master_ALC_INPUT_CO_ART` for company defaults.

## Output Tables

| Table | Description |
|-------|-------------|
| **ARS_CALC_ST_MAJ_CAT** | Working table with DPN, SAL_D, SAL_PD per store x MAJ_CAT |
| **ARS_CALC_ST_ART** | Working table with SAL_D, SAL_PD per store x article |
| **MASTER_GEN_ART_SALE.SAL_PD** | Per-day-sale at option grain (ST_CD x MAJ_CAT x GEN_ART x CLR), ~21L rows |

---

# 3. GRID BUILDER

## What It Does
Creates **dynamic pivot-grid tables** that show stock quantities per store/SLOC combination, enriched with contribution percentages, MBQ (Minimum Base Quantity), and OPT_CNT (Option Count). Each grid is defined by its hierarchy columns (e.g., WERKS + MAJ_CAT, or WERKS + MAJ_CAT + RNG_SEG).

## Grid Definition

Each grid in `ARS_GRID_BUILDER` defines:

| Field | Example | Purpose |
|-------|---------|---------|
| grid_name | MJ | Identifier |
| hierarchy_columns | ["WERKS", "MAJ_CAT"] | Grouping level |
| kpi_filter | STK | Only use SLOCs with this KPI type |
| output_table | ARS_GRID_MJ | Result table name |
| status | Active | Only active grids are executed |
| pivot_only | 0 | If 1: skip lookups & calculations |
| weightage | 1.0 | Priority weight |
| grid_group | Primary | Classification |
| use_for_opt_sale | 0 | If 1: used for PER_OPT_SALE in listing |

## Execution Flow (per grid)

### Step 1 — Get Active SLOCs
```
Source: ET_STORE_STOCK + ARS_STORE_SLOC_SETTINGS
Filter: STATUS = 'ACTIVE', KPI = grid's kpi_filter (if set)
Result: List of SLOC codes (e.g., 0001, 0002, 0004, DH24_PRD_QTY...)
```

### Step 2 — Pivot Stock Data
```
Source: ET_STORE_STOCK (28M rows)
LEFT JOIN: vw_master_product (for hierarchy columns like MAJ_CAT, RNG_SEG)

Pivot: SLOC → columns, SUM(stock value) per hierarchy combination

Output columns:
  [hierarchy_cols] + [one column per SLOC] + [STK_TTL]

STK_TTL = SUM of all STK-type SLOC columns
```

### Step 3 — Post-Pivot Lookups

**Lookup 1: LISTING Filter**
- Source: `Master_ALC_INPUT_ST_MASTER`
- Join on: WERKS = ST_CD
- Action: **DELETE** rows where LISTING != 1

**Lookup 2: Calculation Data**
- Source: `ARS_CALC_ST_MAJ_CAT`
- Join on: WERKS = ST_CD, MAJ_CAT = MAJ_CAT
- Columns pulled: DISP_Q, DPN, SAL_D, SAL_PD, DISP_GR_DGR, BGT_SL_GR_DGR, CONT, etc.

**Lookup 3: Contribution (CONT)**
- Source: `Master_CONT_{LAST_HIER_COL}` (dynamic table name)
- Example: Grid MJ_RNG_SEG uses `Master_CONT_RNG_SEG`
- Join on: WERKS + MAJ_CAT + last hierarchy column
- Fallback: If store CONT is NULL, use company (CO) level CONT

### Step 4 — Calculate MBQ

```
Step A: Raw MBQ
  MBQ = (SAL_PD x BGT_SL_GR_DGR) x SAL_D + (DISP_Q x DISP_GR_DGR)

  Where BGT_SL_GR_DGR defaults to 1 if NULL/0
  Where DISP_GR_DGR defaults to 1 if NULL/0

Step B: Apply CONT
  MBQ = ROUND(MBQ x CONT, 0)

  If CONT = 0 or NULL -> MBQ = 0
```

**In words**: MBQ = planned sales volume (per-day sale rate x budget growth x sale days) + display capacity (display qty x display growth), all multiplied by the contribution percentage.

### Step 5 — Calculate OPT_CNT

```
OPT_CNT = ROUND(DISP_Q x DISP_GR_DGR x CONT / DPN, 0)

If CONT = 0 -> OPT_CNT = 0
If DPN = 0  -> OPT_CNT = 0
```

**In words**: Option count = how many display units (adjusted for growth and contribution) fit into one replenishment cycle (DPN days).

### Step 6 — Multiply DISP_Q by CONT

```
DISP_Q = ROUND(DISP_Q x CONT, 0)
```

*Runs AFTER MBQ and OPT_CNT (which use the raw DISP_Q).*

### Step 7 — Create Primary Key
On the hierarchy columns. Fills NULLs first (numeric -> 0, text -> 'NA'), deduplicates, then creates PK constraint.

## Output
One table per grid definition (e.g., ARS_GRID_MJ, ARS_GRID_MJ_RNG_SEG, ARS_GRID_MJ_CLR, etc.)

---

# 4. LISTING GENERATION

## What It Does
Creates the **master listing table** (`ARS_LISTING`) that combines grid stock data with MSA recommendations. This is the final data preparation output used for allocation decisions.

## Pipeline Overview

```
Part 1    Grid data INSERT (existing stock)
Part 2    MSA missing options INSERT (new recommendations)
Part 2.5  Create indexes (if > 5000 rows)
Part 3.5  Populate DPN, SAL_D, AUTO_GEN_ART_SALE, AGE
Part 3.55 Populate MSA_FNL_Q
Part 3.6  Classify OPT_TYPE (RL / TBL / TOC / MIX)
Part 3.7  MIX aggregation
Part 4    Grid column joins + REQ + PER_OPT_SALE
Part 5    OPT_MBQ, OPT_REQ, VAR_COUNT
```

## Part 1 — Grid Data (Existing Stock)

Insert all store x article combinations from the grid table (e.g., `ARS_GRID_MJ_GEN_ART`):

- **IS_NEW = 0** (existing options with actual stock)
- Stock columns from grid (one per SLOC)
- **STK_TTL** = sum of stock SLOCs

Filters applied:
- Active stores only (LISTING = 1 in ST_MASTER)
- Selected stores (if user filtered)
- Selected MAJ_CATs (if user filtered)

## Part 2 — MSA Missing Options

Insert MSA-recommended articles **NOT already in the listing**:

- **IS_NEW = 1** (newly recommended by MSA)
- All stock columns = 0
- STK_TTL = 0

RDC modes:
- **All**: Every MSA option for every store
- **Own RDC**: Only MSA options matching the store's own RDC
- **Cross RDC**: Take options FROM one set of RDCs, send TO stores of another

## Part 3.5 — Populate Base Attributes

| Column | Source | Join Key |
|--------|--------|----------|
| DPN | ARS_CALC_ST_MAJ_CAT | WERKS + MAJ_CAT |
| SAL_D | ARS_CALC_ST_MAJ_CAT | WERKS + MAJ_CAT |
| AUTO_GEN_ART_SALE | MASTER_GEN_ART_SALE.SAL_PD (option-level per-day-sale, ~21L rows) | WERKS + MAJ_CAT + GEN_ART + CLR |
| AGE | MASTER_GEN_ART_AGE (store-level option age in days) | ST_CD + MAJ_CAT + GEN_ART + CLR |

## Part 3.55 — MSA_FNL_Q
Pre-populate `MSA_FNL_Q` (final available quantity from MSA) for OPT_TYPE classification. Aggregated from `ARS_MSA_GEN_ART` with RDC matching.

## Part 3.6 — OPT_TYPE Classification

Each row is classified into one of 4 types:

| OPT_TYPE | Condition | Meaning |
|----------|-----------|---------|
| **RL** | DPN > 0 AND STK_TTL >= 60% x DPN | **Regular Listed** — adequate stock |
| **TBL** | STK_TTL <= 0 AND MSA_FNL_Q > 0 | **To Be Listed** — no stock but MSA recommends |
| **TOC** | DPN > 0 AND 0 < STK_TTL < 60% x DPN AND MSA_FNL_Q > 0 | **To Order Check** — low stock with MSA backup |
| **MIX** | DPN > 0 AND STK_TTL < 60% x DPN AND MSA_FNL_Q = 0 | **Mix** — low stock, no MSA recommendation |

The 60% threshold means: if current stock is less than 60% of DPN (replenishment cycle quantity), the option is considered "low stock."

## Part 3.7 — MIX Aggregation

MIX rows (IS_NEW = 0 only) are optionally collapsed into fewer lines:

| Mode | Grouping | Description |
|------|----------|-------------|
| **st_maj_rng** (default) | WERKS + MAJ_CAT + RNG_SEG | One MIX line per store x category x range segment |
| **st_maj** | WERKS + MAJ_CAT | One MIX line per store x category |
| **each** | No grouping | Keep every MIX row as-is |

Aggregated rows have:
- GEN_ART_NUMBER = 0 (sentinel)
- CLR = 'MIX', GEN_ART_DESC = 'MIX'
- All numeric columns = SUM
- DPN, SAL_D = fetched from `ARS_CALC_ST_MAJ_CAT` (NOT summed)

## Part 4 — Grid Column Joins

For each active grid (non-pivot-only):

1. Join listing with grid table on hierarchy columns
2. Add prefixed columns: `{GRID}_STK_TTL`, `{GRID}_CONT`, `{GRID}_MBQ`, `{GRID}_OPT_CNT`, `{GRID}_DISP_Q`
3. Calculate REQ:
```
{GRID}_REQ = MAX(0, {GRID}_MBQ - {GRID}_STK_TTL)
```

4. If a grid is marked `use_for_opt_sale = 1`, calculate:
```
                 (MBQ - DISP_Q)       DPN
PER_OPT_SALE = ________________ x _______
                    DISP_Q           SAL_D

= 0 if DISP_Q = 0 or SAL_D = 0
```

**PER_OPT_SALE** represents the estimated per-day sale rate per option, derived from how much the recommended MBQ exceeds the display quantity.

## Part 5 — Final Calculations

### OPT_MBQ (Optimized Minimum Base Quantity)

```
For established articles (AGE >= 15 days or AGE unknown):
  daily_rate = MAX(L-7_daily_sale, AUTO_GEN_ART_SALE)

For new articles (AGE < 15 days):
  daily_rate = MAX(PER_OPT_SALE, L-7_daily_sale, AUTO_GEN_ART_SALE)

OPT_MBQ = DPN + daily_rate x SAL_D
```

Where:
- **L-7_daily_sale** = Last 7 days sale quantity / 7
- **AUTO_GEN_ART_SALE** = Planned sale from MASTER_GEN_ART_SALE
- **PER_OPT_SALE** = From the grid flagged `use_for_opt_sale` (only for new articles)

### OPT_REQ (Optimized Requirement)
```
OPT_REQ = MAX(0, OPT_MBQ - STK_TTL)
```

### VAR_COUNT & VAR_FNL_COUNT
```
VAR_COUNT     = Total color variants for this gen-art in MSA
VAR_FNL_COUNT = Variants with FNL_Q > 0 (have available stock)
```

## Output Table
**ARS_LISTING** — the master listing with all columns from all parts, ready for allocation decisions.

---

# FORMULA QUICK REFERENCE

| Formula | Expression | Used In |
|---------|-----------|---------|
| **FNL_Q** | MAX(STK_QTY - PEND_QTY, 0) | MSA |
| **SAL_D** | INT_DAYS + PRD_DAYS + SL_CVR | Pre-Grid |
| **SAL_PD** | CM_SAL_Q/CM_REM_D (if CM covers window), else (CM + NM blend)/SAL_D | Pre-Grid |
| **MBQ** | (SAL_PD x BGT_GR x SAL_D + DISP_Q x DISP_GR) x CONT | Grid |
| **OPT_CNT** | DISP_Q x DISP_GR x CONT / DPN | Grid |
| **DISP_Q (final)** | DISP_Q x CONT | Grid |
| **REQ** | MAX(0, MBQ - STK_TTL) | Listing |
| **PER_OPT_SALE** | ((MBQ - DISP_Q) / DISP_Q x DPN) / SAL_D | Listing |
| **OPT_MBQ** | DPN + MAX(L-7/7, AUTO_SALE[, PER_OPT]) x SAL_D | Listing |
| **OPT_REQ** | MAX(0, OPT_MBQ - STK_TTL) | Listing |
| **OPT_TYPE=RL** | STK_TTL >= 60% x DPN | Listing |
| **OPT_TYPE=TBL** | STK_TTL <= 0 AND MSA_FNL_Q > 0 | Listing |
| **OPT_TYPE=TOC** | 0 < STK_TTL < 60% x DPN AND MSA_FNL_Q > 0 | Listing |
| **OPT_TYPE=MIX** | STK_TTL < 60% x DPN AND MSA_FNL_Q = 0 | Listing |

---

# 5. CONFIGURABLE PARAMETERS

These values are exposed in the Listing UI and passed to the backend via the generate API:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| **stock_threshold_pct** | 0.6 (60%) | OPT_TYPE boundary: STK_TTL vs DPN ratio that separates RL from TOC/MIX |
| **excess_multiplier** | 2.0 | Excess flag: STK_TTL > X × OPT_MBQ |
| **hold_days** | 0 | OPT_MBQ_WH hold days |
| **age_threshold** | 15 | Articles with AGE < X get PER_OPT_SALE added to the MAX in OPT_MBQ |
| **mix_mode** | st_maj_rng | MIX aggregation: `st_maj_rng` (per WERKS+MAJ_CAT+RNG_SEG), `st_maj` (per WERKS+MAJ_CAT), or `each` (no aggregation) |

---

# DATA FLOW DIAGRAM

```
Master Tables                        Working Tables                    Output
(uploaded by user)                   (computed by system)              (final result)

Master_ALC_INPUT_ST_MASTER ─┐
Master_ALC_INPUT_ST_MAJ_CAT ┼──> ARS_CALC_ST_MAJ_CAT ──────┐
Master_ALC_INPUT_CO_MAJ_CAT ┘    (DPN, SAL_D, SAL_PD,       │
                                   CONT, DISP_Q)             │
                                                              │
ET_STORE_STOCK ─────────────┐                                │
ARS_STORE_SLOC_SETTINGS ────┼──> ARS_GRID_MJ ──────────┐     │
vw_master_product ──────────┘    ARS_GRID_MJ_RNG_SEG ──┤     │
                                 ARS_GRID_MJ_CLR ──────┤     │
                                 (MBQ, OPT_CNT, DISP_Q,│     │
                                  STK_TTL per SLOC)     │     │
                                                        │     │
VW_ET_MSA_STK_WITH_MASTER ──┐                           │     │
MASTER_ALC_PEND ────────────┼──> ARS_MSA_GEN_ART ─┐    │     │
                             │   ARS_MSA_VAR_ART ──┤    │     │
                             │   (FNL_Q, stock per  │    │     │
                             │    SLOC, variants)   │    │     │
                             │                      │    │     │
                             │                      v    v     v
                             │                   ┌─────────────────────┐
MASTER_GEN_ART_SALE ─────────┼──────────────────>│    ARS_LISTING      │
MASTER_GEN_ART_AGE ──────────┘                   │    (Master Output)  │
                                                  │                     │
                                                  │  OPT_TYPE: RL/TBL/ │
                                                  │    TOC/MIX          │
                                                  │  OPT_MBQ, OPT_REQ  │
                                                  │  PER_OPT_SALE      │
                                                  │  Grid-prefix cols   │
                                                  │  VAR_COUNT          │
                                                  └─────────────────────┘
```

---

# 6. DATA MANAGEMENT

## Overview
The Data Management module provides full lifecycle control over all tables in the system — create, upload, browse, edit, export, and drop. All operations are permission-gated and audit-logged.

## All Tables (Browse)
Lists every registered table with row counts, module tags, and search filtering. Dual-mode view: **Registered** (user-created/tracked) vs **All DB Tables** (full database discovery).

## Create Table
Two creation modes:

| Mode | Description |
|------|-------------|
| **Manual Schema** | Define table name, columns, data types, PKs manually |
| **From Excel/CSV** | Upload a file → auto-infer schema (column names, types) from headers + first rows |

Auto-detects data types: INT, FLOAT, DATE, BIT, DECIMAL, NVARCHAR. Table/column names are sanitized (uppercase, special chars → underscore). First column defaults to PK.

## Upload Data
Bulk data upload from CSV/Excel files with three processing modes:

| Mode | Behavior |
|------|----------|
| **Upsert** | Match on PKs: update existing rows, insert new ones |
| **Delete** | Match on PKs: remove matching rows |
| **Append** | Insert only (no update check) |

Features:
- Column mapping UI (file header → table column)
- Row skipping, sheet selection for multi-sheet Excel
- Async processing for large files (100K+ rows)
- Job queue with status tracking (Pending → Processing → Success/Failed)
- Cancel/retry on failed jobs
- Audit batch_id links all operations to the original upload

## Table Data Viewer
Excel-like grid (ag-grid) for browsing and editing table data:

- Paginated (100–10,000 rows per page)
- Floating column filters with server-side filtering
- Inline cell editing (permission-gated, PK columns read-only)
- Cell/range selection with mouse drag or Shift+Click
- Copy: Ctrl+C (values), Ctrl+Shift+C (with headers)
- CSV export
- Column-aware formatting: CONT → 4 decimals, SALE → 2 decimals, else → integer

## Export Data
CSV/Excel export with optional async background jobs for large tables (100K+ rows).

## Jobs Dashboard
Monitor all background jobs (uploads, exports, grid builds) with real-time status badges, timing, and error details.

## Data Editor
Inline editing interface for ad-hoc data corrections. All edits are audit-logged with old/new values, user, timestamp, and IP address.

## Table Management (Settings)
Advanced schema editor for modifying existing tables:

- Add, rename, reorder, and drop columns
- Change data types with constraint validation
- Truncate table (delete all rows, keep structure)
- Drop table (soft-delete — preserves metadata for audit)
- Drag-to-reorder columns with SQL Server synchronization

## Permissions

| Permission | Allows |
|------------|--------|
| TABLE_CREATE | Create new tables |
| TABLE_ALTER | Modify schema (add/drop/rename columns) |
| TABLE_DELETE | Soft-delete tables, truncate data |
| TABLE_READ | Query data, view schemas |
| DATA_UPLOAD | Upload files (upsert/delete/append) |
| DATA_EDIT | Edit individual cells |
| DATA_EXPORT | Export data to CSV/Excel |

---

# 7. DATA VALIDATION

## Overview
The Data Validation module ensures data quality before the allocation pipeline runs. Two tools: **Store SLOC Validation** and **Data Checklist**.

## Store SLOC Validation

Manages Store Location (SLOC) configurations — the storage locations whose stock data feeds into MSA and Grid calculations.

### What It Does
- Discovers distinct SLOCs from `ET_STORE_STOCK` (the source stock table)
- Tracks each SLOC's KPI type and Active/Inactive status
- Detects new SLOCs that appear in fresh data uploads
- Supports bulk save (upsert) of multiple SLOCs at once

### SLOC Settings Table

| Column | Description |
|--------|-------------|
| SLOC | Storage location code (e.g., 0001, 0002, DH24_PRD_QTY) |
| KPI | KPI type assigned to this SLOC (e.g., STK) |
| STATUS | Active or Inactive |
| UPDATED_AT | Last modified timestamp |

### How It Works
1. User clicks **Sync** → system scans `ET_STORE_STOCK` for distinct SLOCs
2. New SLOCs appear with "New" badge — not yet saved
3. User assigns KPI type and Active/Inactive status to each SLOC
4. User saves → system upserts into `ARS_STORE_SLOC_SETTINGS`
5. Grid Builder uses only **Active** SLOCs with matching KPI filter

### Why It Matters
If a SLOC is Inactive, its stock is excluded from grid pivots and MSA calculations. This prevents test locations, damaged-goods SLOCs, or decommissioned areas from polluting stock figures.

## Data Checklist

Tracks which master tables the system depends on and monitors when they were last updated.

### What It Does
- Maintains a checklist of tables required for the allocation pipeline
- Shows live row counts and table existence status
- Records "last checked" timestamps when users review tables
- Auto-detects when tables are dropped and removes them from the checklist
- Groups items by category for organized review

### Checklist Table

| Column | Description |
|--------|-------------|
| table_name | Database table being tracked |
| display_name | User-friendly name shown in UI |
| group_name | Category grouping (e.g., "Master", "Stock", "MSA Output") |
| sort_order | Display order within group |
| active | Whether this item is currently tracked |
| last_checked_at | When user last reviewed this table |

### How It Works
1. User adds tables to the checklist via "Add Table" (picks from available DB tables)
2. System shows each table's row count (via fast partition stats — no locking)
3. User clicks "Check" on a table → stamps `last_checked_at`
4. After an upload or table rebuild, the timestamp shows the table is stale (needs re-check)
5. Auto-cleanup: if a tracked table is dropped, the checklist entry is removed

### Why It Matters
Before running the listing pipeline, operators verify that all master tables have been refreshed with current data. The checklist provides a single view: "is everything up to date?" — preventing allocation decisions based on stale data.

---

# 8. APPLICATION MENU STRUCTURE

```
ARS v2.0
├── Dashboard
├── Allocations
│
├── Data Management
│   ├── All Tables         (browse registered + all DB tables)
│   ├── Create Table       (manual schema or from Excel/CSV)
│   ├── Upload Data        (upsert / delete / append with job tracking)
│   ├── Export Data        (CSV/Excel export, async for large tables)
│   ├── Jobs Dashboard     (monitor all background jobs)
│   └── Data Editor        (inline cell editing with audit log)
│
├── Data Preparation
│   ├── MSA Stock Calculation  (9-step algorithm → ARS_MSA_GEN_ART)
│   ├── BDC Creation           (create BDC documents)
│   ├── Grid Builder           (pivot grids → MBQ/OPT_CNT)
│   ├── Lookup Art Master      (article master data lookup)
│   └── Listing                (master listing → OPT_TYPE/OPT_MBQ)
│
├── Contribution %
│   ├── Presets     (manage contribution presets)
│   ├── Mappings    (map contribution percentages)
│   ├── Execute     (run contribution calculations)
│   └── Review      (review contribution results)
│
├── Trends
│   ├── Dashboard   (trend visualization)
│   ├── Upload      (upload trend data)
│   ├── Review      (review trend uploads)
│   └── Admin       (trend administration)
│
├── Reports
│   └── Pending Allocation  (pending allocation report)
│
├── Data Validation
│   ├── Store Sloc Validation  (manage SLOC KPIs and status)
│   └── Data Checklist         (track table freshness)
│
└── Settings
    ├── App Settings       (application configuration)
    ├── Table Management   (advanced schema editor)
    ├── Users              (user account management)
    ├── Roles              (roles and permissions)
    ├── Row-Level Security (RLS policies)
    └── Audit Log          (system audit trail)
```

---

*Document generated from ARS v2.0 source code — Updated 14 April 2026*
