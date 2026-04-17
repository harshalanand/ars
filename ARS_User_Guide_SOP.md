# ARS v2.0 — User Guide & Standard Operating Procedures

**Auto Replenishment System** — Operations Manual for Planners, Analysts & Allocators

---

# 1. INTRODUCTION

## Who This Guide Is For
This document is written for end users (merchandising planners, allocation analysts, demand planners) who operate the ARS v2.0 system. It explains **what each module does, when to use it, and the exact steps to run it successfully**.

## What ARS Does
ARS takes raw stock data, MSA recommendations, master data, and contribution rules — and produces a prioritized replenishment plan (the "Listing") used to decide which articles to ship to which stores.

## The Four Core Modules (In Order)

```
 ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
 │  1. DATA VALIDATION │ →  │  2. MSA STOCK CALC   │ →  │   3. GRID BUILDER   │ →  │   4. LISTING        │
 │  (Verify sources)   │    │  (Available stock)  │    │  (Pivoted stock +   │    │  (Final output for  │
 │                     │    │                     │    │   MBQ/OPT_CNT)      │    │   allocation)       │
 └─────────────────────┘    └─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

Always run them in this order. Each module depends on the output of the previous one.

## Before You Start
- You need a valid ARS login with appropriate permissions
- The date shown in the header must match today's date
- Recent data uploads (stock, sales, master) should be completed
- The sidebar shows all available menus — items you don't have permission for are hidden

---

# 2. RECOMMENDED DAILY / WEEKLY WORKFLOW

## Daily (Morning Run)
1. **Check Data Validation → Data Checklist** — confirm overnight data loads completed
2. **Verify Data Validation → Store SLOC Validation** — resolve any new SLOCs flagged
3. **Run MSA Stock Calculation** — regenerate available stock at RDCs
4. **Run Grid Builder** — rebuild all active grid tables
5. **Run Listing (Full Pipeline)** — generate the master allocation listing
6. **Review Listing preview** — spot-check summary counts (MIX / RL / TBL / TBC)
7. **Export Listing** — download for allocation planning

## Weekly (Every Monday)
1. Review **Contribution % → Mappings** for any category changes
2. Review **Contribution % → Execute** to refresh CONT values
3. Run the full daily workflow
4. Review **Reports → Pending Allocation**

## After Master Data Changes
1. Re-run **Contribution % → Execute** if contribution data changed
2. Re-run **MSA Stock Calculation** if stock master changed
3. Re-run **Grid Builder** and **Listing**

---

# 3. DATA VALIDATION

## Purpose
Before running allocation calculations, verify that all master tables are fresh and all stock source locations are correctly configured. Stale data or misconfigured SLOCs will silently corrupt the output.

## Menu Location
`Data Validation` → expand sub-menu

---

## 3.1 Data Checklist — SOP

**Navigate to:** `Data Validation → Data Checklist`

### What You See
A list of tables grouped by category (e.g., Master, Stock, MSA Output), each showing:
- Table name / display name
- Live row count
- "Last checked" timestamp
- Check button

### When to Use It
- Every morning before running calculations
- After any master data upload
- When debugging unexpected allocation results

### Step-by-Step
1. Scan the list — any table with an **old "Last checked" timestamp** relative to today's expected data load needs attention.
2. For each stale table:
   - Click the table name (opens the data viewer)
   - Verify row count is as expected (compare with source system)
   - Spot-check 5–10 recent rows for correctness
   - Return to the Checklist and click **Check** → this stamps the current time
3. If a table shows **zero rows** or is **missing**, investigate before proceeding. Do NOT run MSA / Grid / Listing with missing master data.
4. To add a new table to tracking: click **Add Table**, pick the table from the dropdown, set its display name and group.

### Success Criteria
All tables you rely on have `Last checked` = today.

### Common Issues
| Symptom | Likely Cause | Action |
|---------|--------------|--------|
| Table shows 0 rows | Upload failed / was truncated | Re-run upload job |
| Table missing | Table was dropped | Restore from backup or regenerate |
| Row count much lower than yesterday | Partial upload / filter issue | Investigate source data |

---

## 3.2 Store SLOC Validation — SOP

**Navigate to:** `Data Validation → Store Sloc Validation`

### What This Page Does
Stock comes from multiple storage locations (SLOCs) per store — some are real warehouse bins, others are virtual (pending allocation, intransit). Each SLOC must be classified with a **KPI type** (e.g., STK for sellable stock) and marked **Active/Inactive**. The system only uses Active SLOCs with the right KPI for calculations.

### What You See
A table of SLOCs with:
- SLOC code (e.g., 0001, 0002, DH24_PRD_QTY)
- KPI type (editable dropdown)
- Status: Active / Inactive (editable toggle)
- "New" badge on SLOCs that appeared in the latest stock upload but haven't been saved yet
- Latest data date column

### When to Use It
- After every stock data upload (ET_STORE_STOCK refresh)
- When a new store or SLOC is introduced
- When stock figures look wrong (a decommissioned SLOC might be polluting totals)

### Step-by-Step
1. Click **Sync** (top right) — the system scans `ET_STORE_STOCK` for all distinct SLOCs.
2. Any SLOC not in `ARS_STORE_SLOC_SETTINGS` will appear with a **"New" badge**.
3. For each new SLOC:
   - Identify its purpose (ask warehouse team if unsure)
   - Set its **KPI** (typically `STK` for sellable stock, leave blank for virtual SLOCs)
   - Set **Status**: Active if it represents real sellable stock, Inactive if it's a decommissioned / test / virtual location
4. Click **Save** — bulk upsert applies all changes at once.
5. Re-verify by refreshing the page — "New" badges should disappear.

### Success Criteria
- No "New" badge SLOCs remaining
- All real sellable SLOCs marked Active with KPI = STK
- All virtual / test / decommissioned SLOCs marked Inactive

### Common Issues
| Symptom | Likely Cause | Action |
|---------|--------------|--------|
| Stock totals too high | A test SLOC is Active and polluting sums | Mark it Inactive |
| Stock totals too low | A real SLOC was accidentally set Inactive | Toggle it Active |
| "New" SLOC appears after every upload | Upload includes rotating virtual SLOCs | Set its KPI appropriately once, won't recur |

---

# 4. MSA STOCK CALCULATION

## Purpose
Calculates **available stock per RDC/article** by combining warehouse stock with pending allocations. Outputs feed the Grid Builder and Listing.

## Menu Location
`Data Preparation → MSA Stock Calculation`

## What It Does (Conceptually)
For each RDC and article:
```
Available Stock = Warehouse Stock − Pending Allocations
```
Then it keeps only articles with enough available stock to recommend for allocation, and aggregates to the color/style level.

---

## 4.1 MSA Stock Calculation — SOP

### Before You Run
- [ ] Data Checklist → `VW_ET_MSA_STK_WITH_MASTER` and `MASTER_ALC_PEND` are both current
- [ ] Store SLOC Validation has no "New" SLOCs pending
- [ ] You know which SLOCs you want included (usually the standard set)

### Step-by-Step

1. **Open** the MSA Stock Calculation page.

2. **Select SLOCs** (optional)
   - Use the SLOC multi-select to restrict which storage locations contribute to stock totals.
   - If empty, all SLOCs are used.
   - Recommendation: leave empty unless you have a specific reason to filter.

3. **Set the Availability Threshold** (default 25 units)
   - This is the minimum `FNL_Q` an article must have to be recommended.
   - Raising it makes the recommendation list more conservative.
   - Lowering it includes more long-tail articles.

4. **Click Run**
   - The pipeline runs the 9-step algorithm:
     - Filter by SLOC → fill numeric → fill defaults → filter SEG (APP/GM) → pivot by SLOC → merge pending → compute FNL_Q → filter by threshold → aggregate
   - Progress is shown as steps complete

5. **Review Output**
   - Row counts for each output table should appear:
     - `ARS_MSA_TOTAL` (detailed pivot)
     - `ARS_MSA_GEN_ART` (aggregated — the one consumed by Listing)
     - `ARS_MSA_VAR_ART` (color variants — used for VAR_COUNT)

### Success Criteria
- All three output tables have rows
- `ARS_MSA_GEN_ART` row count is in the expected range (typically 10K–20K for full run)
- No error messages

### Common Issues
| Symptom | Cause | Action |
|---------|-------|--------|
| 0 rows in output | Threshold too high / no pending data / SLOC filter too tight | Lower threshold, check inputs |
| Takes > 10 minutes | Large stock dataset | Normal for full run; monitor Jobs Dashboard |
| Row count very low | Threshold too conservative | Lower threshold and re-run |

---

# 5. GRID BUILDER

## Purpose
Creates **pivoted stock grids** — tables showing stock quantities per SLOC column per hierarchy level (e.g., per store × MAJ_CAT, or per store × MAJ_CAT × RNG_SEG). Each grid also carries its own MBQ (replenishment target) and OPT_CNT (option count) figures.

## Menu Location
`Data Preparation → Grid Builder`

## Grid Concepts

| Concept | Meaning |
|---------|---------|
| **Grid** | One pivot table at a specific hierarchy level |
| **Hierarchy columns** | The grouping dimensions — e.g. [WERKS, MAJ_CAT, RNG_SEG] |
| **KPI filter** | Only use SLOCs with this KPI type (e.g., STK for sellable stock) |
| **Output table** | The result table name (e.g., ARS_GRID_MJ_RNG_SEG) |
| **Active / Inactive** | Only Active grids run; Inactive are skipped |
| **Pivot-only** | If checked, skip MBQ/OPT_CNT calculations (use for lookup-only grids) |
| **Weightage** | Priority weight if multiple grids compete |
| **Use for PER_OPT_SALE** | Marks the ONE grid whose MBQ/DISP_Q feed the listing's PER_OPT_SALE column |

---

## 5.1 Running Grid Builder — SOP

### Before You Run
- [ ] MSA Stock Calculation completed successfully
- [ ] Contribution % → Execute has been run (if CONT values changed)
- [ ] Pre-grid calculations have been run (SAL_D, SAL_PD in ARS_CALC_ST_MAJ_CAT)

### Step-by-Step

1. **Open** the Grid Builder page. You see a list of defined grids.

2. **Review grid statuses**
   - Active grids have a green badge
   - Inactive grids are grayed out
   - Pivot-only grids have a purple "PIVOT ONLY" tag
   - The grid marked OPT_SALE has a green "OPT_SALE" badge (should be exactly ONE)

3. **Run all active grids**
   - Click **Run All Active** (top right)
   - Each grid runs its 7-step pipeline:
     1. Get active SLOCs
     2. Pivot stock
     3. Post-pivot lookups (LISTING filter, ARS_CALC_ST_MAJ_CAT, CONT)
     4. Calculate MBQ
     5. Calculate OPT_CNT
     6. Multiply DISP_Q × CONT
     7. Create primary key
   - You can watch progress in the Jobs Dashboard

4. **Verify each output table**
   - Click a grid row → it opens the output table
   - Spot-check: STK_TTL column has numbers, MBQ > 0, OPT_CNT > 0, CONT between 0 and 1

### Success Criteria
- All active grids show "Built OK" status
- Row counts match expectations (should be close to your store count × category count)
- Output tables are populated

### Common Issues
| Symptom | Cause | Action |
|---------|-------|--------|
| MBQ = 0 everywhere | SAL_PD or SAL_D missing in ARS_CALC_ST_MAJ_CAT | Re-run Contribution % → Execute |
| OPT_CNT = 0 everywhere | DPN = 0 or CONT = 0 | Check Master_CONT_* tables |
| Grid fails with "missing column" | A hierarchy column renamed in master | Edit grid definition, remap hierarchy |
| Primary key creation fails | Duplicate hierarchy combinations | Usually means a master table has dupes — clean and re-run |

---

## 5.2 Creating / Editing a Grid

### When to Create a New Grid
- A new reporting hierarchy is needed (e.g., pivot by MACRO_MVGR)
- Category-specific MBQ rules require a separate grid
- Pivot-only lookups for a new dimension

### Step-by-Step
1. Click **Add Grid** (top right)
2. Fill in:
   - **Grid Name** — short identifier (e.g., `MJ_RNG_SEG`). Prefix `MJ_` for standard grids.
   - **Description** — one-line purpose
   - **Hierarchy Columns** — pick from `vw_master_product` columns (multi-select)
   - **KPI Filter** — typically `STK`
   - **Output Table** — auto-suggested as `ARS_GRID_<name>`
   - **Status** — Active to run immediately
   - **Grid Group** — Primary (core grids) or Secondary (supplementary)
   - **Weightage** — 1.0 default
   - **Use for PER_OPT_SALE** — check only if this should drive the listing's PER_OPT_SALE; uncheck on the other grid first
3. Click **Save** — grid definition stored
4. Click **Run** on the new grid to build the output table for the first time

### Editing an Existing Grid
Click the pencil icon → modify → Save → Re-run. The output table is rebuilt from scratch.

---

# 6. LISTING

## Purpose
Generates the **master listing table** (`ARS_LISTING`) — the final output combining grid data with MSA recommendations. This is what planners use to decide which articles go to which stores.

## Menu Location
`Data Preparation → Listing`

## What It Produces
For each store × article combination:
- Actual stock (`STK_TTL`)
- Target stock (`MBQ`, `OPT_MBQ`)
- Replenishment need (`REQ`, `OPT_REQ`)
- Classification (`OPT_TYPE`: RL / TBL / TBC / MIX)
- Supporting data (DPN, SAL_D, AUTO_GEN_ART_SALE, AGE, MSA_FNL_Q, VAR_COUNT, etc.)

---

## 6.1 Listing — SOP

### Before You Run
- [ ] Data Validation checks passed (Data Checklist, Store SLOC Validation)
- [ ] MSA Stock Calculation completed
- [ ] Grid Builder completed for all active grids
- [ ] `MASTER_GEN_ART_AGE` table is populated (option age master data)
- [ ] Contribution % pipeline executed

### Step-by-Step

1. **Open** the Listing page.

2. **Select Run Mode**
   - **Listing Only** — fastest, only rebuilds ARS_LISTING (use when upstream outputs are already fresh)
   - **Full Pipeline** — also runs MSA + Grid Builder as part of the job (use at start of day)

3. **Select RDC Mode**
   - **All** — every store gets every MSA option from every RDC (broadest)
   - **Own** — each store only receives options from its own RDC (most common)
   - **Cross** — take options FROM specific source RDCs, send TO target RDC stores

4. **Select Stores** (optional)
   - Empty = all active stores
   - Typed filter works in the dropdown

5. **Select MAJ_CATs** (optional)
   - Empty = all categories

6. **Choose MIX Rows mode**
   - **MAJ+RNG** (default) — one MIX aggregate line per (store, MAJ_CAT, RNG_SEG) — finer granularity
   - **MAJ** — one MIX aggregate line per (store, MAJ_CAT) — coarser (legacy behavior)
   - **Each** — no aggregation, keep every MIX article as its own row

7. **Set Variables** (keep defaults unless you have a reason)
   - **Stock %** (default 0.6) — threshold for RL vs MIX/TBC classification
   - **Excess ×** (default 2) — when STK > X × OPT_MBQ, flag as excess
   - **Hold days** (default 15) — for new articles, extra cover days added
   - **AGE<** (default 15) — articles younger than N days use PER_OPT_SALE in OPT_MBQ
   - **Fill %** (default 0.6) — VAR_FNL_COUNT/VAR_COUNT threshold for MIX(b)

8. **Click Generate**
   - The pipeline runs:
     - Part 1: Grid data insert
     - Part 2: MSA missing options insert (IS_NEW=1)
     - Part 3.5: DPN, SAL_D, AUTO_GEN_ART_SALE, AGE populated
     - Part 3.55: MSA_FNL_Q + VAR_COUNT populated
     - Part 3.6: OPT_TYPE classified (MIX/RL/TBC/TBL)
     - Part 3.7: MIX aggregation (based on mode)
     - Part 4: Grid column joins + OPT_MBQ + REQ
   - A summary toast shows counts: `MIX=N, TBL=N, TBC=N, RL=N`

9. **Review the Preview**
   - The preview table below shows the first page of results
   - OPT_TYPE column is color-coded:
     - Green = RL (adequate stock)
     - Amber = TBL / TBC (action needed)
     - Red = MIX (no action — low stock without MSA backup)
     - IS_NEW = 1 rows have a yellow background
   - Click column headers to sort; use floating filters for column-level filtering
   - Use the global search for cross-column text search

10. **Export**
    - Click **Export** → downloads as Excel/CSV
    - Or use **Working** / **Full Listing** toggle to control export scope

### Success Criteria
- Total rows roughly = (number of active stores) × (average options per store)
- OPT_TYPE distribution is sensible:
  - RL: typically 40-60% of rows
  - TBL: typically 20-40% (many new MSA options)
  - TBC: small (few percent)
  - MIX: small (few percent after aggregation)
  - untagged: should be 0
- OPT_MBQ values are reasonable (not all 0, not absurdly large)

### Common Issues

| Symptom | Likely Cause | Action |
|---------|--------------|--------|
| Many "untagged" rows | DPN is missing (no ARS_CALC_ST_MAJ_CAT match) | Re-run Contribution Execute |
| All OPT_TYPE = MIX | MSA_FNL_Q not populated (MSA table missing or join failed) | Check MSA table + RDC join |
| OPT_MBQ = 0 for most rows | SAL_D=0 or all daily rates=0 (no sales, no AUTO_GEN_ART_SALE, no L-7) | Check MASTER_GEN_ART_SALE.SAL_PD computed |
| TBL count very low | MSA options aren't matching by CLR/GEN_ART_NUMBER | Verify MSA and listing CLR values match (case/whitespace) |
| Pipeline takes very long (> 10 min) | Large dataset + many grids | Normal — monitor progress; use Listing-Only mode if upstream is fresh |
| Generate button fails with "missing table" | A prerequisite table missing | Go back to Data Checklist |

---

## 6.2 Understanding OPT_TYPE

Each option-row is classified into one of 4 types (evaluated top-to-bottom):

### MIX — Mix / Not Actionable
**Rule (a):** `DPN > 0  AND  STK < 60%×DPN  AND  MSA_FNL_Q = 0`
Option has low stock AND no MSA recommendation — no clear action.

**Rule (b):** `IS_NEW=0  AND  VAR_COUNT > 0  AND  VAR_FNL_COUNT/VAR_COUNT < 60%`
Existing option with poor color fill across variants.

Aggregated (1 MIX row per store × MAJ_CAT × RNG_SEG by default) so the listing stays readable.

### RL — Regular Listing (Replenishment)
**Rule:** `DPN > 0  AND  STK >= 60% × DPN`
Stock is adequate. Normal replenishment applies based on `OPT_REQ`.

### TBC — To Be Check
**Rule:** `DPN > 0  AND  0 < STK < 60%×DPN  AND  MSA_FNL_Q > 0`
Stock is low BUT MSA has some available — human review to decide if replenish or not.

### TBL — To Be Listed
**Rule:** `STK <= 0  AND  MSA_FNL_Q > 0`
Store has zero stock of this option, and MSA recommends it — list it and ship.

### Priority order
If multiple rules could match, the **first rule wins**: MIX → RL → TBC → TBL. This means:
- An option with low stock, MSA available, AND poor VAR fill → becomes MIX (not TBC)
- An option with zero stock and good MSA → becomes TBL (never hits MIX because MIX needs MSA=0)

---

## 6.3 Understanding OPT_MBQ (The Target Stock Level)

```
For established options (AGE >= 15 days or AGE unknown):
  daily_rate = MAX(L-7_daily_sale, AUTO_GEN_ART_SALE)

For new options (AGE < 15 days):
  daily_rate = MAX(PER_OPT_SALE, L-7_daily_sale, AUTO_GEN_ART_SALE)

OPT_MBQ = DPN + daily_rate × SAL_D
OPT_REQ = MAX(0, OPT_MBQ − STK_TTL)
```

**In plain English:** Target stock = display floor (DPN) + enough days of forward cover at the highest reliable daily-sale rate we have.

- L-7/7 = last 7 days of sales ÷ 7 (actual recent demand)
- AUTO_GEN_ART_SALE = planned per-day-sale from MASTER_GEN_ART_SALE
- PER_OPT_SALE = option-level planning signal (only used for new articles where L-7 is unreliable)
- SAL_D = total sale-cover window (from ARS_CALC_ST_MAJ_CAT)
- DPN = display/floor quantity

`OPT_REQ` is what to ship to bring the store up to target.

---

# 7. QUICK REFERENCE

## Keyboard Shortcuts (in tables / previews)
| Shortcut | Action |
|----------|--------|
| Click cell | Select single cell |
| Shift + Click | Extend selection range |
| Click + drag | Select cell range |
| Ctrl + C | Copy selected cells |
| Ctrl + Shift + C | Copy with column headers |
| Ctrl + F | Global search (in preview) |

## Number Formatting (in the viewer)
| Column Pattern | Format |
|----------------|--------|
| Contains `CONT` | 4 decimals (e.g., 0.1234) |
| Contains `SAL` / `SALE` | 2 decimals (e.g., 216.00) |
| Everything else | Integer (e.g., 435) |

## File Locations of Key Output Tables

| Module | Output Table | Purpose |
|--------|--------------|---------|
| MSA | `ARS_MSA_TOTAL` | Full pivot, all SLOCs + FNL_Q |
| MSA | `ARS_MSA_GEN_ART` | Aggregated recommendations (feeds Listing) |
| MSA | `ARS_MSA_VAR_ART` | Color variants |
| Grid | `ARS_GRID_*` | One per grid definition |
| Pre-Grid | `ARS_CALC_ST_MAJ_CAT` | DPN, SAL_D, SAL_PD per store × MAJ_CAT |
| Pre-Grid | `ARS_CALC_ST_ART` | Per store × article |
| Pre-Grid | `MASTER_GEN_ART_SALE.SAL_PD` | Option-grain daily sale rate |
| Listing | `ARS_LISTING` | Master output for allocation |

---

# 8. TROUBLESHOOTING DECISION TREE

```
Something's wrong with the listing
│
├── All rows untagged? → DPN missing → Re-run Contribution Execute
│
├── All rows MIX? → MSA_FNL_Q not populating → Check MSA table + RDC filter
│
├── OPT_MBQ all zero? → SAL_D or daily rates zero → Check pre-grid calc pipeline
│
├── Row count too low? → Store filter / MAJ_CAT filter too tight → Clear filters
│
├── Row count too high? → "All" RDC mode + no filter → Use "Own" mode
│
├── Takes too long? → Run mode = Full Pipeline → Switch to "Listing Only" if upstream is fresh
│
├── Numbers look wrong? → Data Checklist — is everything up to date? → Re-run validation
│
└── System error / crash? → Check Jobs Dashboard for error details → Report to IT with job_id
```

---

# 9. GLOSSARY

| Term | Meaning |
|------|---------|
| **Article / Option** | A unique style-color combination (MAJ_CAT × GEN_ART_NUMBER × CLR) |
| **AGE** | Days since the option was first introduced at this store |
| **AUTO_GEN_ART_SALE** | Planned per-day-sale rate from MASTER_GEN_ART_SALE |
| **BDC** | Business Distribution Center documents |
| **CONT** | Contribution % — relative weight of this hierarchy within its parent |
| **CM_SAL_Q / NM_SAL_Q** | Current / Next (or Previous) month sale quantity |
| **CM_REM_D / NM_REM_D** | Days of data available in each period |
| **DISP_Q** | Display quantity — base units per display unit |
| **DPN** | Days-per-N or Display-Per-Norm — the floor stock level |
| **FNL_Q** | Final available quantity at an RDC after pending allocation |
| **GEN_ART_NUMBER** | Generic article number (style, before color split) |
| **IS_NEW** | 1 if the row came from MSA recommendations (new to store), 0 if from existing grid |
| **KPI** | SLOC classification tag (e.g., STK = sellable stock) |
| **L-7 DAYS SALE-Q** | Total sale quantity in last 7 days |
| **LISTING** | Master output + name of the pipeline module |
| **MAJ_CAT** | Major Category (top-level merchandise hierarchy) |
| **MBQ** | Minimum Base Quantity — target stock per option based on forecast |
| **MIX** | Option classification: low stock + no MSA backup, or poor VAR fill |
| **MSA** | Minimum Stock Availability — recommendation engine |
| **MSA_FNL_Q** | Available MSA stock for this option at the store's RDC |
| **OPT_CNT** | Option Count — # of display units in one replenishment cycle |
| **OPT_MBQ** | Optimized MBQ — demand-driven target = DPN + rate × SAL_D |
| **OPT_REQ** | Optimized Requirement = max(0, OPT_MBQ − STK_TTL) |
| **OPT_TYPE** | Classification: RL / TBL / TBC / MIX |
| **PER_OPT_SALE** | Estimated daily sale per option (used only for new articles) |
| **PEND_ALC** | Pending Allocation — stock committed but not yet shipped |
| **RDC** | Regional Distribution Center |
| **REQ** | Requirement = max(0, MBQ − STK_TTL) per grid prefix |
| **RL** | Regular Listing (replenishment) — adequate stock |
| **RNG_SEG** | Range Segment — mid-level hierarchy |
| **SAL_D** | Sale-cover window in days = INT_DAYS + PRD_DAYS + SL_CVR |
| **SAL_PD** | Per-day-sale rate |
| **SLOC** | Storage Location (warehouse bin identifier) |
| **STK_TTL** | Total stock — sum across active SLOCs |
| **TBL** | To Be Listed — zero stock + MSA available |
| **TBC** | To Be Check — low stock + MSA available |
| **VAR_COUNT** | Total number of color variants for this gen-art in MSA |
| **VAR_FNL_COUNT** | Variants with FNL_Q > 0 (actually stocked) |
| **WERKS** | Store code (legacy SAP terminology) |

---

*ARS v2.0 User Guide & SOP — Updated 15 April 2026*
