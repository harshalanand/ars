# ARS Allocation Engine v3 — Complete SOP with Examples

---

## 1. OVERVIEW

### What is an OPT?
OPT = one unique combination of MAJ_CAT + GEN_ART_NUMBER + CLR.
Example: MAJ_CAT=BC_FACIAL_WH, GEN_ART_NUMBER=1116112559, CLR=BLACK is one OPT.

### What is OPT_TYPE?
| OPT_TYPE | Meaning | When assigned |
|----------|---------|---------------|
| RL | Replenishment | Store already sells this option, has stock, needs more |
| TBC | To Be Confirmed | Store has some stock but below threshold — may convert to RL or MIX |
| TBL | To Be Listed | Brand new option for the store — never sold before |
| MIX | Mixed/Excluded | Excluded from allocation |
| NL | New Listing (final) | TBL that received allocation → confirmed as new listing |

### Allocation Priority
```
RL (first) → TBC (second) → TBL (last)
```
RL gets all pool stock first. Then TBC. Then TBL gets whatever remains.

### Key Columns
| Column | Formula | Meaning |
|--------|---------|---------|
| OPT_MBQ | ACS_D + rate × ALC_D | Base store need (display + sales) |
| OPT_MBQ_WH | ACS_D + rate × (ALC_D + hold_days) | Need with hold buffer (IS_NEW only) |
| OPT_REQ | MAX(0, OPT_MBQ − STK_TTL) | Net base requirement |
| OPT_REQ_WH | MAX(0, OPT_MBQ_WH − STK_TTL) | Net WH requirement |
| Hold variance | OPT_REQ_WH − OPT_REQ | Extra buffer for new options |
| I_ROD | Rounds of demand | How many rounds to repeat allocation |
| CONT | Size contribution % | What share each size gets (from Master_CONT_SZ) |
| FNL_Q | MSA final quantity | Warehouse pool stock per variant-size |
| STK_TTL | Store stock per size | What the store already has |
| MJ_REQ | Grid-level REQ | Total MAJ_CAT requirement for the store |

---

## 2. INPUT: ARS_LISTING_WORKING

### Example Data (3 stores, 4 OPTs)

| WERKS | MAJ_CAT | GEN_ART | CLR | OPT_TYPE | OPT_MBQ | OPT_MBQ_WH | OPT_REQ | OPT_REQ_WH | STK_TTL | I_ROD | ST_RANK | MJ_REQ | ALLOC_FLAG |
|-------|---------|---------|-----|----------|---------|------------|---------|------------|---------|-------|---------|--------|------------|
| HN10 | FACIAL | 1001 | BLK | RL | 40 | 40 | 15 | 15 | 25 | 2 | 1 | 200 | 1 |
| HN10 | FACIAL | 1002 | RED | TBC | 30 | 30 | 12 | 12 | 18 | 1 | 1 | 200 | 1 |
| HN10 | FACIAL | 1003 | BLU | TBL | 33 | 46 | 33 | 46 | 0 | 1 | 1 | 200 | 1 |
| HN14 | FACIAL | 1001 | BLK | RL | 40 | 40 | 20 | 20 | 20 | 1 | 2 | 200 | 1 |

**Notes:**
- RL/TBC: OPT_MBQ_WH = OPT_MBQ (no hold days for existing options)
- TBL: OPT_MBQ_WH = 46 > OPT_MBQ = 33 (hold_days applied for IS_NEW=1)
- HN10 ST_RANK=1 (higher priority), HN14 ST_RANK=2

---

## 3. STEP 1: CREATE ARS_ALLOC_WORKING

**What:** Expand each OPT row to SIZE rows by joining with ARS_MSA_VAR_ART.

**Filter:** ALLOC_FLAG = 1 AND FNL_Q > 0

### Example: OPT 1001/BLK (RL) expands to 4 sizes

| WERKS | GEN_ART | CLR | OPT_TYPE | VAR_ART | SZ | FNL_Q | OPT_MBQ | OPT_MBQ_WH |
|-------|---------|-----|----------|---------|-----|-------|---------|------------|
| HN10 | 1001 | BLK | RL | V001 | S | 40 | 40 | 40 |
| HN10 | 1001 | BLK | RL | V002 | M | 60 | 40 | 40 |
| HN10 | 1001 | BLK | RL | V003 | L | 50 | 40 | 40 |
| HN10 | 1001 | BLK | RL | V004 | XL | 30 | 40 | 40 |
| HN14 | 1001 | BLK | RL | V001 | S | 40 | 40 | 40 |
| HN14 | 1001 | BLK | RL | V002 | M | 60 | 40 | 40 |
| HN14 | 1001 | BLK | RL | V003 | L | 50 | 40 | 40 |
| HN14 | 1001 | BLK | RL | V004 | XL | 30 | 40 | 40 |

**Same pool** (V001/S = 40 pcs) is shared between HN10 and HN14.

### Example: OPT 1003/BLU (TBL) expands to 5 sizes

| WERKS | GEN_ART | CLR | OPT_TYPE | VAR_ART | SZ | FNL_Q | OPT_MBQ | OPT_MBQ_WH |
|-------|---------|-----|----------|---------|-----|-------|---------|------------|
| HN10 | 1003 | BLU | TBL | V010 | S | 25 | 33 | 46 |
| HN10 | 1003 | BLU | TBL | V011 | M | 40 | 33 | 46 |
| HN10 | 1003 | BLU | TBL | V012 | L | 35 | 33 | 46 |
| HN10 | 1003 | BLU | TBL | V013 | XL | 20 | 33 | 46 |
| HN10 | 1003 | BLU | TBL | V014 | XXL | 10 | 33 | 46 |

**Diagnostic:**
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR, OPT_TYPE, COUNT(*) AS sizes, 
       SUM(FNL_Q) AS total_pool
FROM ARS_ALLOC_WORKING
GROUP BY WERKS, GEN_ART_NUMBER, CLR, OPT_TYPE
ORDER BY WERKS, OPT_TYPE
```

---

## 4. STEP 2: ENRICH

### Step 2a: STK_TTL (size-level store stock from ARS_GRID_MJ_VAR_ART)

**RL example (HN10, 1001/BLK):** Store has stock for some sizes:

| SZ | FNL_Q | STK_TTL | Note |
|----|-------|---------|------|
| S | 40 | 8 | Store has 8 pcs of size S |
| M | 60 | 10 | Store has 10 pcs of size M |
| L | 50 | 5 | Store has 5 pcs of size L |
| XL | 30 | 2 | Store has 2 pcs of size XL |

**TBL example (HN10, 1003/BLU):** New listing — store has NO stock:

| SZ | FNL_Q | STK_TTL | Note |
|----|-------|---------|------|
| S | 25 | 0 | New listing |
| M | 40 | 0 | New listing |
| L | 35 | 0 | New listing |
| XL | 20 | 0 | New listing |
| XXL | 10 | 0 | New listing |

### Step 2b: CONT (size contribution ratio from Master_CONT_SZ)

**Source priority:**
1. Master_CONT_SZ WHERE ST_CD = store code (store-specific)
2. Master_CONT_SZ WHERE ST_CD = 'CO' (company default)
3. FNL_Q ratio: this_size_FNL_Q / SUM(FNL_Q) per OPT
4. Last resort: 1 / COUNT(sizes)

**RL example (HN10, 1001/BLK):**

| SZ | CONT | Source | Note |
|----|------|--------|------|
| S | 0.15 | Master_CONT_SZ (ST=HN10) | Store-level |
| M | 0.35 | Master_CONT_SZ (ST=HN10) | Store-level |
| L | 0.30 | Master_CONT_SZ (CO) | CO fallback |
| XL | 0.20 | Master_CONT_SZ (CO) | CO fallback |
| **SUM** | **1.00** | | |

**TBL example (HN10, 1003/BLU) — sizes not in CONT table:**

| SZ | CONT | Source | Note |
|----|------|--------|------|
| S | 0.19 | FNL_Q ratio (25/130) | No CONT table entry |
| M | 0.31 | FNL_Q ratio (40/130) | No CONT table entry |
| L | 0.27 | FNL_Q ratio (35/130) | No CONT table entry |
| XL | 0.15 | FNL_Q ratio (20/130) | No CONT table entry |
| XXL | 0.08 | FNL_Q ratio (10/130) | No CONT table entry |
| **SUM** | **1.00** | | |

**IMPORTANT:** If CONT from Master_CONT_SZ does NOT sum to 1.0, total size demand will be less than OPT_MBQ_WH. This is the #1 cause of "only 10 pcs allocated" issues.

**Diagnostic:**
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR,
       SUM(CONT) AS cont_sum, COUNT(*) AS sz_count,
       MAX(OPT_MBQ_WH) AS opt_mbq_wh,
       ROUND(SUM(CONT) * MAX(OPT_MBQ_WH), 0) AS expected_total_demand
FROM ARS_ALLOC_WORKING
GROUP BY WERKS, GEN_ART_NUMBER, CLR
HAVING SUM(CONT) < 0.95 OR SUM(CONT) > 1.05
ORDER BY cont_sum
```

### Step 2c: Size-level demand

**Formulas:**
```
SZ_MBQ    = ROUND(OPT_MBQ    × CONT, 0)     ← base need per size
SZ_REQ    = MAX(0, SZ_MBQ    − STK_TTL)      ← net base need
SZ_MBQ_WH = ROUND(OPT_MBQ_WH × CONT, 0)     ← need with hold
SZ_REQ_WH = MAX(0, SZ_MBQ_WH − STK_TTL)      ← net WH need (waterfall uses this)
```

**RL example (HN10, 1001/BLK, OPT_MBQ=40, OPT_MBQ_WH=40):**

| SZ | CONT | STK_TTL | SZ_MBQ | SZ_REQ | SZ_MBQ_WH | SZ_REQ_WH |
|----|------|---------|--------|--------|-----------|-----------|
| S | 0.15 | 8 | 6 | 0 | 6 | 0 |
| M | 0.35 | 10 | 14 | 4 | 14 | 4 |
| L | 0.30 | 5 | 12 | 7 | 12 | 7 |
| XL | 0.20 | 2 | 8 | 6 | 8 | 6 |
| **SUM** | | | **40** | **17** | **40** | **17** |

**Note:** RL has OPT_MBQ_WH = OPT_MBQ → SZ_REQ = SZ_REQ_WH. No hold buffer.
**Note:** Size S: SZ_MBQ=6 but STK_TTL=8 → SZ_REQ=0 (store already has enough).

**TBL example (HN10, 1003/BLU, OPT_MBQ=33, OPT_MBQ_WH=46):**

| SZ | CONT | STK_TTL | SZ_MBQ | SZ_REQ | SZ_MBQ_WH | SZ_REQ_WH |
|----|------|---------|--------|--------|-----------|-----------|
| S | 0.19 | 0 | 6 | 6 | 9 | 9 |
| M | 0.31 | 0 | 10 | 10 | 14 | 14 |
| L | 0.27 | 0 | 9 | 9 | 12 | 12 |
| XL | 0.15 | 0 | 5 | 5 | 7 | 7 |
| XXL | 0.08 | 0 | 3 | 3 | 4 | 4 |
| **SUM** | | | **33** | **33** | **46** | **46** |

**Note:** TBL has STK_TTL=0 → SZ_REQ = SZ_MBQ, SZ_REQ_WH = SZ_MBQ_WH.
**Note:** SZ_REQ (33) is base need, SZ_REQ_WH (46) includes hold buffer.

**Diagnostic:**
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR, SZ, CONT, STK_TTL,
       SZ_MBQ, SZ_REQ, SZ_MBQ_WH, SZ_REQ_WH, FNL_Q,
       OPT_MBQ, OPT_MBQ_WH, OPT_REQ, OPT_REQ_WH
FROM ARS_ALLOC_WORKING
WHERE GEN_ART_NUMBER = 1001 AND WERKS = 'HN10'
ORDER BY SZ
```

---

## 5. STEP 3: TRACKING COLUMNS + SAVE ORIGINALS

**On ARS_ALLOC_WORKING:** ALLOC_QTY=0, HOLD_QTY=0, ROUND_ALLOC=0, ALLOC_STATUS='PENDING'
**On ARS_LISTING_WORKING:** ALLOC_STATUS='PENDING', OPT_REQ_ORIG, OPT_REQ_WH_ORIG

**CRITICAL:** OPT_REQ_ORIG = OPT_REQ saved NOW (before post-sync changes it).

| WERKS | GEN_ART | OPT_REQ | OPT_REQ_ORIG | OPT_REQ_WH | OPT_REQ_WH_ORIG |
|-------|---------|---------|--------------|-----------|----------------|
| HN10 | 1001 | 15 | 15 | 15 | 15 |
| HN10 | 1003 | 33 | 33 | 46 | 46 |

After allocation, post-sync will zero OPT_REQ. But OPT_REQ_ORIG stays at 15/33.

---

## 6. STEP 4: POOL TRACKER

**Key:** (RDC, MAJ_CAT, GEN_ART, CLR, VAR_ART, SZ)
**Shared across ALL stores.** All stores compete for the same pool.

| VAR_ART | SZ | FNL_Q_ORIG | FNL_Q_REM | Shared by |
|---------|-----|-----------|-----------|-----------|
| V001 | S | 40 | 40 | HN10 + HN14 |
| V002 | M | 60 | 60 | HN10 + HN14 |
| V003 | L | 50 | 50 | HN10 + HN14 |
| V004 | XL | 30 | 30 | HN10 + HN14 |

---

## 7. STEP 4.5: STORE BUDGET

| WERKS | MAJ_CAT | MJ_REQ_ORIG | MJ_REQ_REM | TOTAL_ALLOC | ELIGIBLE |
|-------|---------|------------|-----------|------------|----------|
| HN10 | FACIAL | 200 | 200 | 0 | 1 |
| HN14 | FACIAL | 200 | 200 | 0 | 1 |

---

## 8. STEP 5: INITIAL ELIGIBILITY

| Check | Rule | Example Pass | Example Fail |
|-------|------|-------------|-------------|
| E1 | LISTING = 1 | LISTING=1 → Pass | LISTING=0 → INELIGIBLE |
| E2 | ALLOC_FLAG = 1 | PRI_CT%=100 → Pass | PRI_CT%=50 → INELIGIBLE |
| E3 | OPT_TYPE != 'MIX' | OPT_TYPE=RL → Pass | OPT_TYPE=MIX → INELIGIBLE |
| E4 | MSA_FNL_Q > 0 | MSA_FNL_Q=160 → Pass | MSA_FNL_Q=0 → INELIGIBLE |
| E5 | OPT_REQ_WH >= 1 | OPT_REQ_WH=15 → Pass | OPT_REQ_WH=0 → INELIGIBLE |

---

## 9. STEP 6: PRIMARY ALLOCATION

### 9.1 RL Round 1 — Waterfall Example

**Processing:** RL first, Round 1.

**OPT 1001/BLK, Size M pool:** FNL_Q_REM = 60, two stores compete:

| Store | ST_RANK | SZ_REQ_WH | prev_demand | pool_available | round_alloc |
|-------|---------|-----------|------------|---------------|-------------|
| HN10 | 1 (first) | 4 | 0 | 60 | **4** |
| HN14 | 2 (second) | 8 | 4 | 56 | **8** |

HN10 gets 4, HN14 gets 8. Pool M remaining = 60 - 12 = 48.

**OPT 1001/BLK, Size S pool:** FNL_Q_REM = 40:

| Store | ST_RANK | SZ_REQ_WH | prev_demand | pool_available | round_alloc |
|-------|---------|-----------|------------|---------------|-------------|
| HN10 | 1 | 0 | 0 | 40 | **0** (SZ_REQ_WH=0, STK enough) |
| HN14 | 2 | 3 | 0 | 40 | **3** |

HN10 size S already has enough stock (STK_TTL=8 >= SZ_MBQ=6).

**After RL Round 1 — HN10 alloc_working for 1001/BLK:**

| SZ | SZ_REQ_WH | ROUND_ALLOC | Pool after |
|----|-----------|-------------|------------|
| S | 0 | 0 | 40 |
| M | 4 | 4 | 48 |
| L | 7 | 7 | 43 |
| XL | 6 | 6 | 24 |
| **SUM** | **17** | **17** | |

**Budget clip:** HN10 total = 17, MJ_REQ_REM = 200. 17 < 200 → no clip.

**Pool deduction:** FNL_Q_REM reduced by ROUND_ALLOC for each size.

**Commit:** ALLOC_QTY += ROUND_ALLOC at size level. ROUND_ALLOC = 0.

**Post-sync:** OPT_REQ_WH = MAX(0, 40 - 25 - 17) = 0 → this OPT is fully served for this round.

### 9.2 RL Round 2 (I_ROD = 2)

Only HN10's OPT 1001/BLK has I_ROD=2. HN14 has I_ROD=1 → skipped.

**Scale demand for round 2:**
```
SZ_MBQ    = OPT_MBQ    × 2 × CONT
SZ_REQ    = MAX(0, SZ_MBQ − STK_TTL − prev_ALLOC_QTY)
```

| SZ | CONT | SZ_MBQ (R2) | STK_TTL | prev_alloc | SZ_REQ_WH (R2) |
|----|------|------------|---------|-----------|----------------|
| S | 0.15 | 12 | 8 | 0 | 4 |
| M | 0.35 | 28 | 10 | 4 | 14 |
| L | 0.30 | 24 | 5 | 7 | 12 |
| XL | 0.20 | 16 | 2 | 6 | 8 |

Round 2 demands more — for 2 replenishment cycles.

**Waterfall runs again** with updated pool (FNL_Q_REM already reduced from R1).

### 9.3 RECALC after RL completes

After all RL rounds finish:
1. MJ_REQ = MAX(0, MJ_REQ_ORIG − total_alloc_for_store)
2. Recalculate H_ flags
3. Recalculate PRI_CT% / SEC_CT%
4. Update ALLOC_FLAG

| WERKS | MJ_REQ_ORIG | RL_alloc | MJ_REQ_REM | ELIGIBLE |
|-------|------------|---------|-----------|----------|
| HN10 | 200 | 55 | 145 | 1 |
| HN14 | 200 | 28 | 172 | 1 |

Both stores still eligible → TBC can proceed.

---

### 9.4 TBC Round 1

**OPT 1002/RED (TBC):** Only HN10 has this OPT.

| SZ | CONT | STK_TTL | SZ_REQ_WH | FNL_Q_REM | round_alloc |
|----|------|---------|-----------|-----------|-------------|
| S | 0.20 | 5 | 1 | 30 | 1 |
| M | 0.30 | 8 | 1 | 45 | 1 |
| L | 0.30 | 4 | 5 | 38 | 5 |
| XL | 0.20 | 1 | 5 | 20 | 5 |
| **SUM** | | | **12** | | **12** |

**After commit:** ALLOC_QTY per size = [1, 1, 5, 5] = 12 total.

**RECALC after TBC:**

| WERKS | MJ_REQ_ORIG | RL+TBC_alloc | MJ_REQ_REM | ELIGIBLE |
|-------|------------|-------------|-----------|----------|
| HN10 | 200 | 55+12=67 | 133 | 1 |

Still eligible → TBL can proceed.

---

### 9.5 TBL Round 1

**OPT 1003/BLU (TBL):** New listing, STK_TTL=0 for all sizes. Hold variance = 46 - 33 = 13.

**Waterfall uses SZ_REQ_WH (includes hold buffer):**

| SZ | CONT | SZ_REQ_WH | FNL_Q_REM | round_alloc |
|----|------|-----------|-----------|-------------|
| S | 0.19 | 9 | 25 | 9 |
| M | 0.31 | 14 | 40 | 14 |
| L | 0.27 | 12 | 35 | 12 |
| XL | 0.15 | 7 | 20 | 7 |
| XXL | 0.08 | 4 | 10 | 4 |
| **SUM** | | **46** | | **46** |

**Pool deduction:** Full 46 pcs deducted from pool (covers ALLOC + HOLD).

**Budget clip:** HN10 total = 67 + 46 = 113. MJ_REQ_REM = 133. 113 < 133 → no clip.

**Commit:** ALLOC_QTY at size level = [9, 14, 12, 7, 4] = 46 total.

**TBL Size Validation:** 5 sizes available out of 5 → 100% >= 60% threshold → PASS.

---

### 9.6 RECALC after TBL

| WERKS | MJ_REQ_ORIG | Total alloc | MJ_REQ_REM | ELIGIBLE |
|-------|------------|------------|-----------|----------|
| HN10 | 200 | 113 | 87 | 1 |

---

## 10. STEP 7: FALLBACK (optional)

### When does fallback trigger?
When `enable_fallback = True` and some OPTs have ALLOC_FLAG = 0 (PRI_CT% < 100).

### Example: 3 Primary grids — MJ (seq=1), RNG_SEG (seq=2), CLR (seq=3)

**OPT X:** Has MJ_REQ=50, RNG_SEG_REQ=30, but CLR_REQ=0.
- H_MJ=1, H_RNG_SEG=1, H_CLR=0
- GH_MJ=1, GH_RNG_SEG=1, GH_CLR=1
- PRI_CT% = (1+1+0) / (1+1+1) × 100 = 67% → ALLOC_FLAG = 0 → INELIGIBLE

### Fallback Level 1: Demote CLR (highest seq=3)
- CLR becomes Secondary
- New PRI_CT% = (1+1) / (1+1) × 100 = 100% → ALLOC_FLAG = 1 → now eligible!
- Insert OPT X into alloc_working
- Apply boost

### Boost modes
**full_mbq (default):** OPT_MBQ × growth%
```
OPT_MBQ = 33, growth = 130%
Boosted OPT_MBQ = 33 × 1.3 = 43
```

**sales_only:** ACS_D + (OPT_MBQ − ACS_D) × growth%
```
ACS_D = 18, OPT_MBQ = 33, growth = 130%
Sales component = 33 − 18 = 15
Boosted OPT_MBQ = 18 + (15 × 1.3) = 18 + 19.5 = 38
(display component stays at 18, only velocity boosted)
```

**str (STR-based tiers):** Dynamic boost based on days-of-cover
| Days of cover (STK_TTL / daily_sale) | Boost % |
|--------------------------------------|---------|
| < 30 days | 150% |
| < 45 days | 130% |
| < 60 days | 120% |
| < 90 days | 110% |
| >= 90 days | 100% (no boost) |

### After boost: recalculate SZ_MBQ, SZ_REQ, SZ_MBQ_WH, SZ_REQ_WH
Then run waterfall (only_new=True — skips already-processed OPTs).

### Fallback Level 2: Demote RNG_SEG (seq=2)
- RNG_SEG becomes Secondary
- Only MJ (seq=1) remains as Primary → PRI_CT% = H_MJ/GH_MJ × 100
- Even more OPTs become eligible
- Run allocation again for newly eligible OPTs

### After all fallback levels: restore demoted grids to Primary.

---

## 11. STEP 8: REFLECT TO WORKING TABLE (ALLOC/HOLD SPLIT)

This is the final output — where ALLOC_QTY and HOLD_QTY appear on ARS_LISTING_WORKING.

### Step 8.1: Total dispatched

```sql
ALLOC_QTY = SUM(size-level ALLOC_QTY) from ARS_ALLOC_WORKING
```

### Step 8.2: ALLOC/HOLD Split using OPT_REQ_ORIG

```
ALLOC_QTY = MIN(total_dispatched, OPT_REQ_ORIG)    ← base need FIRST
HOLD_QTY  = total_dispatched − ALLOC_QTY             ← hold reduced first when pool short
```

**Rule: ALLOC always gets priority. HOLD is sacrificed first.**

### RL Example (HN10, 1001/BLK)
```
dispatched = 17 (sum of all sizes from alloc_working)
OPT_REQ_ORIG = 15
ALLOC_QTY = MIN(17, 15) = 15
HOLD_QTY  = 17 − 15 = 2
```
Wait — RL has OPT_MBQ_WH = OPT_MBQ → OPT_REQ_WH = OPT_REQ = 15. So dispatched (17) > OPT_REQ (15) means the waterfall gave 2 pcs extra. HOLD_QTY = 2. This can happen due to rounding across sizes.

### TBC Example (HN10, 1002/RED)
```
dispatched = 12
OPT_REQ_ORIG = 12
ALLOC_QTY = MIN(12, 12) = 12
HOLD_QTY  = 12 − 12 = 0
```
No hold — TBC has OPT_MBQ_WH = OPT_MBQ.

### TBL Example (HN10, 1003/BLU) — WITH hold buffer
```
dispatched = 46 (from waterfall using SZ_REQ_WH)
OPT_REQ_ORIG = 33

ALLOC_QTY = MIN(46, 33) = 33  ← base need filled
HOLD_QTY  = 46 − 33 = 13      ← hold buffer (extra for new listing)
```

### TBL Example — partial pool (only 40 available)
```
dispatched = 40
OPT_REQ_ORIG = 33

ALLOC_QTY = MIN(40, 33) = 33  ← base need still fully filled!
HOLD_QTY  = 40 − 33 = 7       ← hold reduced (13 → 7)
```

### TBL Example — very short pool (only 25 available)
```
dispatched = 25
OPT_REQ_ORIG = 33

ALLOC_QTY = MIN(25, 33) = 25  ← all goes to alloc (base not fully filled)
HOLD_QTY  = 25 − 25 = 0       ← hold completely sacrificed
```

### Step 8.3: ALLOC_STATUS
| Status | Condition |
|--------|-----------|
| ALLOCATED | ALLOC_QTY >= OPT_REQ_ORIG |
| PARTIAL | ALLOC_QTY > 0 but < OPT_REQ_ORIG |
| NOT_PROCESSED | No allocation at all |
| INELIGIBLE | Failed E1-E5 |

### Final Result for HN10:

| GEN_ART | CLR | OPT_TYPE | OPT_REQ_ORIG | Dispatched | ALLOC_QTY | HOLD_QTY | Status |
|---------|-----|----------|-------------|-----------|-----------|----------|--------|
| 1001 | BLK | RL | 15 | 17 | 15 | 2 | ALLOCATED |
| 1002 | RED | TBC | 12 | 12 | 12 | 0 | ALLOCATED |
| 1003 | BLU | TBL | 33 | 46 | 33 | 13 | ALLOCATED |

**Diagnostic:**
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR, OPT_TYPE, FINAL_OPT_TYPE,
       OPT_REQ_ORIG, OPT_REQ_WH_ORIG,
       ALLOC_QTY, HOLD_QTY,
       ALLOC_QTY + ISNULL(HOLD_QTY, 0) AS TOTAL_DISPATCHED,
       ALLOC_STATUS
FROM ARS_LISTING_WORKING
WHERE WERKS = 'HN10'
ORDER BY OPT_TYPE, GEN_ART_NUMBER
```

---

## 12. STEP 8.5: FINALIZE OPT_TYPE

| GEN_ART | Original | ALLOC > 0? | FINAL_OPT_TYPE |
|---------|----------|-----------|----------------|
| 1001 | RL | Yes | RL |
| 1002 | TBC | Yes | RL (confirmed as replenishment) |
| 1003 | TBL | Yes | NL (new listing confirmed) |

If TBC got 0: FINAL_OPT_TYPE = MIX
If TBL got 0: FINAL_OPT_TYPE = TBL (stays as request)

---

## 13. STEP 9: CLEANUP

Drop #alloc_pool, #store_budget, ARS_ALLOC_BREAK_RANKS.

---

## 14. COMPLETE DIAGNOSTIC QUERIES

### A. Check CONT distribution per OPT
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR, SZ, CONT, FNL_Q, STK_TTL,
       SUM(CONT) OVER (PARTITION BY WERKS, GEN_ART_NUMBER, CLR) AS CONT_SUM,
       SZ_MBQ, SZ_REQ, SZ_MBQ_WH, SZ_REQ_WH
FROM ARS_ALLOC_WORKING
WHERE WERKS = 'HN10' AND GEN_ART_NUMBER = 1003
ORDER BY SZ
```

### B. Check size-level allocation
```sql
SELECT WERKS, GEN_ART_NUMBER, SZ, OPT_TYPE, CONT, STK_TTL,
       SZ_REQ, SZ_REQ_WH, FNL_Q, ALLOC_QTY,
       ALLOC_STATUS, ALLOC_ROUND
FROM ARS_ALLOC_WORKING
WHERE WERKS = 'HN10' AND GEN_ART_NUMBER = 1003
ORDER BY SZ
```

### C. Check OPT-level ALLOC/HOLD split
```sql
SELECT WERKS, GEN_ART_NUMBER, CLR, OPT_TYPE,
       OPT_MBQ, OPT_MBQ_WH,
       OPT_REQ_ORIG, OPT_REQ_WH_ORIG,
       ALLOC_QTY, HOLD_QTY,
       ALLOC_QTY + ISNULL(HOLD_QTY, 0) AS TOTAL,
       ALLOC_STATUS, FINAL_OPT_TYPE
FROM ARS_LISTING_WORKING
ORDER BY WERKS, MAJ_CAT, OPT_TYPE, GEN_ART_NUMBER
```

### D. Check budget (MJ_REQ cap)
```sql
SELECT W.WERKS, W.MAJ_CAT,
       MAX(ISNULL(W.MJ_REQ_ORIG, W.MJ_REQ)) AS MJ_REQ,
       SUM(ISNULL(W.ALLOC_QTY, 0) + ISNULL(W.HOLD_QTY, 0)) AS TOTAL_DISPATCHED,
       CASE WHEN SUM(ISNULL(W.ALLOC_QTY, 0) + ISNULL(W.HOLD_QTY, 0)) 
            > MAX(ISNULL(W.MJ_REQ_ORIG, W.MJ_REQ))
            THEN 'OVER-BUDGET' ELSE 'OK' END AS BUDGET_STATUS
FROM ARS_LISTING_WORKING W
WHERE ISNULL(W.ALLOC_QTY, 0) + ISNULL(W.HOLD_QTY, 0) > 0
GROUP BY W.WERKS, W.MAJ_CAT
ORDER BY TOTAL_DISPATCHED DESC
```

### E. Check pool consumption
```sql
SELECT GEN_ART_NUMBER, CLR, SZ, FNL_Q_ORIG, FNL_Q_REM,
       FNL_Q_ORIG - FNL_Q_REM AS CONSUMED,
       ROUND(CAST(FNL_Q_ORIG - FNL_Q_REM AS FLOAT) / NULLIF(FNL_Q_ORIG, 0) * 100, 1) AS PCT_USED
FROM #alloc_pool
WHERE GEN_ART_NUMBER = 1003
ORDER BY SZ
```

### F. Summary by OPT_TYPE
```sql
SELECT OPT_TYPE, ALLOC_STATUS, COUNT(*) AS cnt,
       SUM(ALLOC_QTY) AS total_alloc, SUM(HOLD_QTY) AS total_hold,
       SUM(ALLOC_QTY + ISNULL(HOLD_QTY, 0)) AS total_dispatched
FROM ARS_LISTING_WORKING
GROUP BY OPT_TYPE, ALLOC_STATUS
ORDER BY OPT_TYPE, ALLOC_STATUS
```

---

## 15. KNOWN ISSUES AND WATCHPOINTS

### Issue 1: CONT not summing to 1.0
**Symptom:** OPT should get 46 pcs but only gets 10.
**Cause:** Master_CONT_SZ has values summing to 0.22 for this MAJ_CAT.
**Diagnose:** Run diagnostic query A — check CONT_SUM column.
**Options:**
- Fix data in Master_CONT_SZ so values sum to 1.0
- Normalize CONT in code (divide each by SUM — business decision)
- Accept as-is (business may want intentional under-fill)

### Issue 2: OPT_REQ_ORIG = 0
**Symptom:** ALLOC_QTY=0, everything goes to HOLD.
**Cause:** Store's STK_TTL >= OPT_MBQ at listing time → OPT_REQ=0.
**Meaning:** Store doesn't actually need base stock — any allocation is pure hold buffer.

### Issue 3: Pool exhaustion
**Symptom:** Later stores (higher ST_RANK) get PARTIAL or 0.
**Cause:** Earlier stores consumed all pool.
**Diagnose:** Run diagnostic query E — check PCT_USED.

### Issue 4: ALLOC_FLAG = 0
**Symptom:** OPT marked INELIGIBLE despite having requirement.
**Cause:** PRI_CT% < 100 — not all primary grids have coverage for this option.
**Fix:** Enable fallback to demote grids and retry.

### Issue 5: IS_NEW=0 but expecting HOLD
**Symptom:** RL/TBC options always have HOLD_QTY=0.
**Cause:** For existing options, OPT_MBQ_WH = OPT_MBQ (no hold days). This is correct behavior — hold buffer only applies to new listings (IS_NEW=1, OPT_TYPE=TBL).
