# V2 RETAIL — COMPLETE PROJECT HANDOVER FOR CLAUDE CODE
## Auto Replenishment System (ARS) — Multi-Agent Deployment Guide

**Date:** April 7, 2026
**Owner:** Akash Agarwal, Director, V2 Retail (320+ stores, value apparel retail, India)
**Goal:** Replace 20-machine 14-hour Excel replenishment process with automated system

---

# SECTION 1: CREDENTIALS & ACCESS

## 1.1 Azure (ARS Backend)
- **API:** https://ars-v2retail-api.azurewebsites.net
- **Login:** superadmin / Admin@12345
- **Azure SQL:** ars-v2retail-sql.database.windows.net / arsadmin / [SQL_PASS_IN_ENV]
- **Databases:** Claude (system), Rep_data (business)
- **Azure Tenant:** 3eb968d0-bf19-40f9-b191-f3186ac38f02
- **Azure Client ID:** 8f54a771-3b04-4458-bef3-f1fa98dc38a0
- **Azure Client Secret:** [AZURE_SECRET_IN_ENV]
- **Subscription:** 7c2e7784-61b3-4aa7-9967-f41b381406dd
- **Resource Group:** rg-ars-prod
- **GitHub repo:** https://github.com/harshalanand/ars

## 1.2 Snowflake (Scoring Engine)
- **Account:** iafphkw-hh80816
- **User:** akashv2kart
- **Password:** [SF_PASS_IN_ENV]
- **Warehouse:** ALLOC_WH
- **Database V2_ALLOCATION:** Engine 1+2 (budget + scoring)
- **Database V2RETAIL:** GOLD schema (analytics, stock, sales)
- **Dashboard:** replen.v2retail.net (Cloudflare Worker + Pages)

## 1.3 Supabase (Budget Source of Truth)
- **URL:** https://pymdqnnwwxrgeolvgvgv.supabase.co
- **Anon Key:** eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB5bWRxbm53d3hyZ2VvbHZndmd2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTMzMzU0NzYsImV4cCI6MjA2ODkxMTQ3Nn0.jUrb0jIg6qjj2Rlh9DxYesSnbstoD4uoDCswqOqAkUM
- **Service Role Key:** [SUPABASE_SERVICE_KEY_IN_ENV]
- **Key tables:** co_budget_store_major_category (387K rows, RLS blocks anon key — use service_role), co_budget_company_major_category_size (1,512 rows)

## 1.4 Local Server (S28/HOPC560)
- **Server:** HOPC560 (Windows 11 Pro)
- **SQL Server:** sa / vrl@55555, ODBC Driver 18
- **Databases:** Claude + Rep_data (same schema as Azure)
- **ARS codebase:** D:\ars\backend + D:\ars\frontend

---

# SECTION 2: THE PROBLEM WE'RE SOLVING

V2 Retail runs 320+ value apparel stores. Every day, the replenishment team decides which articles (products) in which sizes to send from DCs to stores.

**Current process (Excel):** 4 interconnected Excel files with VBA macros, run across 20 machines by 20 planners. Each planner handles a set of MAJCATs (Major Categories like M_JEANS, L_KURTI_HS). A single MAJCAT run takes 30-45 minutes. Full daily batch: 14 hours.

**Target process (automated):** Snowflake pre-computes 246M article×store scores. Azure ARS reads scores, fills option slots using L-ART waterfall, allocates sizes, generates delivery orders. All 242 MAJCATs in under 1 hour.

---

# SECTION 3: ARCHITECTURE — UNIFIED (WHAT WE NEED)

```
SUPABASE (budget source of truth)
  ├── co_budget_store_major_category: 387K rows (store × MAJCAT × month budgets)
  ├── co_budget_company_major_category_size: 1,512 rows (size contribution %)
  └── major_category_fixture_density: 558 rows (option density per MAJCAT)
         │
         ▼
SNOWFLAKE (scoring + analytics, source of truth for Engines 1+2)
  ├── V2_ALLOCATION.RESULTS.ARTICLE_SCORES: 246M scored article×store pairs
  ├── V2_ALLOCATION.RAW.ALLOC_BUDGET_CASCADE: 15K budget cascade rows
  ├── V2_ALLOCATION.RAW.MSA_ARTICLES: 560K articles
  ├── V2_ALLOCATION.RAW.CONT_RNG_SEG: 676K contribution % rows
  ├── V2_ALLOCATION.RAW.SCORE_CONFIG: 14 score weights
  ├── V2RETAIL.GOLD.FACT_STOCK_GENCOLOR: 4.97M store×article stock
  ├── V2RETAIL.GOLD.FACT_SALE_GENCOLOR: 12.5M sale rows
  └── V2RETAIL.GOLD.DIM_ARTICLE_GENCOLOR: 1.16M article master
         │
         ▼ (Azure reads Snowflake scores + store stock via snowflake-connector-python)
         │
AZURE ARS (allocation engine, Engines 3+4+5)
  ├── Engine 3: L-ART → Continuation → MIX greedy filler
  ├── Engine 4: Size allocator (Supabase size contribution %)
  ├── Engine 5: Delivery order output (store × article × size × qty × MRP)
  └── Dashboard UI: allocation.html (6 tabs)
         │
         ▼ (results pushed back to Snowflake)
         │
replen.v2retail.net (unified dashboard — analytics + allocation results)
```

---

# SECTION 4: WHAT EXISTS TODAY (DATA INVENTORY)

## 4.1 Snowflake V2_ALLOCATION.RAW (41 tables)
| Table | Rows | Purpose |
|-------|------|---------|
| ARTICLE_SCORES (RESULTS) | 246,203,071 | Pre-scored article×store pairs (242 MAJCATs, 455 stores, 75K articles) |
| MSA_ARTICLES | 560,367 | DC articles with stock by SLOC |
| BUDGET_MAJCAT | 387,341 | Store×MAJCAT budget from Supabase |
| BUDGET_STORE_MAJCAT | 387,341 | Same as above (duplicate) |
| CONT_RNG_SEG | 676,169 | RNG_SEG contribution % |
| CONT_CLR | 2,059,146 | Color contribution % |
| CONT_FAB | 825,727 | Fabric contribution % |
| CONT_MACRO_MVGR | 1,036,867 | MVGR contribution % |
| CONT_SZ | 995,552 | Size contribution % |
| CONT_VND_CD | 1,011,200 | Vendor contribution % |
| ALLOC_BUDGET_CASCADE | 14,999 | Pre-computed budget cascade |
| SCORE_CONFIG | 14 | Score weights |
| SIZE_CONTRIBUTION | 1,512 | Size distribution per MAJCAT |
| STORES | 499 | Store master |
| DC_ARTICLE_PRIORITY | 0 | ⚠️ EMPTY — HERO/FOCUS never fire |
| STORE_SPECIFIC_LISTING | 0 | ⚠️ EMPTY — ST_SPECIFIC never fires |
| FIXTURE_DENSITY | 558 | Options per store per MAJCAT |
| ARS_ARTICLE_SCORES | 727,296 | Azure allocation results pushed back |
| ARS_OPTION_ASSIGNMENTS | 55,595 | Azure option assignments pushed back |

## 4.2 Snowflake V2RETAIL.GOLD (analytics)
| Table | Rows | Purpose |
|-------|------|---------|
| FACT_STOCK_GENCOLOR | 4,969,008 | Store×article stock (326 stores, 100K articles) — **THIS IS THE L-ART DATA** |
| FACT_SALE_GENCOLOR | 12,496,659 | Store×article sales history |
| DIM_ARTICLE_GENCOLOR | 1,157,650 | Article master with MAJOR_CATEGORY |
| DIM_STORE | 315 | Store master |
| FACT_CONT_PCT_NATIONAL | 1,091 | National contribution % |
| FACT_CONT_PCT_STORE | 168,314 | Store-level contribution % |

## 4.3 Azure SQL (Rep_data)
| Table | Rows | Purpose |
|-------|------|---------|
| ALLOCATION_MRDC_RAW_DATA | ~560K | MSA articles (same as Snowflake MSA_ARTICLES) |
| alloc_engine_settings | 17 | Engine configuration |
| alloc_score_config | 14 | Score weights (same as Snowflake) |
| alloc_runs | ~50 | Allocation run history |
| alloc_assignments_* | varies | Per-run option assignments |
| alloc_variants_* | varies | Per-run size-level variants |

---

# SECTION 5: THE ALLOCATION WATERFALL (Excel Process to Replicate)

## 5.1 The Correct Process (from Excel's 29-step algorithm)

For each store × MAJCAT:

```
STEP 1: BUDGET CASCADE (Engine 1 — Snowflake)
  Company budget → Store budget → MAJCAT budget → SEG budget → Option slots
  MBQ = Minimum Buy Quantity per option slot
  Example: Store HA10, M_JEANS: 16 option slots, MBQ = 55

STEP 2: SCORE ARTICLES (Engine 2 — Snowflake, 246M pairs)
  For each DC article, compute score against each store:
    Score = ST_SPECIFIC(9999) + HERO(100) + FOCUS(60) + ASSORTED(30) +
            SEG(30) + MVGR(25) + VENDOR(20) + MRP(15) + FABRIC(10) +
            COLOR(10) + SEASON(10) + MVGR1(15) + GP_PSF(10) + NECK(5)
  Score range: 20-120 (no ST_SPECIFIC or HERO data currently)

STEP 3: L-ART WATERFALL (Engine 3 — Azure, THIS IS THE KEY)
  Phase 1: L-ART (Listed Articles)
    Count articles ALREADY IN the store (from FACT_STOCK_GENCOLOR)
    These automatically fill option slots — no DC dispatch needed
    Status: "L" (Listed) or "L_ONLY" (in store but no DC stock)

  Phase 2: CONTINUATION
    L-ART articles that ALSO have DC stock
    These get replenishment quantities from DC
    Status: "L" (continuation — can be replenished)

  Phase 3: MIX (New articles from DC)
    Remaining empty slots = MBQ - L-ART filled
    Fill with highest-scored DC articles NOT already in store
    Status: "MIX"

  Fill Rate = (L-ART + Continuation + MIX) / MBQ × 100%

  Example (Store HA10, M_JEANS):
    MBQ = 55 slots
    Store has 400+ M_JEANS articles in stock (L-ART)
    L-ART fills all 55 slots → Fill Rate = 100%
    Only need MIX if store has fewer articles than MBQ

STEP 4: SIZE ALLOCATION (Engine 4 — Azure)
  For each option assignment, break down to size level:
    M_JEANS: sz32=26%, sz34=24%, sz30=21%, sz36=12%, sz28=11%, sz38=6%
  Source: Supabase co_budget_company_major_category_size (1,512 rows)
  Output: store × article × color × size × qty × MRP × value

STEP 5: DELIVERY ORDERS (Engine 5 — Azure)
  Generate SAP-ready dispatch list
  Output: WERKS (store) × ARTICLE × SIZE × QTY × MRP × VALUE
```

---

# SECTION 6: WHAT'S BROKEN / NEEDS FIXING

## 6.1 Critical: Two scoring systems built by mistake
- **Snowflake** has 246M pre-computed scores (correct, production-scale)
- **Azure ARS** has its OWN Python scorer that re-computes 201K pairs per run (duplicate, slow)
- **Fix:** Azure must read Snowflake scores, not re-score

## 6.2 Critical: L-ART waterfall not working
- Code exists in `option_filler.py` but Snowflake store stock never loads on Azure
- `snowflake-connector-python` missing from requirements.txt (was added but app crashes on cold start)
- **Fix:** Either make snowflake-connector-python work on Azure B2, or proxy via replen.v2retail.net API

## 6.3 Critical: Fill rate shows 23% instead of ~100%
- Because L-ART phase never fires (store stock not loaded)
- All slots filled with MIX only
- **Fix:** Once L-ART works, fill rate will jump to near 100%

## 6.4 Missing data: DC_ARTICLE_PRIORITY empty
- HERO/FOCUS score weights (100/60 points) never fire
- Need to populate from V2 Retail's DC priority lists

## 6.5 Missing: Daily cron
- Snowflake COMPUTE_SCORES should run daily to refresh 246M pairs
- Azure should run allocation for all 242 MAJCATs after scoring
- Results should push back to Snowflake for analytics

---

# SECTION 7: CODE STRUCTURE

## 7.1 Azure ARS Backend (FastAPI)
```
/home/claude/ars/backend/
├── main.py                              # FastAPI entry point
├── requirements.txt                     # Python dependencies (NEEDS snowflake-connector-python)
├── app/
│   ├── api/v1/endpoints/
│   │   ├── allocation_engine.py         # 738 lines, allocation API endpoints
│   │   ├── auth.py, users.py, roles.py  # Standard CRUD
│   │   ├── tables.py                    # Dynamic table operations
│   │   ├── msa_stock.py                 # MSA calculation
│   │   ├── listing.py                   # Listing generation
│   │   └── ... (21 modules total)
│   ├── services/allocation/
│   │   ├── engine.py                    # 1005 lines — MAIN orchestrator
│   │   ├── snowflake_loader.py          # NEW — reads Snowflake scores + store stock
│   │   ├── article_scorer.py            # DUPLICATE — remove after merger
│   │   ├── budget_cascade.py            # DUPLICATE — remove after merger
│   │   ├── option_filler.py             # 397 lines — L-ART waterfall + greedy MIX filler
│   │   ├── size_allocator.py            # Size distribution per article
│   │   └── snowflake_client.py          # OLD — replaced by snowflake_loader.py
│   ├── database/session.py              # DB engines (Claude + Rep_data)
│   └── core/config.py                   # Settings
├── static/allocation.html               # 41KB dashboard UI (6 tabs)
└── scripts/                             # SQL migration scripts
```

## 7.2 Key Files to Modify

### engine.py — The orchestrator
Currently tries to:
1. Load MSA articles from Azure SQL ← REMOVE, use Snowflake
2. Score locally with article_scorer.py ← REMOVE, use Snowflake ARTICLE_SCORES
3. Load budget from Supabase ← KEEP as fallback, prefer Snowflake ALLOC_BUDGET_CASCADE
4. Run option_filler (greedy fill) ← KEEP, this is the unique value
5. Run size_allocator ← KEEP
6. Save results ← KEEP

**Target flow:**
1. Read scored pairs from Snowflake (91K rows per MAJCAT, 8s)
2. Read store stock from Snowflake (35K rows per MAJCAT, 4s)
3. Read budget from Snowflake ALLOC_BUDGET_CASCADE
4. Run L-ART waterfall (option_filler.py)
5. Run size allocator
6. Save results to Azure SQL + push to Snowflake

### snowflake_loader.py — The Snowflake data reader
Already created with 4 functions:
- `get_scored_pairs(majcat, top_n=200)` → 91K rows in 8s
- `get_store_stock(scored_gacs, majcat)` → 35K rows in 4s
- `get_budget_cascade(majcat)` → from ALLOC_BUDGET_CASCADE
- `get_dc_variant_stock(scored_gacs)` → for size allocation
- `test_connection()` → connectivity test

### option_filler.py — The waterfall
Already has L-ART → MIX waterfall code. The `fill()` method accepts `store_stock_gencolor` parameter. When this DataFrame is populated (from Snowflake), Phase 1 fills L-ART slots first, Phase 2 fills MIX.

---

# SECTION 8: SNOWFLAKE QUERIES (VERIFIED WORKING)

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='[SF_PASS_IN_ENV]',
    database='V2_ALLOCATION', schema='RESULTS', warehouse='ALLOC_WH',
)

# Get scored pairs for M_JEANS (top 200 per store)
# Returns: 91,000 rows in 8s
cur.execute("""
    SELECT ST_CD, GEN_ART_COLOR, GEN_ART, COLOR, SEG, TOTAL_SCORE,
           DC_STOCK_QTY, MRP, VENDOR_CODE, FABRIC, SEASON,
           IS_ST_SPECIFIC, PRIORITY_TYPE
    FROM ARTICLE_SCORES
    WHERE MAJCAT = 'M_JEANS'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_CD ORDER BY TOTAL_SCORE DESC) <= 200
""")

# Get store stock matching scored articles
# Returns: 35,169 rows in 4s
cur.execute("CREATE OR REPLACE TEMPORARY TABLE tmp_gacs (gac VARCHAR(100))")
# ... insert scored_gacs in batches of 500 ...
cur.execute("""
    SELECT f.STORE_CODE, f.GENCOLOR_KEY, f.STK_QTY
    FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR f
    JOIN tmp_gacs t ON f.GENCOLOR_KEY = t.gac
    WHERE f.STK_QTY > 0
""")

# Get budget cascade
cur.execute("""
    SELECT ST_CD, MAJCAT, OPT_COUNT, MBQ, BGT_DISP_Q
    FROM V2_ALLOCATION.RAW.ALLOC_BUDGET_CASCADE
    WHERE MAJCAT = 'M_JEANS'
""")
# Returns: st_cd=HA10, opt_count=16, mbq=55, bgt_disp_q=840
```

---

# SECTION 9: AGENT TASKS (FOR CLAUDE CODE)

## Agent 1: Fix Snowflake Connectivity on Azure
**Priority: CRITICAL**
**Difficulty: Medium**

The `snowflake-connector-python` package causes Azure App Service to crash on cold start (login timeout to Azure SQL). Options:
1. Add `snowflake-connector-python` to requirements.txt and increase startup timeout
2. Use a lighter Snowflake connector (snowflake-snowpark-python or REST API)
3. Run Snowflake queries from a separate Azure Function and cache results in Azure SQL
4. Use replen.v2retail.net Cloudflare Worker as a proxy (add 2 new API endpoints)

**Test:** After fix, `GET /api/v1/allocation-engine/snowflake/test` should return `{"status": "connected", "article_scores": 246203071, "store_stock": 4969008}`

## Agent 2: Complete the L-ART Waterfall
**Priority: CRITICAL**
**Difficulty: Low (code exists, just needs data)**

Once Snowflake connectivity works:
1. Verify `snowflake_loader.get_store_stock()` returns data
2. Verify `option_filler.fill()` receives `store_stock_gencolor` with data
3. Run M_JEANS — should show L-ART assignments (not all MIX)
4. Fill rate should jump from 23% to ~100%

**Test:** Run M_JEANS, check assignments have `art_status='L'` and `st_stock > 0`

## Agent 3: Remove Duplicate Scoring
**Priority: HIGH**
**Difficulty: Low**

1. In `engine.py`, remove the local scorer fallback path entirely
2. Remove `article_scorer.py` (duplicate of Snowflake COMPUTE_SCORES)
3. Remove `budget_cascade.py` (duplicate of Snowflake ALLOC_BUDGET_CASCADE)
4. Engine should ONLY read from Snowflake — no fallback

## Agent 4: Run All 242 MAJCATs
**Priority: HIGH**
**Difficulty: Medium**

1. Add endpoint: `POST /api/v1/allocation-engine/run-all`
2. Get list of MAJCATs from Snowflake: `get_available_majcats()` → 242
3. Run each MAJCAT sequentially (or parallel with thread pool)
4. Track progress and report completion
5. Target: all 242 MAJCATs in < 1 hour

## Agent 5: Daily Cron Pipeline
**Priority: MEDIUM**
**Difficulty: Medium**

1. Cloudflare Worker cron (21:00 UTC daily):
   - Trigger Snowflake COMPUTE_SCORES for all MAJCATs
   - After scoring complete, trigger Azure ARS `run-all`
2. Azure results pushed back to Snowflake tables (ARS_ARTICLE_SCORES, ARS_OPTION_ASSIGNMENTS)
3. replen.v2retail.net dashboard shows both scoring + allocation results

## Agent 6: Populate Missing Data
**Priority: MEDIUM**
**Difficulty: Low**

1. **DC_ARTICLE_PRIORITY**: Get HERO/FOCUS article lists from V2 Retail planning team, insert into Snowflake
2. **STORE_SPECIFIC_LISTING**: Get store-specific mandates, insert into Snowflake
3. **RNG_SEG mapping**: Replace MRP-quartile approximation with real SAP segment mapping
4. Re-run COMPUTE_SCORES after data populated — scores will increase from 20-120 range to 20-10119

## Agent 7: Dashboard & Reporting
**Priority: LOW**
**Difficulty: Medium**

1. Update `allocation.html` to show L-ART vs MIX breakdown per store
2. Add fill rate chart (should show ~100% when L-ART works)
3. Add company-level summary: total stores, total options, total value, by MAJCAT
4. Connect to replen.v2retail.net for unified analytics view

---

# SECTION 10: PRODUCTION TEST RESULTS (Current, MIX-only)

| MAJCAT | Scored | Slots | Variants | Duration | MRP Range | Stores |
|--------|--------|-------|----------|----------|-----------|--------|
| M_JEANS | 201,588 | 6,848 | 5,984 | 64s | ₹600-1,049 | 428 |
| FW_M_SLIPPER | 32,046 | 1,311 | 277 | 13s | ₹175-375 | 311 |
| M_TEES_HS | 342,400 | 6,848 | 6,376 | 102s | ₹200-475 | 428 |

**Note:** These use the duplicate Azure scorer (score=80 flat). With Snowflake scores (20-120 range), results will be more differentiated and accurate.

---

# SECTION 11: THE EXCEL FORMULAS (KEY REFERENCES)

The original Excel system has 300+ formulas across 22 sheets. Key ones:

- **C.3 (Budget Cascade):** `OPT at SEG = MROUND(MAJCAT_options × SEG_cont%, 1)`
- **C.6 (MBQ, 7 types):** DISP, B_MTH, SSN, DISP+B_MTH, DISP+SSN, DISP/B_MTH, DISP/SSN
- **C.7 Step 3 (Sort key):** `SEG + L-OPT% (ascending) + reverse option number` — allocates to stores with LOWEST fill rate first
- **C.8 (Conservative rounding):** Only round up if fraction > 0.7
- **C.10 (Final dispatch):** `DISP-Q = budget_dispatch × dispatch% × option_active_flag`

These are already translated into Python in `budget_cascade.py` and `option_filler.py`.

---

# SECTION 12: DEPLOYMENT COMMANDS

## Deploy to Azure
```bash
cd /home/claude/ars/backend
zip -r /tmp/ars-deploy.zip . -x "__pycache__/*" "venv/*" "logs/*" "*.pyc" ".env"

# Get Azure token
TOKEN=$(curl -s -X POST "https://login.microsoftonline.com/3eb968d0-bf19-40f9-b191-f3186ac38f02/oauth2/v2.0/token" \
  -d "client_id=8f54a771-3b04-4458-bef3-f1fa98dc38a0" \
  -d "client_secret=[AZURE_SECRET_IN_ENV]" \
  -d "scope=https://management.azure.com/.default" -d "grant_type=client_credentials" \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))")

# Deploy
curl -X POST "https://ars-v2retail-api.scm.azurewebsites.net/api/zipdeploy?isAsync=true" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/zip" --data-binary @/tmp/ars-deploy.zip

# Restart
curl -X POST "https://management.azure.com/subscriptions/7c2e7784-61b3-4aa7-9967-f41b381406dd/resourceGroups/rg-ars-prod/providers/Microsoft.Web/sites/ars-v2retail-api/restart?api-version=2022-09-01" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Length: 0"
```

## Run allocation
```bash
# Login
TOKEN=$(curl -s -X POST "https://ars-v2retail-api.azurewebsites.net/api/v1/auth/login" \
  -H "Content-Type: application/json" -d '{"username":"superadmin","password":"Admin@12345"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('access_token',''))")

# Run M_JEANS
curl -X POST "https://ars-v2retail-api.azurewebsites.net/api/v1/allocation-engine/run" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"majcats":["M_JEANS"],"rdc_code":"DH24","current_month":4}'
```

---

*This document was generated on April 7, 2026 from 4 conversation transcripts totaling 41,329 lines of dialogue, covering the complete build history of the V2 Retail ARS system.*
