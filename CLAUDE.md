# CLAUDE.md — Instructions for Claude Code

## DO NOT clone or set up locally. Work directly in this repo and deploy to Azure.

## PROJECT OVERVIEW
V2 Retail Auto Replenishment System. 320+ stores, 242 MAJCATs, replaces 20-machine Excel process.
Owner: Akash Agarwal, Director V2 Retail.

## HOW TO DEPLOY (every code change)
```bash
# 1. Get Azure token
TOKEN=$(curl -s -X POST "https://login.microsoftonline.com/3eb968d0-bf19-40f9-b191-f3186ac38f02/oauth2/v2.0/token" \
  -d "client_id=8f54a771-3b04-4458-bef3-f1fa98dc38a0" \
  -d "client_secret=[AZURE_SECRET_IN_ENV]" \
  -d "scope=https://management.azure.com/.default" -d "grant_type=client_credentials" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

# 2. Zip and deploy backend
cd backend
zip -r /tmp/ars-deploy.zip . -x "__pycache__/*" "venv/*" "logs/*" "*.pyc" ".env"
curl -X POST "https://ars-v2retail-api.scm.azurewebsites.net/api/zipdeploy?isAsync=true" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/zip" --data-binary @/tmp/ars-deploy.zip

# 3. Restart
curl -X POST "https://management.azure.com/subscriptions/7c2e7784-61b3-4aa7-9967-f41b381406dd/resourceGroups/rg-ars-prod/providers/Microsoft.Web/sites/ars-v2retail-api/restart?api-version=2022-09-01" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Length: 0"

# 4. Wait 120s, then test
sleep 120
curl -s "https://ars-v2retail-api.azurewebsites.net/health"
```

## HOW TO LOGIN TO THE API
```bash
APP_TOKEN=$(curl -s -X POST "https://ars-v2retail-api.azurewebsites.net/api/v1/auth/login" \
  -H "Content-Type: application/json" -d '{"username":"superadmin","password":"Admin@12345"}' \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
```

## HOW TO TEST SNOWFLAKE (from any machine with Python)
```python
import snowflake.connector
conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='[SF_PASS_IN_ENV]',
    database='V2_ALLOCATION', schema='RESULTS', warehouse='ALLOC_WH',
)
# 246M scored pairs, 4.97M store stock rows available
```

## CREDENTIALS
- **Azure API:** https://ars-v2retail-api.azurewebsites.net | superadmin / Admin@12345
- **Azure SQL:** ars-v2retail-sql.database.windows.net | arsadmin / [SQL_PASS_IN_ENV]
- **Azure Tenant:** 3eb968d0-bf19-40f9-b191-f3186ac38f02
- **Azure Client:** 8f54a771-3b04-4458-bef3-f1fa98dc38a0 | Secret: [AZURE_SECRET_IN_ENV]
- **Snowflake:** iafphkw-hh80816 | akashv2kart / [SF_PASS_IN_ENV]
- **Supabase:** https://pymdqnnwwxrgeolvgvgv.supabase.co
  - Service Role: [SUPABASE_SERVICE_KEY_IN_ENV]
- **Dashboard:** replen.v2retail.net (Cloudflare Worker)

## ARCHITECTURE (Unified — what we're building)
```
Supabase (budget) → Snowflake (246M scores + 5M store stock) → Azure ARS (fill → size → DO)
                                                                      ↓
                                                              replen.v2retail.net
```
- **Snowflake owns:** Engines 1+2 (budget cascade + article scoring)
- **Azure ARS owns:** Engines 3+4+5 (L-ART waterfall filler + size allocator + delivery orders)
- **DO NOT rebuild scoring on Azure** — read Snowflake's 246M pre-computed scores

## WHAT NEEDS TO BE DONE (priority order)

### 1. CRITICAL: Make snowflake-connector-python work on Azure
File: `backend/requirements.txt` — add `snowflake-connector-python==3.12.3`
Problem: Azure App Service B2 crashes on cold start when this package is installed (SQL login timeout).
Options:
  a) Increase Azure startup timeout + connection pool settings
  b) Lazy-import snowflake connector (don't import at startup)
  c) Use Azure Functions as a Snowflake proxy
  d) Cache Snowflake data in Azure SQL on a schedule

### 2. CRITICAL: Wire Snowflake scores into the engine
File: `backend/app/services/allocation/engine.py`
File: `backend/app/services/allocation/snowflake_loader.py` (already created)
The engine currently falls back to a duplicate local scorer. Make it ONLY use Snowflake.
Key query: `SELECT ... FROM V2_ALLOCATION.RESULTS.ARTICLE_SCORES WHERE MAJCAT = 'M_JEANS' QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_CD ORDER BY TOTAL_SCORE DESC) <= 200`
Returns: 91K rows in 8s

### 3. CRITICAL: Wire Snowflake store stock for L-ART waterfall
File: `backend/app/services/allocation/option_filler.py` (waterfall code already exists)
Query: `SELECT STORE_CODE, GENCOLOR_KEY, STK_QTY FROM V2RETAIL.GOLD.FACT_STOCK_GENCOLOR WHERE GENCOLOR_KEY IN (...scored articles...) AND STK_QTY > 0`
Returns: 35K rows in 4s
This makes fill rate jump from 23% to ~100%

### 4. Remove duplicate code
Delete: `backend/app/services/allocation/article_scorer.py` (duplicate of Snowflake)
Delete: `backend/app/services/allocation/budget_cascade.py` (duplicate of Snowflake)
Clean: Remove all local scoring paths from `engine.py`

### 5. Run all 242 MAJCATs
Add endpoint: `POST /api/v1/allocation-engine/run-all`
Loop through Snowflake MAJCATs, run Engine 3→4→5 for each
Target: < 1 hour for all 242 categories

### 6. Daily automation
Cloudflare Worker cron at replen.v2retail.net:
  21:00 UTC → Snowflake COMPUTE_SCORES → Azure run-all → results back to Snowflake

## KEY DATA (verified, in Snowflake right now)
- ARTICLE_SCORES: 246,203,071 rows (242 MAJCATs × 455 stores × 75K articles)
- FACT_STOCK_GENCOLOR: 4,969,008 rows (326 stores × 100K articles) — L-ART data
- ALLOC_BUDGET_CASCADE: 14,999 rows (stores × MAJCATs with MBQ)
- MSA_ARTICLES: 560,367 rows (DC articles)
- Score range: 20-120 (no HERO/FOCUS data yet)

## THE ALLOCATION WATERFALL (the core algorithm)
For each store × MAJCAT:
1. Get MBQ (e.g., 55 option slots for store HA10, M_JEANS)
2. Phase 1 L-ART: Articles already in store fill slots first (from FACT_STOCK_GENCOLOR)
3. Phase 2 Continuation: L-ART with DC stock get replenishment
4. Phase 3 MIX: New DC articles fill remaining empty slots (from ARTICLE_SCORES)
5. Fill Rate = (L-ART + MIX) / MBQ (should be ~100%)
6. Size allocation: Break each option into sizes using Supabase size contribution %

## FILE STRUCTURE
```
backend/
├── app/services/allocation/
│   ├── engine.py              # Main orchestrator (1005 lines)
│   ├── snowflake_loader.py    # Reads Snowflake scores + store stock
│   ├── option_filler.py       # L-ART → MIX waterfall (397 lines)
│   ├── size_allocator.py      # Size distribution
│   ├── article_scorer.py      # ❌ DELETE (duplicate of Snowflake)
│   └── budget_cascade.py      # ❌ DELETE (duplicate of Snowflake)
├── app/api/v1/endpoints/
│   └── allocation_engine.py   # API endpoints (738 lines)
├── static/allocation.html     # Dashboard UI
└── requirements.txt           # NEEDS snowflake-connector-python
```

## FULL HANDOVER DOCUMENT
See: V2_RETAIL_HANDOVER.md in this repo (465 lines, complete data inventory, all credentials, 7 agent tasks)
