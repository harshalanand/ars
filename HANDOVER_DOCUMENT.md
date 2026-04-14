# ARS - Auto Replenishment System v2.0
## Developer Handover Document

**Project Owner:** Santosh Kumar
**Date:** 2026-04-06
**Version:** 2.0

---

## 1. PROJECT OVERVIEW

ARS is an enterprise-grade multi-store retail auto-replenishment system. It manages inventory allocation, MSA stock calculations, contribution analysis, BDC creation, trend analysis, and store-level data validation.

**Architecture:** FastAPI (Python) Backend + React (Vite) Frontend
**Database:** SQL Server 2019+ (Two-database model on server HOPC560)
**Authentication:** JWT (HS256, 8-hour access tokens)

---

## 2. TECHNOLOGY STACK

### Backend
| Component | Technology | Version |
|-----------|-----------|---------|
| Framework | FastAPI | 0.109.2 |
| Server | Uvicorn | 0.27.1 |
| ORM | SQLAlchemy | 2.0.27 |
| DB Driver | pyodbc | 5.1.0 |
| Data Processing | pandas, numpy | 2.2.0, 1.26.4 |
| Auth | python-jose, passlib (bcrypt) | 3.3.0, 1.7.4 |
| Validation | Pydantic | 2.6.1 |
| Logging | Loguru | 0.7.2 |

### Frontend
| Component | Technology | Version |
|-----------|-----------|---------|
| Framework | React | 18.3.1 |
| Build Tool | Vite | 5.4.0 |
| Router | react-router-dom | 6.26.0 |
| State | Zustand | 4.5.4 |
| Data Grid | AG Grid Enterprise | 32.0.2 |
| Charts | Recharts | 2.12.7 |
| HTTP | Axios | 1.7.4 |
| Styling | Tailwind CSS | 3.4.7 |
| Icons | Lucide React | 0.424.0 |

---

## 3. DIRECTORY STRUCTURE

```
D:\ars\
|
+-- backend\
|   +-- main.py                           # FastAPI entry point
|   +-- requirements.txt                  # Python dependencies
|   +-- app\
|   |   +-- api\v1\
|   |   |   +-- router.py                 # All route registrations
|   |   |   +-- endpoints\                # 21 API modules
|   |   |       +-- auth.py               # Login, JWT, password
|   |   |       +-- users.py              # User CRUD
|   |   |       +-- roles.py              # Role management
|   |   |       +-- rls.py                # Row-level security
|   |   |       +-- tables.py             # Table operations
|   |   |       +-- msa_stock.py          # MSA stock calculation
|   |   |       +-- grid_builder.py       # Grid builder
|   |   |       +-- bdc.py                # BDC creation
|   |   |       +-- contrib.py            # Contribution %
|   |   |       +-- trends.py             # Trend analysis
|   |   |       +-- checklist.py          # Data checklist
|   |   |       +-- listing.py            # Listing generation
|   |   |       +-- dashboard.py          # Dashboard API
|   |   |       +-- reports.py            # Reports
|   |   |       +-- settings.py           # App settings
|   |   |       +-- maintenance.py        # TempDB cleanup (superadmin)
|   |   |       +-- lookup_art_master.py  # Article master lookup
|   |   |       +-- sloc_validation.py    # SLOC validation
|   |   |       +-- audit.py              # Audit logs
|   |   |       +-- msa.py                # Legacy MSA
|   |   |       +-- allocations.py        # Allocation engine
|   |   +-- services\                     # Business logic
|   |   |   +-- auth_service.py
|   |   |   +-- msa_service.py            # MSA 9-step calculation
|   |   |   +-- msa_result_storage.py     # ARS_MSA_* table storage
|   |   |   +-- msa_job_service.py        # Background MSA jobs
|   |   |   +-- allocation_engine.py      # Allocation algorithm
|   |   |   +-- upsert_engine.py          # Bulk upsert with audit
|   |   |   +-- table_mgmt_service.py     # Dynamic table DDL
|   |   |   +-- upload_job_service.py     # Background upload
|   |   |   +-- export_job_service.py     # Background export
|   |   |   +-- audit_service.py          # Audit logging
|   |   |   +-- tempdb_cleanup_service.py # TempDB cleanup thread
|   |   |   +-- grid_calculations.py      # Grid builder logic
|   |   +-- models\                       # SQLAlchemy models
|   |   |   +-- rbac.py                   # Users, Roles, Permissions
|   |   |   +-- retail.py                 # Products, Allocations, Stock
|   |   |   +-- rls.py                    # Stores, Access controls
|   |   |   +-- audit.py                  # Audit, Jobs, Permissions
|   |   +-- schemas\                      # Pydantic request/response
|   |   +-- security\                     # JWT, password, dependencies
|   |   +-- database\
|   |   |   +-- session.py                # DB engines & sessions
|   |   +-- core\
|   |       +-- config.py                 # All settings (DB, JWT, etc.)
|   +-- scripts\                          # SQL migrations & DB creation
|       +-- create_claude_db.sql          # Full Claude DB schema
|       +-- create_rep_data_db.sql        # Full Rep_data DB schema
|       +-- 011_create_msa_result_tables.sql
|       +-- 012_create_msa_tracking_tables.sql
|       +-- 013_create_msa_storage_jobs.sql
|       +-- 014_create_ars_msa_tables.sql
|
+-- frontend\
    +-- package.json
    +-- vite.config.js
    +-- src\
        +-- App.jsx                       # Route definitions
        +-- services\api.js               # Axios API client
        +-- store\authStore.js            # Zustand auth state
        +-- pages\                        # 38 page components
        +-- components\
            +-- layout\Sidebar.jsx        # Navigation menu
            +-- filters\CascadingFilters.jsx
            +-- tables\                   # AG Grid components
```

---

## 4. DATABASE ARCHITECTURE

### Two-Database Model

| Database | Purpose | MDF Path | LDF Path |
|----------|---------|----------|----------|
| **Claude** | System DB: RBAC, RLS, Audit, Jobs, Metadata | `E:\MSSQL_DATA\Claude.mdf` | `E:\MSSQL_DATA\Claude_log.ldf` |
| **Rep_data** | Business DB: Products, Stock, Allocations, MSA results | `E:\MSSQL_DATA\Rep_data.mdf` | `E:\MSSQL_DATA\Rep_data_log.ldf` |

**Server:** HOPC560
**Auth:** SQL Server (sa / vrl@55555)
**Driver:** ODBC Driver 18 for SQL Server
**RCSI:** Enabled on Rep_data (readers don't block writers)

### Database Creation Scripts
- `backend/scripts/create_claude_db.sql` - Creates Claude DB + all 20 system tables
- `backend/scripts/create_rep_data_db.sql` - Creates Rep_data DB + all 21 business tables

Run these on a fresh SQL Server to recreate the entire schema from scratch.

---

## 5. DATABASE SCHEMA - CLAUDE (System DB)

### RBAC Tables (5)

**rbac_roles** - System roles (SUPER_ADMIN, ADMIN, PLANNER, ANALYST, VIEWER)
```
id INT PK IDENTITY, role_name NVARCHAR(100) UNIQUE, role_code NVARCHAR(50) UNIQUE,
description NVARCHAR(500), is_system_role BIT, is_active BIT, created_at, updated_at, created_by
```

**rbac_permissions** - 50+ granular permissions per module/action/resource
```
id INT PK, permission_name, permission_code UNIQUE, module, action, resource, description, is_active, created_at
```

**rbac_role_permissions** - Role-to-permission mapping
```
id INT PK, role_id FK, permission_id FK, granted_at, granted_by | UNIQUE(role_id, permission_id)
```

**rbac_users** - User accounts with login tracking
```
id INT PK, username UNIQUE, email UNIQUE, mobile_no UNIQUE, password_hash, full_name,
employee_code, phone, is_active, is_locked, failed_attempts, last_login, password_changed_at,
created_at, updated_at, created_by
```

**rbac_user_roles** - User-to-role mapping
```
id INT PK, user_id FK, role_id FK, assigned_at, assigned_by, is_active | UNIQUE(user_id, role_id)
```

### RLS Tables (5)

**rls_stores** - Master store directory
```
id INT PK, store_code UNIQUE, store_name, region, hub, division, business_unit,
store_grade (A/B/C/D), city, state, is_active, created_at, updated_at
```

**rls_user_store_access** - Per-user store access
```
id INT PK, user_id FK, store_code, access_level (READ/WRITE/FULL), granted_at, granted_by, is_active
```

**rls_user_region_access** - Bulk region access
```
id INT PK, user_id FK, region, hub, division, business_unit, access_level, granted_at, granted_by, is_active
```

**rls_column_restrictions** - Column visibility/masking per role
```
id INT PK, table_name, column_name, role_id FK, is_visible, is_masked, mask_pattern, can_edit, created_at
```

**rls_table_role_access** - Table-level CRUD per role
```
id INT PK, table_name, role_id FK, can_read, can_write, can_upload, can_export, granted_at, granted_by
```

### Audit Tables (2)

**audit_log** - Master audit trail
```
id BIGINT PK, table_name, action_type (INSERT/UPDATE/DELETE/UPSERT/BULK_UPLOAD/SCHEMA_CHANGE),
record_primary_key, old_data JSON, new_data JSON, changed_columns JSON, changed_by, changed_at,
source (UI/API/UPLOAD/SYSTEM), ip_address, user_agent, session_id, batch_id, duration_ms, row_count, notes
```

**data_change_log** - Column-level change detail
```
id BIGINT PK, audit_log_id, table_name, action_type, record_key, column_name,
old_value, new_value, data_type, changed_by, changed_at, source, batch_id, row_index
```

### Job Tracking Tables (4)

**export_jobs** - Background export tracking
```
id BIGINT PK, job_id UNIQUE, table_name, status, format, columns JSON, filters JSON,
total_rows, processed_rows, file_path, file_size, error_message, created_by, created_at,
started_at, completed_at, downloaded
```

**upload_jobs** - Background upload tracking
```
id BIGINT PK, job_id UNIQUE, table_name, file_name, file_path, file_size, status,
primary_key_columns, mode (upsert/delete), total/processed/inserted/updated/deleted/error_rows,
error_message, error_details JSON, created_by, ip_address, timestamps, duration_ms
```

**msa_storage_jobs** - MSA result storage jobs
```
id BIGINT PK, job_id UNIQUE, sequence_id, status, total_rows, processed_rows,
inserted_msa, inserted_colors, inserted_variants, error_message, created_by, timestamps, duration_ms
```

**table_permissions** / **table_settings** / **export_settings** - Configuration tables

### Metadata Registry (2)

**sys_table_registry** - All tables registered
```
id INT PK, table_name UNIQUE, display_name, description, module, primary_key_columns JSON,
is_system_table, is_active, row_count, created_at, updated_at, created_by
```

**sys_column_registry** - Column metadata per table
```
id INT PK, table_id FK, column_name, display_name, data_type, max_length,
is_nullable, is_primary_key, default_value, column_order, is_active, created_at
```

---

## 6. DATABASE SCHEMA - REP_DATA (Business DB)

### Product Hierarchy (5 tables)
```
retail_division → retail_sub_division → retail_major_category
retail_size_master, retail_color_master
```

### Article Tables (2)
```
retail_gen_article (parent) → retail_variant_article (size x color variants)
```

### Allocation Tables (2)
```
alloc_header (allocation run metadata)
alloc_detail (per-store x variant quantities)
```

### Stock & Sales (3)
```
store_stock (current inventory per store/variant)
store_sales (daily sales history)
warehouse_stock (central warehouse levels)
```

### Contribution Analysis (3)
```
Cont_presets (formula/standard configurations)
Cont_mappings (suffix/value mapping rules)
Cont_mapping_assignments (column-to-mapping links)
```

### MSA Calculation Results (6)
```
MSA_Calculation_Sequence (sequence tracking with parameters)
MSA_Column_Definitions (dynamic column registry)
MSA_Filter_Config (saved filter presets)
ARS_MSA_TOTAL (id + sequence_id + dynamic columns)
ARS_MSA_GEN_ART (id + sequence_id + dynamic columns)
ARS_MSA_VAR_ART (id + sequence_id + dynamic columns)
```

### Other
```
ARS_SLOC_SETTINGS (SLOC configuration)
ARS_CHECKLIST (data freshness tracking)
ARS_LISTING (dynamically generated listing table)
```

### External/Source Tables (read-only)
```
ET_MSA_STK, VW_ET_MSA_STK_WITH_MASTER, MASTER_ALC_PEND,
Master_ALC_INPUT_ST_MASTER, COUNT_STOCK_DATA_18M, Master_HIER_*
```

---

## 7. KEY ALGORITHMS

### MSA Stock Calculation (9 Steps)

**File:** `backend/app/services/msa_service.py` → `calculate()`

```
Step 1: Filter by SLOC (store locations)
Step 2: Normalize STK_Q to numeric, fill NaN → 0
Step 3: Fill missing dimensions (CLR→"A", M_VND_NM→"NA", SZ→"A", etc.)
Step 4: Filter by SEG = ['APP', 'GM'] (Apparel, General Merchandise)
Step 5: Pivot by SLOC → creates store-level stock columns
Step 6: Load MASTER_ALC_PEND, pivot by MOA, merge on ST_CD + ARTICLE_NUMBER
Step 7: Calculate FNL_Q = max(STK_QTY - PEND_QTY, 0)
Step 8: Generate color variants — group by [ST_CD, MAJ_CAT, GEN_ART_NUMBER, CLR],
        keep rows where group FNL_Q sum > threshold
Step 9: Aggregate to generated colors — group by hierarchy columns, sum metrics
```

**Output:** 3 result sets saved to ARS_MSA_TOTAL, ARS_MSA_GEN_ART, ARS_MSA_VAR_ART
**Note:** ST_CD is renamed to RDC in all output

### Allocation Engine

**File:** `backend/app/services/allocation_engine.py`

```
1. Resolve eligible stores (by division/region/grade)
2. Fetch warehouse stock (available inventory)
3. Calculate per-store quantity based on allocation basis:
   - RATIO: grade_ratio[store_grade] * size_curve[size]
   - SALES: proportional to historical sales
   - STOCK: fill to target inventory level
   - MANUAL: use override_qty
4. Apply min/max per-store constraints
5. Cap at warehouse availability
6. Save AllocationHeader + AllocationDetail rows
```

### Background Job Processing

All heavy operations (upload, export, MSA storage) use a worker thread pattern:
```
1. API creates job record (status: pending)
2. Job added to in-memory Queue
3. Daemon worker thread picks up jobs FIFO
4. Worker updates status: running → completed/failed
5. Frontend polls job status endpoint
```

---

## 8. AUTHENTICATION & AUTHORIZATION

### Login Flow
```
POST /api/v1/auth/login { username, password }
  → bcrypt verify → generate access_token (8hr) + refresh_token (7d)
  → response includes user roles + permissions
```

### Permission System
- **50+ granular permissions** organized by module (admin, tables, data, allocation, etc.)
- **5 default roles:** SUPER_ADMIN, ADMIN, PLANNER, ANALYST, VIEWER
- **Row-Level Security:** per-store and per-region access filtering
- **Column Security:** hide/mask sensitive columns (cost_price, margin_pct) per role

### Default Super Admin
```
Username: superadmin
Email: admin@nubo.in
Password: Admin@12345
```

---

## 9. HOW TO SET UP & RUN

### Prerequisites
- Python 3.11+
- Node.js 18+
- SQL Server 2019+ with ODBC Driver 18
- Git

### Backend Setup
```bash
cd D:\ars\backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Create databases (run in SSMS or sqlcmd)
# Execute: scripts/create_claude_db.sql
# Execute: scripts/create_rep_data_db.sql

# Start server
python main.py
# or: uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Setup
```bash
cd D:\ars\frontend
npm install
npm run dev          # Development (localhost:3000)
npm run build        # Production build → dist/
```

### Startup Sequence (Backend)
1. Loguru logging configured
2. Database connection verified (Claude DB)
3. RCSI enabled on Rep_data
4. SQLAlchemy auto-creates model tables
5. Super admin account created if missing
6. Default permissions seeded
7. Hanging jobs marked as failed
8. TempDB cleanup thread started (every 5 min)

---

## 10. CONFIGURATION

### Backend (`backend/app/core/config.py`)

| Setting | Value | Notes |
|---------|-------|-------|
| DB_SERVER | HOPC560 | SQL Server instance |
| DB_NAME | Claude | System database |
| DATA_DB_NAME | Rep_data | Business database |
| DB_USERNAME | sa | SQL auth |
| DB_PASSWORD | vrl@55555 | **CHANGE IN PRODUCTION** |
| DB_POOL_SIZE | 10 | Connection pool |
| DB_MAX_OVERFLOW | 15 | Max extra connections |
| DB_POOL_RECYCLE | 180 | Recycle every 3 min |
| JWT_SECRET_KEY | (hardcoded) | **CHANGE IN PRODUCTION** |
| JWT_ACCESS_TOKEN_EXPIRE | 480 min | 8 hours |
| MAX_LOGIN_ATTEMPTS | 5 | Then 30 min lockout |
| MAX_UPLOAD_SIZE_MB | 100 | File upload limit |
| UPLOAD_CHUNK_SIZE | 2000 | Rows per batch |

### Frontend (`.env`)
```
VITE_API_URL=/api/v1
```

---

## 11. API ENDPOINTS SUMMARY (193+)

| Module | Prefix | Endpoints | Key Operations |
|--------|--------|-----------|---------------|
| Auth | /auth | 5 | login, refresh, me, profile, change-password |
| Users | /users | 6 | CRUD, unlock |
| Roles | /roles | 5 | CRUD, assign permissions |
| RLS | /rls | 12 | store/region/column access |
| Tables | /tables | 15+ | CRUD, schema, data, distinct values |
| Allocations | /allocations | 9 | run, override, approve, execute |
| MSA | /msa | 20+ | columns, filter, calculate, results, jobs |
| Contribution | /contrib | 20+ | presets, mappings, execute, review |
| BDC | /bdc | 7 | upload, save, download, sequences |
| Grid Builder | /grid-builder | 12+ | build, calculations, save |
| Trends | /trends | 15+ | dashboard, upload, review, admin |
| Upload/Export | /upload, /export | 10+ | jobs, status, download |
| Reports | /reports | 3 | pending allocation |
| Checklist | /checklist | 5 | items, available-tables, add, reorder |
| Listing | /listing | 4 | config, generate, preview |
| Settings | /settings | 8 | app config, table settings |
| Dashboard | /dashboard | 3 | stats, recent activity |
| Audit | /audit | 4 | logs, table history, user history |
| SLOC Validation | /sloc-validation | 5 | settings, validate |
| Maintenance | /maintenance | 2 | tempdb cleanup (superadmin) |

---

## 12. FRONTEND PAGES (38)

### Navigation Structure

```
Dashboard
Allocations
  +-- List / New / Detail
Data Management
  +-- All Tables / Create Table / Upload Data / Export Data / Jobs Dashboard / Data Editor
Data Preparation
  +-- MSA Stock Calculation
  +-- BDC Creation
  +-- Grid Builder
  +-- Lookup Art Master
  +-- Listing
Contribution %
  +-- Presets / Mappings / Execute / Review
Trends
  +-- Dashboard / Upload / Review
Reports
  +-- Pending Allocation
Data Validation
  +-- Store SLOC Validation / Data Checklist
Settings (Admin)
  +-- App Settings / Table Management / Users / Roles / RLS / Audit Logs
```

---

## 13. CLAUDE CODE CHAT HISTORY

All development was done using Claude Code CLI. Key sessions covered:

### Phase 1: Foundation
- Project scaffolding (FastAPI + React + Vite)
- SQLAlchemy model design (RBAC, RLS, Retail, Audit)
- JWT authentication with bcrypt password hashing
- Permission system with 50+ granular codes

### Phase 2: Core Features
- Dynamic table management (create, alter, schema introspection)
- Bulk upsert engine with audit logging
- Background job processing (upload, export)
- AG Grid Enterprise integration

### Phase 3: Data Preparation
- MSA Stock Calculation (9-step algorithm)
- BDC Creation with sequence tracking
- Grid Builder with dynamic lookups
- Contribution % analysis (presets, mappings, execution)
- Listing generation (cross-join stores x MSA gen-arts)

### Phase 4: Visualization & Validation
- Trend Dashboard with Recharts
- Store SLOC Validation
- Data Checklist with freshness tracking
- Pending Allocation reports

### Phase 5: Security & Polish
- RBAC (5-phase implementation)
- Row-Level Security (store, region, column)
- Global UI standardization (Tailwind)
- TempDB cleanup service
- Performance optimizations (RCSI, NOLOCK, partition stats)

### Recent Changes (April 2026)
- MSA tables renamed: cl_msa → ARS_MSA_TOTAL, cl_generated_color → ARS_MSA_GEN_ART, cl_color_variant → ARS_MSA_VAR_ART
- MSA table schema simplified to id + sequence_id (dynamic columns)
- ST_CD renamed to RDC in MSA output
- Auto-create tables with IDENTITY detection
- Checklist performance fix (partition stats vs COUNT(*))
- listing.py import fix (app.core.database → app.database.session)

---

## 14. PRODUCTION DEPLOYMENT CHECKLIST

- [ ] Change JWT_SECRET_KEY to strong random value (32+ chars)
- [ ] Move DB credentials to environment variables / .env file
- [ ] Set APP_ENV=production, DEBUG=False
- [ ] Configure CORS_ORIGINS for actual domain
- [ ] Set LOG_LEVEL to WARNING or ERROR
- [ ] Change SUPER_ADMIN_PASSWORD
- [ ] Run database creation scripts on production SQL Server
- [ ] Verify MDF/LDF paths match production disk layout
- [ ] Enable SQL Server Agent for maintenance plans
- [ ] Set up database backup schedule
- [ ] Test all API endpoints with Postman/Swagger
- [ ] Verify RLS filters work correctly per role
- [ ] Load test with expected data volumes
- [ ] Set up monitoring for DB connection pool usage
- [ ] Configure firewall rules (port 8000 for API, 3000 for dev)

---

## 15. KNOWN ISSUES & TECHNICAL DEBT

1. **Hardcoded credentials** in config.py — must use env vars in production
2. **TempDB cleanup** drops ## tables older than 10 min — may be too aggressive for long-running reports
3. **Audit log grows indefinitely** — needs periodic archival/purge strategy
4. **No rate limiting** on API endpoints
5. **No WebSocket** for real-time job status — frontend polls every few seconds
6. **Export files** stored locally — should use object storage (S3/Azure Blob) in production
7. **E: drive performance** — if HDD, consider moving databases back to SSD

---

## 16. CONTACT

**Developer:** Santosh Kumar
**System:** ARS v2.0 - Auto Replenishment System
**Repository:** https://github.com/harshalanand/ars
**Server:** HOPC560 (Windows 11 Pro)

---

*Document generated: 2026-04-06*
*Built with Claude Code (Anthropic)*
