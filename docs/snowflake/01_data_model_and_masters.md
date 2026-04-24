# Snowflake Data Model & Masters for ARS

> Complete table catalog for MSA, Grid Builder, and Listing on Snowflake. All DDL is ready to run against `ARS_PROD`. Schemas follow the layout from the [migration README](00_README_Snowflake_Migration.md): `RAW`, `MASTER`, `STAGE`, `MART`, `AUDIT`.

Run everything below under role `ACCOUNTADMIN` or a role with `CREATE TABLE` on each schema. Set context first:

```sql
USE ROLE  ACCOUNTADMIN;
USE DATABASE ARS_PROD;
USE WAREHOUSE WH_CALC;
```

---

## 1. Layer summary

| Schema | Purpose | Retention | Grain |
|---|---|---|---|
| `RAW` | Landed source extracts: ET_STORE_STOCK, ET_STORE_SALES, SAP pulls | 7 days | Per upload file |
| `MASTER` | Slowly-changing dimensions (products, stores, sizes, SLOC, presets) | 30 days | One current version |
| `STAGE` | Intermediate results between pipeline steps | 1 day | Per run |
| `MART` | Published tables consumed by API + UI | 7–14 days | See each table |
| `AUDIT` | Job runs, audit log, checklist | 90 days | Append-only |

---

## 2. RAW — source extracts

### 2.1 `RAW.ET_STORE_STOCK` — warehouse + store stock snapshot

Replaces `Rep_data.dbo.ET_STORE_STOCK`. One row per `(ST_CD × SLOC × GEN_ART_NUMBER × CLR × SZ)` from SAP.

```sql
CREATE OR REPLACE TABLE RAW.ET_STORE_STOCK (
    SNAPSHOT_DT          DATE        NOT NULL,
    ST_CD                VARCHAR(20) NOT NULL,   -- warehouse/RDC or store code
    WERKS                VARCHAR(20),            -- SAP plant (= store code for store stock)
    SLOC                 VARCHAR(10) NOT NULL,   -- SAP storage location (V01_FRESH, V02_RESERVE, …)
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    VAR_ART              NUMBER(18,0),           -- variant (color+size) id
    SZ                   VARCHAR(10),
    STK_Q                NUMBER(18,3) DEFAULT 0,
    LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_FILE            VARCHAR(500)
)
CLUSTER BY (SNAPSHOT_DT, ST_CD);
```

### 2.2 `RAW.ET_STORE_SALES` — sales history feed

```sql
CREATE OR REPLACE TABLE RAW.ET_STORE_SALES (
    SALE_DT              DATE        NOT NULL,
    WERKS                VARCHAR(20) NOT NULL,
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    VAR_ART              NUMBER(18,0),
    SZ                   VARCHAR(10),
    SALE_Q               NUMBER(18,3) DEFAULT 0,
    SALE_VAL             NUMBER(18,2) DEFAULT 0,
    LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_FILE            VARCHAR(500)
)
CLUSTER BY (SALE_DT, WERKS);
```

### 2.3 `RAW.MASTER_ALC_PEND` — pending allocations

Pending (in-transit) allocations; used to subtract from warehouse stock in MSA Step 6.

```sql
CREATE OR REPLACE TABLE RAW.MASTER_ALC_PEND (
    ST_CD                VARCHAR(20) NOT NULL,   -- source warehouse/RDC
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    VAR_ART              NUMBER(18,0),
    SZ                   VARCHAR(10),
    PEND_QTY             NUMBER(18,3) DEFAULT 0,
    AS_OF_DT             DATE        NOT NULL,
    LOAD_TS              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (AS_OF_DT, ST_CD);
```

---

## 3. MASTER — dimensions

### 3.1 `MASTER.VW_MASTER_PRODUCT` (materialized) — product hierarchy

Drives every grid and every listing row. Replaces `vw_master_product` (a SQL Server view). In Snowflake we materialize it as a table refreshed by the upload pipeline (views with many joins are fine, but Grid Builder reads this tens of times per run — materialize).

```sql
CREATE OR REPLACE TABLE MASTER.VW_MASTER_PRODUCT (
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    MAJ_CAT              VARCHAR(40),           -- e.g. M_TEES_HS
    SEG                  VARCHAR(10),           -- APP | GM | SVC | …
    RNG_SEG              VARCHAR(40),
    MACRO_MVGR           VARCHAR(40),
    MICRO_MVGR           VARCHAR(40),
    FAB                  VARCHAR(40),
    M_VND_CD             VARCHAR(20),
    SSN                  VARCHAR(10),
    COL_FAM              VARCHAR(20),
    DESCR                VARCHAR,
    VAR_ART              NUMBER(18,0),
    SZ                   VARCHAR(10),
    UPDATED_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_VMP PRIMARY KEY (GEN_ART_NUMBER, CLR, VAR_ART, SZ)
)
CLUSTER BY (MAJ_CAT, GEN_ART_NUMBER);
```

### 3.2 `MASTER.RETAIL_GEN_ARTICLE` — generic-article master

```sql
CREATE OR REPLACE TABLE MASTER.RETAIL_GEN_ARTICLE (
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL PRIMARY KEY,
    GEN_ART_DESC         VARCHAR,
    MAJ_CAT              VARCHAR(40),
    SEG                  VARCHAR(10),
    RNG_SEG              VARCHAR(40),
    SSN                  VARCHAR(10),
    FAB                  VARCHAR(40),
    M_VND_CD             VARCHAR(20),
    ACTIVE_FLAG          NUMBER(1) DEFAULT 1
);
```

### 3.3 `MASTER.RETAIL_VARIANT_ARTICLE` — colour × size variants

```sql
CREATE OR REPLACE TABLE MASTER.RETAIL_VARIANT_ARTICLE (
    VAR_ART              NUMBER(18,0) NOT NULL PRIMARY KEY,
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    COL_FAM              VARCHAR(20),
    SZ                   VARCHAR(10),
    ACTIVE_FLAG          NUMBER(1) DEFAULT 1
);
```

### 3.4 `MASTER.MASTER_STORE` — store master

```sql
CREATE OR REPLACE TABLE MASTER.MASTER_STORE (
    WERKS                VARCHAR(20) NOT NULL PRIMARY KEY,
    STORE_NAME           VARCHAR,
    RDC                  VARCHAR(20) NOT NULL,    -- servicing warehouse
    ZONE                 VARCHAR(20),
    CITY                 VARCHAR(40),
    CLUSTER_CD           VARCHAR(20),
    ACTIVE_FLAG          NUMBER(1) DEFAULT 1
);
```

### 3.5 `MASTER.MASTER_CONT_SZ` — size contribution master

```sql
CREATE OR REPLACE TABLE MASTER.MASTER_CONT_SZ (
    MAJ_CAT              VARCHAR(40) NOT NULL,
    SZ                   VARCHAR(10) NOT NULL,
    SZ_CONT_PCT          NUMBER(9,6) NOT NULL,    -- 0.0 – 1.0
    CONSTRAINT PK_CS PRIMARY KEY (MAJ_CAT, SZ)
);
```

### 3.6 `MASTER.MASTER_SLOC` — storage location allow/deny list

```sql
CREATE OR REPLACE TABLE MASTER.MASTER_SLOC (
    SLOC                 VARCHAR(10) NOT NULL PRIMARY KEY,
    SLOC_DESC            VARCHAR,
    INCLUDE_IN_MSA       NUMBER(1) DEFAULT 1,
    SEG                  VARCHAR(10)
);
```

### 3.7 `MASTER.MASTER_GEN_ART_SALE` — per-option sale reference

Used by Listing Part 3.5b.

```sql
CREATE OR REPLACE TABLE MASTER.MASTER_GEN_ART_SALE (
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    AUTO_SALE_D          NUMBER(12,4),             -- auto daily sale
    AS_OF_DT             DATE,
    CONSTRAINT PK_MGAS PRIMARY KEY (GEN_ART_NUMBER, CLR)
);
```

### 3.8 `MASTER.MASTER_GEN_ART_AGE` — article age days

Used by Listing Part 3.5c.

```sql
CREATE OR REPLACE TABLE MASTER.MASTER_GEN_ART_AGE (
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL PRIMARY KEY,
    AGE_D                NUMBER(10)                -- days since first listing
);
```

### 3.9 `MASTER.CONT_PRESETS` — tunable presets per MAJ_CAT

```sql
CREATE OR REPLACE TABLE MASTER.CONT_PRESETS (
    MAJ_CAT              VARCHAR(40) NOT NULL,
    STOCK_PCT            NUMBER(9,6) DEFAULT 0.60,   -- Stock%
    EXCESS_X             NUMBER(9,6) DEFAULT 2.00,   -- Excess×
    HOLD_D               NUMBER(5)    DEFAULT 15,
    AGE_D_THRESHOLD      NUMBER(5)    DEFAULT 15,
    REQ_W                NUMBER(9,6) DEFAULT 0.40,   -- store-ranking req weight
    FILL_W               NUMBER(9,6) DEFAULT 0.60,
    ACS_D_FALLBACK       NUMBER(9,4) DEFAULT 18,
    MIN_SZ               NUMBER(5)    DEFAULT 3,
    ACTIVE_FLAG          NUMBER(1)    DEFAULT 1,
    UPDATED_TS           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CP PRIMARY KEY (MAJ_CAT)
);
```

### 3.10 `MASTER.ARS_GRID_BUILDER` — grid configuration

Controls what grids Grid Builder emits. Drives the dynamic loop in doc 03.

```sql
CREATE OR REPLACE TABLE MASTER.ARS_GRID_BUILDER (
    GRID_ID              NUMBER IDENTITY(1,1) PRIMARY KEY,
    GRID_NAME            VARCHAR(40) NOT NULL UNIQUE,      -- MJ, MJ_FAB, MJ_VAR_ART …
    HIERARCHY_COLUMNS    ARRAY       NOT NULL,             -- ["WERKS","MAJ_CAT","FAB"]
    GRID_GROUP           VARCHAR(20) NOT NULL,             -- 'Primary' | 'Secondary'
    STATUS               VARCHAR(10) NOT NULL,             -- 'ACTIVE' | 'INACTIVE'
    SEQ                  NUMBER(5)   NOT NULL,             -- 10, 20, 30…
    USE_FOR_OPT_SALE     NUMBER(1)   DEFAULT 0,            -- 1 on grid feeding PER_OPT_SALE
    OUTPUT_PREFIX        VARCHAR(20)                       -- e.g. "FAB" → FAB_STK_TTL
);
```

Seed it (matches current ARS set):

```sql
INSERT INTO MASTER.ARS_GRID_BUILDER
    (GRID_NAME, HIERARCHY_COLUMNS, GRID_GROUP, STATUS, SEQ, USE_FOR_OPT_SALE, OUTPUT_PREFIX)
SELECT * FROM VALUES
    ('MJ',             ARRAY_CONSTRUCT('WERKS','MAJ_CAT'),                                                     'Primary',   'ACTIVE', 10, 0, 'MJ'),
    ('MJ_RNG_SEG',     ARRAY_CONSTRUCT('WERKS','MAJ_CAT','RNG_SEG'),                                           'Secondary', 'ACTIVE', 20, 1, 'RNG_SEG'),
    ('MJ_MACRO_MVGR',  ARRAY_CONSTRUCT('WERKS','MAJ_CAT','MACRO_MVGR'),                                        'Secondary', 'ACTIVE', 22, 0, 'MACRO_MVGR'),
    ('MJ_MICRO_MVGR',  ARRAY_CONSTRUCT('WERKS','MAJ_CAT','MICRO_MVGR'),                                        'Secondary', 'ACTIVE', 24, 0, 'MICRO_MVGR'),
    ('MJ_FAB',         ARRAY_CONSTRUCT('WERKS','MAJ_CAT','FAB'),                                               'Secondary', 'ACTIVE', 30, 0, 'FAB'),
    ('MJ_CLR',         ARRAY_CONSTRUCT('WERKS','MAJ_CAT','CLR'),                                               'Secondary', 'ACTIVE', 40, 0, 'CLR'),
    ('MJ_M_VND_CD',    ARRAY_CONSTRUCT('WERKS','MAJ_CAT','M_VND_CD'),                                          'Secondary', 'ACTIVE', 50, 0, 'M_VND_CD'),
    ('MJ_VAR_ART',     ARRAY_CONSTRUCT('WERKS','MAJ_CAT','GEN_ART_NUMBER','CLR','VAR_ART','SZ'),               'Primary',   'ACTIVE', 99, 0, 'VAR');
```

### 3.11 `MASTER.ARS_GRID_HIERARCHY` — runtime registry

Populated by Grid Builder at run time; read by Listing Part 7 to know which `GH_*` / `H_*` flag columns to set.

```sql
CREATE OR REPLACE TABLE MASTER.ARS_GRID_HIERARCHY (
    GRID_NAME            VARCHAR(40) NOT NULL,
    LEVEL_COL            VARCHAR(40) NOT NULL,        -- e.g. FAB, CLR, M_VND_CD
    GROUP_KIND           VARCHAR(20),                 -- 'Primary' | 'Secondary'
    OUTPUT_PREFIX        VARCHAR(20),
    BUILT_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_GH PRIMARY KEY (GRID_NAME, LEVEL_COL)
);
```

---

## 4. MART — outputs

### 4.1 MSA outputs (doc 02)

```sql
CREATE OR REPLACE TABLE MART.ARS_MSA_TOTAL (
    RDC                  VARCHAR(20) NOT NULL,        -- renamed from ST_CD (Apr 2026)
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40),
    VAR_ART              NUMBER(18,0),
    SZ                   VARCHAR(10),
    MAJ_CAT              VARCHAR(40),
    SEG                  VARCHAR(10),
    -- pivot columns, one per distinct SLOC (populated dynamically)
    -- STK_QTY = sum across all pivoted SLOC columns
    STK_QTY              NUMBER(18,3) DEFAULT 0,
    BUILT_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (RDC, MAJ_CAT);

CREATE OR REPLACE TABLE MART.ARS_MSA_GEN_ART (
    RDC                  VARCHAR(20) NOT NULL,
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40) NOT NULL,
    MAJ_CAT              VARCHAR(40),
    SEG                  VARCHAR(10),
    STK_QTY              NUMBER(18,3) DEFAULT 0,
    PEND_QTY             NUMBER(18,3) DEFAULT 0,
    FNL_Q                NUMBER(18,3) DEFAULT 0,      -- MAX(STK - PEND, 0)
    BUILT_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_MSA_GEN PRIMARY KEY (RDC, GEN_ART_NUMBER, CLR)
)
CLUSTER BY (RDC, MAJ_CAT);

CREATE OR REPLACE TABLE MART.ARS_MSA_VAR_ART (
    RDC                  VARCHAR(20) NOT NULL,
    GEN_ART_NUMBER       NUMBER(18,0) NOT NULL,
    CLR                  VARCHAR(40) NOT NULL,
    VAR_ART              NUMBER(18,0) NOT NULL,
    SZ                   VARCHAR(10)  NOT NULL,
    MAJ_CAT              VARCHAR(40),
    SEG                  VARCHAR(10),
    STK_QTY              NUMBER(18,3) DEFAULT 0,
    PEND_QTY             NUMBER(18,3) DEFAULT 0,
    FNL_Q                NUMBER(18,3) DEFAULT 0,
    BUILT_TS             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_MSA_VAR PRIMARY KEY (RDC, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
)
CLUSTER BY (RDC, MAJ_CAT);
```

### 4.2 Grid outputs (doc 03)

Grid Builder produces one table per active grid, named `MART.ARS_GRID_<grid_name>`. Columns follow the pattern:

```
<hierarchy columns>,
<prefix>_STK_TTL,
<prefix>_STR,
<prefix>_CONT,
<prefix>_MBQ,
<prefix>_OPT_CNT,
<prefix>_DISP_Q
```

Example for grid `MJ_FAB` (hierarchy `["WERKS","MAJ_CAT","FAB"]`, prefix `FAB`):

```sql
CREATE OR REPLACE TABLE MART.ARS_GRID_MJ_FAB (
    WERKS        VARCHAR(20) NOT NULL,
    MAJ_CAT      VARCHAR(40) NOT NULL,
    FAB          VARCHAR(40),
    FAB_STK_TTL  NUMBER(18,3) DEFAULT 0,
    FAB_STR      NUMBER(10)   DEFAULT 0,
    FAB_CONT     NUMBER(9,6)  DEFAULT 0,
    FAB_MBQ      NUMBER(18,3) DEFAULT 0,
    FAB_OPT_CNT  NUMBER(10)   DEFAULT 0,
    FAB_DISP_Q   NUMBER(18,3) DEFAULT 0,
    BUILT_TS     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (WERKS, MAJ_CAT);
```

Variant grid `MART.ARS_GRID_MJ_VAR_ART` carries `WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ` plus the six `VAR_*` metrics; this is the seed for Listing Part 1.

### 4.3 Listing outputs (doc 04)

```sql
CREATE OR REPLACE TABLE MART.ARS_LISTING (
    WERKS            VARCHAR(20) NOT NULL,
    MAJ_CAT          VARCHAR(40) NOT NULL,
    GEN_ART_NUMBER   NUMBER(18,0) NOT NULL,
    CLR              VARCHAR(40)  NOT NULL,
    VAR_ART          NUMBER(18,0),
    SZ               VARCHAR(10),
    RDC              VARCHAR(20),

    -- stock & sale
    STK_TTL          NUMBER(18,3) DEFAULT 0,
    ACS_D            NUMBER(12,4) DEFAULT 0,
    ALC_D            NUMBER(12,4) DEFAULT 0,
    AGE_D            NUMBER(10),
    IS_NEW           NUMBER(1)    DEFAULT 0,

    -- MSA
    MSA_FNL_Q        NUMBER(18,3) DEFAULT 0,
    VAR_COUNT        NUMBER(10)   DEFAULT 0,
    VAR_FNL_COUNT    NUMBER(10)   DEFAULT 0,

    -- classification
    OPT_TYPE         VARCHAR(8),      -- MIX | RL | TBC | TBL
    FINAL_OPT_TYPE   VARCHAR(8),      -- post-alloc relabel (NL etc.)

    -- product attrs (from vw_master_product)
    SEG              VARCHAR(10),
    RNG_SEG          VARCHAR(40),
    MACRO_MVGR       VARCHAR(40),
    MICRO_MVGR       VARCHAR(40),
    FAB              VARCHAR(40),
    M_VND_CD         VARCHAR(20),
    COL_FAM          VARCHAR(20),
    SSN              VARCHAR(10),

    -- Part 4a/4c outputs
    PER_OPT_SALE     NUMBER(12,4),
    OPT_MBQ          NUMBER(18,3),
    OPT_REQ          NUMBER(18,3),
    OPT_MBQ_WH       NUMBER(18,3),
    OPT_REQ_WH       NUMBER(18,3),
    MAX_DAILY_SALE   NUMBER(12,4),
    ART_EXCESS       NUMBER(18,3),

    -- per-grid metrics — joined in Part 4a (one set per ACTIVE grid)
    -- MJ_STK_TTL, MJ_MBQ, MJ_CONT, MJ_OPT_CNT, MJ_STR, MJ_DISP_Q,
    -- FAB_STK_TTL, FAB_MBQ, …  (add columns programmatically at publish time)

    BUILT_TS         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (WERKS, MAJ_CAT);

CREATE OR REPLACE TABLE MART.ARS_LISTING_WORKING LIKE MART.ARS_LISTING;
ALTER TABLE MART.ARS_LISTING_WORKING ADD COLUMN PRI_CT_PCT   NUMBER(9,4);
ALTER TABLE MART.ARS_LISTING_WORKING ADD COLUMN SEC_CT_PCT   NUMBER(9,4);
ALTER TABLE MART.ARS_LISTING_WORKING ADD COLUMN ALLOC_FLAG   NUMBER(1);
ALTER TABLE MART.ARS_LISTING_WORKING ADD COLUMN ALLOC_QTY    NUMBER(18,3) DEFAULT 0;
ALTER TABLE MART.ARS_LISTING_WORKING ADD COLUMN ST_RANK      NUMBER(10);

CREATE OR REPLACE TABLE MART.ARS_STORE_RANKING (
    WERKS           VARCHAR(20) NOT NULL,
    MAJ_CAT         VARCHAR(40) NOT NULL,
    MJ_REQ          NUMBER(18,3),
    MJ_STK_TTL      NUMBER(18,3),
    FILL_PCT        NUMBER(9,4),
    REQ_SCORE       NUMBER(12,4),
    FILL_SCORE      NUMBER(12,4),
    RANK_SCORE      NUMBER(12,4),
    ST_RANK         NUMBER(10),
    BUILT_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SR PRIMARY KEY (WERKS, MAJ_CAT)
);

CREATE OR REPLACE TABLE MART.ARS_ALLOC_WORKING (
    WERKS           VARCHAR(20) NOT NULL,
    MAJ_CAT         VARCHAR(40) NOT NULL,
    GEN_ART_NUMBER  NUMBER(18,0) NOT NULL,
    CLR             VARCHAR(40)  NOT NULL,
    VAR_ART         NUMBER(18,0) NOT NULL,
    SZ              VARCHAR(10)  NOT NULL,
    RDC             VARCHAR(20),
    OPT_TYPE        VARCHAR(8),
    ST_RANK         NUMBER(10),
    MSA_FNL_Q       NUMBER(18,3),
    STK_TTL         NUMBER(18,3),
    SZ_CONT_PCT     NUMBER(9,6),

    -- allocation result
    SHIP_QTY        NUMBER(18,3) DEFAULT 0,
    HOLD_QTY        NUMBER(18,3) DEFAULT 0,
    POOL_CONSUMED   NUMBER(18,3) DEFAULT 0,
    ALLOC_STATUS    VARCHAR(20),      -- ALLOCATED | PARTIAL | SKIPPED | INELIGIBLE
    SKIP_REASON     VARCHAR(200),
    ALLOC_ROUND     NUMBER(5),
    WAVE            VARCHAR(10),      -- PRI_100 / PRI_80 / SEC_100 / SEC_80
    FOCUS_FLAG      VARCHAR(10),

    BUILT_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (WERKS, MAJ_CAT);
```

### 4.4 Calculation cascade tables (Listing inputs)

```sql
CREATE OR REPLACE TABLE MART.ARS_CALC_ST_MAJ_CAT (
    WERKS           VARCHAR(20) NOT NULL,
    MAJ_CAT         VARCHAR(40) NOT NULL,
    ACS_D           NUMBER(12,4) DEFAULT 0,   -- avg daily sale
    ALC_D           NUMBER(12,4) DEFAULT 0,   -- allocation period days
    FOCUS_FLAG      VARCHAR(10),
    MJ_REQ          NUMBER(18,3),
    BUILT_TS        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CS PRIMARY KEY (WERKS, MAJ_CAT)
);

CREATE OR REPLACE TABLE MART.ARS_CALC_ST_GEN_ART (
    WERKS           VARCHAR(20) NOT NULL,
    GEN_ART_NUMBER  NUMBER(18,0) NOT NULL,
    CLR             VARCHAR(40)  NOT NULL,
    I_ROD           NUMBER(10),       -- iteration rounds per option
    AUTO_SALE_D     NUMBER(12,4),
    CONSTRAINT PK_CGA PRIMARY KEY (WERKS, GEN_ART_NUMBER, CLR)
);
```

---

## 5. AUDIT — ops

### 5.1 Job runs

```sql
CREATE OR REPLACE TABLE AUDIT.JOB_RUNS (
    JOB_ID          NUMBER IDENTITY(1,1) PRIMARY KEY,
    JOB_NAME        VARCHAR(40) NOT NULL,      -- MSA | GRID | LISTING | ALLOC
    STATUS          VARCHAR(20),               -- STARTED | SUCCESS | FAILED
    STARTED_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FINISHED_TS     TIMESTAMP_NTZ,
    ROWS_AFFECTED   NUMBER,
    ERR_MESSAGE     VARCHAR,
    RUN_USER        VARCHAR(40),
    RUN_PARAMS      VARIANT                    -- JSON of request body
);
```

### 5.2 Checklist (daily readiness)

```sql
CREATE OR REPLACE TABLE AUDIT.ARS_CHECKLIST (
    CHECK_DT        DATE NOT NULL,
    CHECK_NAME      VARCHAR(60) NOT NULL,      -- MSA_UPTO_DATE, STOCK_LOADED, etc.
    STATUS          VARCHAR(10) NOT NULL,      -- PASS | FAIL | WARN
    DETAIL          VARCHAR,
    CHECKED_TS      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CKL PRIMARY KEY (CHECK_DT, CHECK_NAME)
);
```

---

## 6. Row-access policy (replaces Rep_data RLS)

```sql
-- Example: only ARS_APP can see all stores; store-scoped user sees only its WERKS
CREATE OR REPLACE ROW ACCESS POLICY MART.RLS_BY_WERKS
AS (werks VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('ARS_APP','ACCOUNTADMIN')
    OR werks IN (
        SELECT ASSIGNED_WERKS FROM MASTER.USER_STORE_SCOPE
        WHERE SF_USER = CURRENT_USER()
    );

ALTER TABLE MART.ARS_LISTING         ADD ROW ACCESS POLICY MART.RLS_BY_WERKS ON (WERKS);
ALTER TABLE MART.ARS_LISTING_WORKING ADD ROW ACCESS POLICY MART.RLS_BY_WERKS ON (WERKS);
ALTER TABLE MART.ARS_ALLOC_WORKING   ADD ROW ACCESS POLICY MART.RLS_BY_WERKS ON (WERKS);
```

---

## 7. Seeding from Azure SQL (one-time)

Python ETL, pandas chunked read from SQL Server + `write_pandas` to Snowflake:

```python
CHUNK = 200_000
for table, schema in [
    ("VW_MASTER_PRODUCT", "MASTER"),
    ("MASTER_CONT_SZ",     "MASTER"),
    ("MASTER_STORE",       "MASTER"),
    ("ARS_GRID_BUILDER",   "MASTER"),
    ("CONT_PRESETS",       "MASTER"),
]:
    for chunk in pd.read_sql(f"SELECT * FROM {table}", sql_server_engine, chunksize=CHUNK):
        write_pandas(sf_conn, chunk, table_name=table, schema=schema,
                     quote_identifiers=False, auto_create_table=False)
```

For transactional tables (`ET_STORE_STOCK`, `ET_STORE_SALES`, `MASTER_ALC_PEND`) prefer **Snowflake COPY INTO** from CSVs exported via `bcp` or Azure Data Factory — it's 10–20× faster than pandas at scale.

---

## 8. Verification

```sql
-- Table count per schema
SELECT TABLE_SCHEMA, COUNT(*) AS tables
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'ARS_PROD'
GROUP BY TABLE_SCHEMA;

-- Row counts after seed
SELECT 'VW_MASTER_PRODUCT' AS tbl, COUNT(*) FROM MASTER.VW_MASTER_PRODUCT
UNION ALL SELECT 'MASTER_STORE',      COUNT(*) FROM MASTER.MASTER_STORE
UNION ALL SELECT 'MASTER_CONT_SZ',    COUNT(*) FROM MASTER.MASTER_CONT_SZ
UNION ALL SELECT 'ARS_GRID_BUILDER',  COUNT(*) FROM MASTER.ARS_GRID_BUILDER;

-- Expected grid names after seed (should return 8)
SELECT GRID_NAME, GRID_GROUP, STATUS, SEQ FROM MASTER.ARS_GRID_BUILDER ORDER BY SEQ;
```

Move on to [02_msa_stock_calculation.md](02_msa_stock_calculation.md).
