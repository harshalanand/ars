"""
Grid Builder — Pre-Grid Calculations
======================================
All calculations done in ARS_CALC_ST_MAJ_CAT (master tables NOT modified).

PRIORITY RULE for CO_MAJ_CAT vs ST_MAJ_CAT:
  - CO_MAJ_CAT = company level → applies to ALL stores for that MAJ_CAT
  - ST_MAJ_CAT = store level   → applies to THAT store only
  - If BOTH have values → take MAX
  - CO values are applied first, then ST overrides/merges

STEPS:
  1. Create calc table (copy from ST_MAJ_CAT)
  2. Merge CO_MAJ_CAT values (apply to all stores)
  3. Apply defaults: LISTING, I_ROD, MANUAL_MBQ, growth rates, CLR/DPN
  4. Calculate SAL_D (total sale days)
  5. Calculate SAL_PD (per day sale)

To change column names or logic → edit below and restart backend.
"""
from typing import List, Dict, Any
from sqlalchemy import text
from loguru import logger


# ==========================================================================
# TABLE NAMES — Change here if your table names differ
# ==========================================================================
TABLES = {
    "ST_MAJ":   "Master_ALC_INPUT_ST_MAJ_CAT",
    "CO_MAJ":   "Master_ALC_INPUT_CO_MAJ_CAT",
    "ST_MAST":  "Master_ALC_INPUT_ST_MASTER",
    "CALC":     "ARS_CALC_ST_MAJ_CAT",
}

# ==========================================================================
# COLUMN NAMES — Change here if your column names differ
# ==========================================================================
# From ST_MASTER
COL_INT_DAYS    = "INT_DAYS"
COL_PRD_DAYS    = "PRD_DAYS"
COL_SL_CVR      = "SL_CVR"

# Sale calculation
COL_CM_SAL_Q    = "CM_SAL_Q"
COL_CM_REM_D    = "CM_REM_D"
COL_NM_SAL_D    = "NM_SAL_Q"
COL_NM_REM_D    = "NM_REM_D"

# Columns that need CO/ST merge + defaults
COL_LISTING     = "LISTING"
COL_I_ROD       = "I_ROD"
COL_MANUAL_MBQ  = "MANUAL_MBQ"
COL_DISP_GR_DGR = "DISP_GR_DGR"
COL_LW_ACT_GR   = "LW_ACT_SL_GR_DGR"
COL_BGT_SL_GR   = "BGT_SL_GR_DGR"
COL_CLR_MIN     = "CLR_MIN"
COL_CLR_MAX     = "CLR_MAX"
COL_DPN         = "DPN"
COL_DISP_Q      = "DISP_Q"

# Output columns
COL_SAL_D       = "SAL_D"
COL_SAL_PD      = "SAL_PD"
COL_SRC         = "SALE_COVER_SRC"


# ==========================================================================
# HELPERS
# ==========================================================================
def _run(conn, sql, params=None):
    conn.execute(text(sql) if isinstance(sql, str) else sql, params or {})
    conn.commit()

def _exists(conn, tbl):
    return conn.execute(text(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :t"
    ), {"t": tbl}).scalar() > 0

def _col_exists(conn, tbl, col):
    return conn.execute(text(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t AND COLUMN_NAME = :c"
    ), {"t": tbl, "c": col}).scalar() > 0

def _ensure_col(conn, tbl, col, dtype="FLOAT"):
    if not _col_exists(conn, tbl, col):
        try: _run(conn, f"ALTER TABLE [{tbl}] ADD [{col}] {dtype} NULL")
        except: pass


# ==========================================================================
# PRIMARY KEYS
# ==========================================================================
REQUIRED_PKS = {
    "Master_ALC_INPUT_ST_MASTER":  ["ST_CD"],
    "Master_ALC_INPUT_ST_MAJ_CAT": ["ST_CD", "MAJ_CAT"],
    "Master_ALC_INPUT_CO_MAJ_CAT": ["MAJ_CAT"],
}

def ensure_primary_keys(conn) -> List[str]:
    logs = []
    for tbl, pk_cols in REQUIRED_PKS.items():
        if not _exists(conn, tbl): continue
        has_pk = conn.execute(text(
            "SELECT COUNT(*) FROM sys.key_constraints WHERE type='PK' AND OBJECT_NAME(parent_object_id)=:t"
        ), {"t": tbl}).scalar() > 0
        if has_pk: continue
        pk_list = ", ".join(f"[{c}]" for c in pk_cols)
        try:
            for c in pk_cols:
                try: _run(conn, f"ALTER TABLE [{tbl}] ALTER COLUMN [{c}] NVARCHAR(255) NOT NULL")
                except: pass
            _run(conn, f"ALTER TABLE [{tbl}] ADD CONSTRAINT [PK_{tbl}] PRIMARY KEY ({pk_list})")
            logs.append(f"Added PK ({', '.join(pk_cols)}) to {tbl}")
        except Exception as e:
            logs.append(f"PK failed for {tbl}: {str(e)[:80]}")
    return logs


# ==========================================================================
# STEP 1: CREATE CALC TABLE
# ==========================================================================
def _step_create_calc(conn, steps):
    """Copy ST_MAJ_CAT → ARS_CALC_ST_MAJ_CAT."""
    SRC  = TABLES["ST_MAJ"]
    CALC = TABLES["CALC"]
    if not _exists(conn, SRC):
        steps.append({"step": "Create calc table", "detail": f"{SRC} not found", "status": "skip"})
        return False
    _run(conn, f"IF OBJECT_ID('{CALC}','U') IS NOT NULL DROP TABLE [{CALC}]")
    _run(conn, f"SELECT * INTO [{CALC}] FROM [{SRC}] WITH (NOLOCK)")

    # Ensure all output columns exist in calc table
    _ensure_col(conn, CALC, COL_SAL_D)
    _ensure_col(conn, CALC, COL_SAL_PD)
    _ensure_col(conn, CALC, COL_SRC, "NVARCHAR(50)")

    cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}]")).scalar()
    steps.append({"step": "Create calc table", "detail": f"{cnt} rows from {SRC} → {CALC} (+ {COL_SAL_D}, {COL_SAL_PD})", "status": "ok"})
    return True


# ==========================================================================
# STEP 2: MERGE CO_MAJ_CAT VALUES
# ==========================================================================
# For columns where CO_MAJ_CAT has values → apply to ALL stores.
# If ST_MAJ_CAT also has values → take MAX of both.

# Columns to merge from CO_MAJ_CAT with MAX logic
# ==========================================================================
# MERGE RULES — Easy to change per column
# ==========================================================================
# "co_override"  = CO always wins. Applies to all stores.
# "max"          = MAX of ST and CO values. If only one has data, use that.
#
# Add/remove/change rules here:
MERGE_RULES = {
    #  Column               Rule            Note
    COL_LISTING:          "co_override",  # CO top priority. If CO says N → all stores get 0
    COL_I_ROD:            "co_override",  # CO top priority. If CO has data → all stores same
    COL_MANUAL_MBQ:       "max",          # MAX of ST vs CO
    COL_DISP_GR_DGR:      "max",          # MAX of ST vs CO
    COL_LW_ACT_GR:        "max",          # MAX of ST vs CO
    COL_BGT_SL_GR:        "max",          # MAX of ST vs CO
    COL_CLR_MIN:          "max",          # MAX of ST vs CO
    COL_CLR_MAX:          "max",          # MAX of ST vs CO
    COL_DPN:              "max",          # MAX of ST vs CO
}


def _step_merge_co_values(conn, steps):
    """
    Merge CO_MAJ_CAT values into calc table using MERGE_RULES above.

    co_override = CO always wins → applies to ALL stores for that MAJ_CAT
    max         = MAX(ST value, CO value) → whichever is higher
    """
    CALC   = TABLES["CALC"]
    CO_MAJ = TABLES["CO_MAJ"]
    if not _exists(conn, CO_MAJ):
        steps.append({"step": "Merge CO_MAJ_CAT", "detail": "Table not found", "status": "skip"})
        return

    merged = []
    for col, rule in MERGE_RULES.items():
        if not _col_exists(conn, CO_MAJ, col) or not _col_exists(conn, CALC, col):
            continue

        try:
            if rule == "co_override":
                # ── CO ALWAYS WINS: if CO has data → overwrite ALL stores ──
                _run(conn, f"""
                    UPDATE C SET C.[{col}] = CO.[{col}]
                    FROM [{CALC}] C
                    INNER JOIN [{CO_MAJ}] CO WITH (NOLOCK) ON C.[MAJ_CAT] = CO.[MAJ_CAT]
                    WHERE CO.[{col}] IS NOT NULL
                """)
                merged.append(f"{col} (CO override)")

            elif rule == "max":
                # ── MAX: take higher of ST and CO ──────────────────────────
                _run(conn, f"""
                    UPDATE C SET C.[{col}] =
                        CASE
                            WHEN ISNULL(C.[{col}],0) >= ISNULL(CO.[{col}],0) THEN C.[{col}]
                            ELSE CO.[{col}]
                        END
                    FROM [{CALC}] C
                    INNER JOIN [{CO_MAJ}] CO WITH (NOLOCK) ON C.[MAJ_CAT] = CO.[MAJ_CAT]
                    WHERE CO.[{col}] IS NOT NULL AND CO.[{col}] > 0
                """)
                merged.append(f"{col} (MAX)")

        except Exception as e:
            steps.append({"step": f"Merge CO {col}", "detail": str(e)[:100], "status": "error"})

    steps.append({"step": "Merge CO_MAJ_CAT", "detail": f"{len(merged)} cols: {', '.join(merged)}", "status": "ok"})


# ==========================================================================
# STEP 3: APPLY DEFAULTS
# ==========================================================================
def _step_defaults(conn, steps):
    """Apply default values for blank/null columns."""
    CALC = TABLES["CALC"]
    applied = []

    # 1. LISTING: blank or 'Y' → 1, 'N' → 0
    if _col_exists(conn, CALC, COL_LISTING):
        try:
            _run(conn, f"""
                UPDATE [{CALC}] SET [{COL_LISTING}] =
                    CASE
                        WHEN [{COL_LISTING}] IS NULL OR LTRIM(RTRIM(CAST([{COL_LISTING}] AS NVARCHAR(10)))) = '' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(CAST([{COL_LISTING}] AS NVARCHAR(10))))) = 'Y' THEN 1
                        WHEN UPPER(LTRIM(RTRIM(CAST([{COL_LISTING}] AS NVARCHAR(10))))) = 'N' THEN 0
                        WHEN ISNUMERIC(CAST([{COL_LISTING}] AS NVARCHAR(10))) = 1 THEN CAST([{COL_LISTING}] AS INT)
                        ELSE 1
                    END
            """)
            applied.append(f"{COL_LISTING}: blank/Y→1, N→0")
        except Exception as e:
            steps.append({"step": f"Default {COL_LISTING}", "detail": str(e)[:100], "status": "error"})

    # 2. I_ROD: blank or 0 → default 1
    if _col_exists(conn, CALC, COL_I_ROD):
        try:
            _run(conn, f"UPDATE [{CALC}] SET [{COL_I_ROD}] = 1 WHERE [{COL_I_ROD}] IS NULL OR [{COL_I_ROD}] = 0")
            applied.append(f"{COL_I_ROD}: null/0→1")
        except: pass

    # 3. Growth rates: DISP_GR_DGR, LW_ACT_SL_GR_DGR, BGT_SL_GR_DGR → default 1 if null/blank
    for col in [COL_DISP_GR_DGR, COL_LW_ACT_GR, COL_BGT_SL_GR]:
        if _col_exists(conn, CALC, col):
            try:
                _run(conn, f"UPDATE [{CALC}] SET [{col}] = 1 WHERE [{col}] IS NULL OR [{col}] = 0")
                applied.append(f"{col}: null/0→1")
            except: pass

    # 4. MANUAL_MBQ: keep only >0 values, null the rest
    if _col_exists(conn, CALC, COL_MANUAL_MBQ):
        try:
            _run(conn, f"UPDATE [{CALC}] SET [{COL_MANUAL_MBQ}] = NULL WHERE [{COL_MANUAL_MBQ}] IS NULL OR [{COL_MANUAL_MBQ}] <= 0")
            applied.append(f"{COL_MANUAL_MBQ}: <=0→NULL")
        except: pass

    steps.append({"step": "Apply defaults", "detail": "; ".join(applied), "status": "ok"})


# ==========================================================================
# STEP 4: CALCULATE SAL_D (Total Sale Days)
# ==========================================================================
def _step_sal_d(conn, steps):
    """SAL_D = INT_DAYS + PRD_DAYS + SL_CVR (priority: ST_MAJ > CO_MAJ > ST_MASTER)."""
    CALC    = TABLES["CALC"]
    ST_MAST = TABLES["ST_MAST"]
    CO_MAJ  = TABLES["CO_MAJ"]

    if not _exists(conn, ST_MAST):
        steps.append({"step": "SAL_D", "detail": f"{ST_MAST} not found", "status": "skip"})
        return

    _ensure_col(conn, CALC, COL_SAL_D)
    _ensure_col(conn, CALC, COL_SRC, "NVARCHAR(50)")

    # Priority 3: ST_MASTER (base for all)
    try:
        _run(conn, f"""
            UPDATE C SET C.[{COL_SRC}]='ST_MASTER',
                C.[{COL_SAL_D}] = ISNULL(S.[{COL_INT_DAYS}],0)+ISNULL(S.[{COL_PRD_DAYS}],0)+ISNULL(S.[{COL_SL_CVR}],0)
            FROM [{CALC}] C
            INNER JOIN [{ST_MAST}] S WITH (NOLOCK) ON C.[ST_CD]=S.[ST_CD]
        """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [{COL_SAL_D}]>0")).scalar()
        steps.append({"step": "SAL_D (ST_MASTER)", "detail": f"{cnt} rows", "status": "ok"})
    except Exception as e:
        steps.append({"step": "SAL_D (ST_MASTER)", "detail": str(e)[:150], "status": "error"})
        return

    # Priority 2: CO_MAJ_CAT override
    if _exists(conn, CO_MAJ) and _col_exists(conn, CO_MAJ, COL_SL_CVR):
        try:
            _run(conn, f"""
                UPDATE C SET C.[{COL_SRC}]='CO_MAJ_CAT',
                    C.[{COL_SAL_D}] = ISNULL(S.[{COL_INT_DAYS}],0)+ISNULL(S.[{COL_PRD_DAYS}],0)+ISNULL(CO.[{COL_SL_CVR}],0)
                FROM [{CALC}] C
                INNER JOIN [{ST_MAST}] S WITH (NOLOCK) ON C.[ST_CD]=S.[ST_CD]
                INNER JOIN [{CO_MAJ}] CO WITH (NOLOCK) ON C.[MAJ_CAT]=CO.[MAJ_CAT]
                WHERE CO.[{COL_SL_CVR}] IS NOT NULL AND CO.[{COL_SL_CVR}] > 0
            """)
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [{COL_SRC}]='CO_MAJ_CAT'")).scalar()
            steps.append({"step": "SAL_D (CO_MAJ_CAT)", "detail": f"Override {cnt} rows", "status": "ok"})
        except Exception as e:
            steps.append({"step": "SAL_D (CO_MAJ_CAT)", "detail": str(e)[:150], "status": "error"})

    # Priority 1: ST_MAJ_CAT own SL_CVR (highest priority)
    if _col_exists(conn, CALC, COL_SL_CVR):
        try:
            _run(conn, f"""
                UPDATE C SET C.[{COL_SRC}]='ST_MAJ_CAT',
                    C.[{COL_SAL_D}] = ISNULL(S.[{COL_INT_DAYS}],0)+ISNULL(S.[{COL_PRD_DAYS}],0)+ISNULL(C.[{COL_SL_CVR}],0)
                FROM [{CALC}] C
                INNER JOIN [{ST_MAST}] S WITH (NOLOCK) ON C.[ST_CD]=S.[ST_CD]
                WHERE C.[{COL_SL_CVR}] IS NOT NULL AND C.[{COL_SL_CVR}] > 0
            """)
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [{COL_SRC}]='ST_MAJ_CAT'")).scalar()
            steps.append({"step": "SAL_D (ST_MAJ_CAT)", "detail": f"Override {cnt} rows", "status": "ok"})
        except Exception as e:
            steps.append({"step": "SAL_D (ST_MAJ_CAT)", "detail": str(e)[:150], "status": "error"})


# ==========================================================================
# STEP 5: CALCULATE SAL_PD (Per Day Sale)
# ==========================================================================
def _step_sal_pd(conn, steps):
    CALC = TABLES["CALC"]
    needed = [COL_CM_SAL_Q, COL_CM_REM_D, COL_NM_SAL_D, COL_NM_REM_D, COL_SAL_D]
    missing = [c for c in needed if not _col_exists(conn, CALC, c)]
    if missing:
        steps.append({"step": "SAL_PD", "detail": f"Missing: {missing}", "status": "skip"})
        return
    _ensure_col(conn, CALC, COL_SAL_PD)
    try:
        _run(conn, f"""
            UPDATE [{CALC}] SET [{COL_SAL_PD}] =
                CASE
                    WHEN ISNULL([{COL_SAL_D}],0)=0 OR ISNULL([{COL_CM_REM_D}],0)=0 THEN 0
                    WHEN [{COL_CM_REM_D}] >= [{COL_SAL_D}] THEN
                        (CAST([{COL_CM_SAL_Q}] AS FLOAT)/[{COL_CM_REM_D}]) / [{COL_SAL_D}]
                    ELSE
                        CASE WHEN ISNULL([{COL_NM_REM_D}],0)=0 THEN
                            (CAST([{COL_CM_SAL_Q}] AS FLOAT)/[{COL_CM_REM_D}]) / [{COL_SAL_D}]
                        ELSE
                            (CAST([{COL_CM_SAL_Q}] AS FLOAT)
                             + (CAST([{COL_NM_SAL_D}] AS FLOAT)/[{COL_NM_REM_D}])
                               * ([{COL_SAL_D}]-[{COL_CM_REM_D}])
                            ) / [{COL_SAL_D}]
                        END
                END
        """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [{COL_SAL_PD}]>0")).scalar()
        steps.append({"step": "SAL_PD", "detail": f"{cnt} rows calculated", "status": "ok"})
    except Exception as e:
        steps.append({"step": "SAL_PD", "detail": str(e)[:150], "status": "error"})


# ==========================================================================
# MAIN: Run all pre-grid calculations
# ==========================================================================
def calculate_per_day_sale(conn) -> List[Dict[str, Any]]:
    """
    Full pre-grid calculation pipeline:
      1. Copy Master → ARS_CALC_ST_MAJ_CAT
      2. Merge CO_MAJ_CAT values (MAX logic)
      3. Apply defaults (LISTING, I_ROD, growth rates)
      4. SAL_D (total sale days)
      5. SAL_PD (per day sale)
    """
    steps = []

    # Ensure PKs
    for msg in ensure_primary_keys(conn):
        steps.append({"step": "Ensure PK", "detail": msg, "status": "ok"})

    # Step 1
    if not _step_create_calc(conn, steps):
        return steps

    # Step 2
    _step_merge_co_values(conn, steps)

    # Step 3
    _step_defaults(conn, steps)

    # Step 4
    _step_sal_d(conn, steps)

    # Step 5
    _step_sal_pd(conn, steps)

    return steps
