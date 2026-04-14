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

from app.utils.db_helpers import (
    run_sql, table_exists, column_exists, ensure_column,
)


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
# HELPERS — delegating to shared db_helpers
# ==========================================================================
_run = run_sql
_exists = table_exists
_col_exists = column_exists
_ensure_col = ensure_column


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
                except Exception: pass
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
    "CONT":               "st_first",     # ST first, CO fallback if ST is NULL/0
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

            elif rule == "st_first":
                # ── ST FIRST: use ST value, CO fallback if ST is NULL/0 ────
                _ensure_col(conn, CALC, col)
                _run(conn, f"""
                    UPDATE C SET C.[{col}] = CO.[{col}]
                    FROM [{CALC}] C
                    INNER JOIN [{CO_MAJ}] CO WITH (NOLOCK) ON C.[MAJ_CAT] = CO.[MAJ_CAT]
                    WHERE (C.[{col}] IS NULL OR TRY_CAST(C.[{col}] AS FLOAT) = 0)
                      AND CO.[{col}] IS NOT NULL AND TRY_CAST(CO.[{col}] AS FLOAT) > 0
                """)
                merged.append(f"{col} (ST first, CO fallback)")

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
        except Exception as e:
            logger.debug(f"Default {COL_I_ROD}: {e}")

    # 3. Growth rates: DISP_GR_DGR, LW_ACT_SL_GR_DGR, BGT_SL_GR_DGR → default 1 if null/blank
    for col in [COL_DISP_GR_DGR, COL_LW_ACT_GR, COL_BGT_SL_GR]:
        if _col_exists(conn, CALC, col):
            try:
                _run(conn, f"UPDATE [{CALC}] SET [{col}] = 1 WHERE [{col}] IS NULL OR [{col}] = 0")
                applied.append(f"{col}: null/0→1")
            except Exception as e:
                logger.debug(f"Default {col}: {e}")

    # 4. MANUAL_MBQ: keep only >0 values, null the rest
    if _col_exists(conn, CALC, COL_MANUAL_MBQ):
        try:
            _run(conn, f"UPDATE [{CALC}] SET [{COL_MANUAL_MBQ}] = NULL WHERE [{COL_MANUAL_MBQ}] IS NULL OR [{COL_MANUAL_MBQ}] <= 0")
            applied.append(f"{COL_MANUAL_MBQ}: <=0→NULL")
        except Exception as e:
            logger.debug(f"Default {COL_MANUAL_MBQ}: {e}")

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
                    WHEN ISNULL([{COL_CM_REM_D}],0)=0 THEN 0
                    WHEN [{COL_CM_REM_D}] >= ISNULL([{COL_SAL_D}],0) THEN
                        CAST([{COL_CM_SAL_Q}] AS FLOAT) / [{COL_CM_REM_D}]
                    WHEN ISNULL([{COL_SAL_D}],0)=0 THEN 0
                    ELSE
                        CASE WHEN ISNULL([{COL_NM_REM_D}],0)=0 THEN
                            CAST([{COL_CM_SAL_Q}] AS FLOAT) / [{COL_CM_REM_D}]
                        ELSE
                            (CAST([{COL_CM_SAL_Q}] AS FLOAT)
                             + (CAST([{COL_NM_SAL_D}] AS FLOAT) / [{COL_NM_REM_D}])
                               * ([{COL_SAL_D}] - [{COL_CM_REM_D}])
                            ) / [{COL_SAL_D}]
                        END
                END
        """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [{COL_SAL_PD}]>0")).scalar()
        steps.append({"step": "SAL_PD", "detail": f"{cnt} rows calculated", "status": "ok"})
    except Exception as e:
        steps.append({"step": "SAL_PD", "detail": str(e)[:150], "status": "error"})


# ==========================================================================
# ARS_CALC_ST_ART — Article-level calc table
# Same pattern as ST_MAJ_CAT but at GEN_ART level
# Sources: Master_ALC_INPUT_ST_ART, MASTER_ALC_INPUT_CO_ART, MASTER_GEN_ART_SALE
# ==========================================================================

ART_TABLES = {
    "ST_ART":   "Master_ALC_INPUT_ST_ART",
    "CO_ART":   "MASTER_ALC_INPUT_CO_ART",
    "ART_SALE": "MASTER_GEN_ART_SALE",          # used SEPARATELY for SAL_PD only
    "CALC_ART": "ARS_CALC_ST_ART",
}

# ==========================================================================
# ART-LEVEL MERGE RULES — mirrors MAJ_CAT rules
# ==========================================================================
# Rule: if CO has value → use CO (company), else keep ST (store) value.
#   "co_override"  : CO always wins if CO value is not NULL
#   "max"          : MAX(ST value, CO value)
#   "st_first"     : ST wins, CO used only when ST is NULL/0
# ==========================================================================
ART_MERGE_RULES = {
    "LISTING":         "co_override",
    "I_ROD":           "co_override",
    "MANUAL_MBQ":      "max",
    "FOCUS_W_CAP":     "co_override",
    "FOCUS_WO_CAP":    "co_override",
    "CORE":            "co_override",
    "AUTO":            "co_override",
    "HH_ART":          "co_override",
}

# Article key column: CO_ART uses "10_DIGIT" (= GEN_ART_NUMBER)
_ART_KEY_ALIASES = ["GEN_ART_NUMBER", "10_DIGIT", "ART_NUMBER", "ARTICLE_NUMBER"]


def _find_art_key(conn, table: str):
    """Return the column name in `table` that represents the article number."""
    cols = [r[0] for r in conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t"
    ), {"t": table}).fetchall()]
    cols_upper = {c.upper(): c for c in cols}
    for alias in _ART_KEY_ALIASES:
        if alias in cols_upper:
            return cols_upper[alias]
    return None


def _step_create_calc_art(conn, steps):
    """Step A1: Copy Master_ALC_INPUT_ST_ART → ARS_CALC_ST_ART (mirrors MAJ_CAT flow).

    If ST_ART is empty, falls back to synthesizing a base from
    (active stores) × (CO_ART rows) so every store gets the company-level
    article definitions.
    """
    SRC    = ART_TABLES["ST_ART"]
    CO_ART = ART_TABLES["CO_ART"]
    CALC   = ART_TABLES["CALC_ART"]

    if not _exists(conn, SRC) and not _exists(conn, CO_ART):
        steps.append({"step": "Create ART calc", "detail": "Neither ST_ART nor CO_ART found", "status": "skip"})
        return False

    _run(conn, f"IF OBJECT_ID('{CALC}','U') IS NOT NULL DROP TABLE [{CALC}]")

    # Try ST_ART first
    st_count = 0
    if _exists(conn, SRC):
        st_count = conn.execute(text(f"SELECT COUNT(*) FROM [{SRC}]")).scalar() or 0

    if st_count > 0:
        _run(conn, f"SELECT * INTO [{CALC}] FROM [{SRC}] WITH (NOLOCK)")
        src_label = SRC
    else:
        # ST_ART is empty → synthesize from stores × CO_ART (company defaults apply to all stores)
        if not _exists(conn, CO_ART):
            steps.append({"step": "Create ART calc", "detail": f"{SRC} empty and {CO_ART} missing", "status": "skip"})
            return False

        co_key = _find_art_key(conn, CO_ART)
        if not co_key:
            steps.append({"step": "Create ART calc", "detail": f"No article key in {CO_ART}", "status": "skip"})
            return False

        ST_MAST = TABLES["ST_MAST"]
        co_cols = [r[0] for r in conn.execute(text(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t"
        ), {"t": CO_ART}).fetchall()]
        co_cols_upper = {c.upper(): c for c in co_cols}

        # Build select list from CO_ART; rename 10_DIGIT → GEN_ART_NUMBER
        sel_parts = ["ST.[ST_CD]"]
        if "MAJ_CAT" in co_cols_upper:
            sel_parts.append(f"CO.[{co_cols_upper['MAJ_CAT']}] AS [MAJ_CAT]")
        sel_parts.append(f"TRY_CAST(CO.[{co_key}] AS BIGINT) AS [GEN_ART_NUMBER]")
        if "CLR" in co_cols_upper:
            sel_parts.append(f"CO.[{co_cols_upper['CLR']}] AS [CLR]")

        # Add all other CO_ART cols (except the keys we already handled)
        handled = {"MAJ_CAT", co_key.upper(), "CLR"}
        for c in co_cols:
            if c.upper() not in handled and c.upper() != "UPLOAD_DATETIME":
                sel_parts.append(f"CO.[{c}]")

        _run(conn, f"""
            SELECT {', '.join(sel_parts)}
            INTO [{CALC}]
            FROM [{ST_MAST}] ST WITH (NOLOCK)
            CROSS JOIN [{CO_ART}] CO WITH (NOLOCK)
            WHERE ST.[ST_CD] IS NOT NULL
        """)
        src_label = f"{CO_ART} × {ST_MAST} (ST_ART was empty)"

    # Ensure output cols
    _ensure_col(conn, CALC, "SAL_D")
    _ensure_col(conn, CALC, "SAL_PD")
    _ensure_col(conn, CALC, "SALE_COVER_SRC", "NVARCHAR(50)")

    cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}]")).scalar()
    steps.append({"step": "Create ART calc", "detail": f"{cnt} rows from {src_label}", "status": "ok"})
    return True


def _step_merge_co_art(conn, steps):
    """Step A2: Merge MASTER_ALC_INPUT_CO_ART values into ARS_CALC_ST_ART.

    CO_ART is company-level (no ST_CD) → applies to ALL stores for that article.
    Join keys detected dynamically — CO_ART uses '10_DIGIT' = GEN_ART_NUMBER.
    """
    CALC   = ART_TABLES["CALC_ART"]
    CO_ART = ART_TABLES["CO_ART"]
    if not _exists(conn, CO_ART):
        steps.append({"step": "Merge CO_ART", "detail": "Table not found", "status": "skip"})
        return

    co_cols = [r[0] for r in conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t"
    ), {"t": CO_ART}).fetchall()]
    co_cols_upper = {c.upper(): c for c in co_cols}

    co_art_key = _find_art_key(conn, CO_ART)
    if not co_art_key:
        steps.append({"step": "Merge CO_ART", "detail": "No article key found in CO_ART", "status": "skip"})
        return

    # Build join condition: MAJ_CAT (if present) + article key, CLR (if present on both sides)
    join_parts = [f"C.[GEN_ART_NUMBER] = TRY_CAST(CO.[{co_art_key}] AS BIGINT)"]
    if "MAJ_CAT" in co_cols_upper:
        join_parts.insert(0, "C.[MAJ_CAT] = CO.[MAJ_CAT]")
    if "CLR" in co_cols_upper and _col_exists(conn, CALC, "CLR"):
        # CLR match only when CO specifies it; NULL CLR in CO = applies to all colors
        join_parts.append("(CO.[CLR] IS NULL OR C.[CLR] = CO.[CLR])")
    join_cond = " AND ".join(join_parts)

    merged = []
    for col, rule in ART_MERGE_RULES.items():
        if col.upper() not in co_cols_upper:
            continue
        actual_co_col = co_cols_upper[col.upper()]
        if not _col_exists(conn, CALC, col):
            # Add column to CALC if missing (text cols as NVARCHAR, else FLOAT)
            dtype = "NVARCHAR(50)" if col.upper() in ("LISTING","I_ROD","CORE","AUTO","HH_ART","FOCUS_W_CAP","FOCUS_WO_CAP") else "FLOAT"
            _ensure_col(conn, CALC, col, dtype)

        try:
            if rule == "co_override":
                _run(conn, f"""
                    UPDATE C SET C.[{col}] = CO.[{actual_co_col}]
                    FROM [{CALC}] C
                    INNER JOIN [{CO_ART}] CO WITH (NOLOCK) ON {join_cond}
                    WHERE CO.[{actual_co_col}] IS NOT NULL
                """)
            elif rule == "max":
                _run(conn, f"""
                    UPDATE C SET C.[{col}] =
                        CASE WHEN ISNULL(TRY_CAST(C.[{col}] AS FLOAT), 0)
                              >= ISNULL(TRY_CAST(CO.[{actual_co_col}] AS FLOAT), 0)
                             THEN C.[{col}] ELSE CO.[{actual_co_col}] END
                    FROM [{CALC}] C
                    INNER JOIN [{CO_ART}] CO WITH (NOLOCK) ON {join_cond}
                    WHERE CO.[{actual_co_col}] IS NOT NULL
                """)
            elif rule == "st_first":
                _run(conn, f"""
                    UPDATE C SET C.[{col}] = CO.[{actual_co_col}]
                    FROM [{CALC}] C
                    INNER JOIN [{CO_ART}] CO WITH (NOLOCK) ON {join_cond}
                    WHERE (C.[{col}] IS NULL OR ISNULL(TRY_CAST(C.[{col}] AS FLOAT), 0) = 0)
                      AND CO.[{actual_co_col}] IS NOT NULL
                """)
            merged.append(f"{col}({rule})")
        except Exception as e:
            steps.append({"step": f"Merge CO_ART {col}", "detail": str(e)[:100], "status": "error"})

    steps.append({"step": "Merge CO_ART", "detail": f"{len(merged)} cols: {', '.join(merged)}", "status": "ok"})


def _step_art_sal_d(conn, steps):
    """Step A3: SAL_D for article level — from ST_MASTER (same as MAJ_CAT level)."""
    CALC    = ART_TABLES["CALC_ART"]
    ST_MAST = TABLES["ST_MAST"]

    if not _exists(conn, ST_MAST) or not _col_exists(conn, CALC, "ST_CD"):
        steps.append({"step": "ART SAL_D", "detail": "ST_MASTER or ST_CD missing", "status": "skip"})
        return

    _ensure_col(conn, CALC, "SAL_D")
    _ensure_col(conn, CALC, "SALE_COVER_SRC", "NVARCHAR(50)")

    try:
        # Base: INT_DAYS + PRD_DAYS + SL_CVR from ST_MASTER
        _run(conn, f"""
            UPDATE C SET C.[SALE_COVER_SRC]='ST_MASTER',
                C.[SAL_D] = ISNULL(S.[{COL_INT_DAYS}],0)+ISNULL(S.[{COL_PRD_DAYS}],0)+ISNULL(S.[{COL_SL_CVR}],0)
            FROM [{CALC}] C
            INNER JOIN [{ST_MAST}] S WITH (NOLOCK) ON C.[ST_CD]=S.[ST_CD]
        """)
        # Override with ST_ART's own SL_CVR if available
        if _col_exists(conn, CALC, COL_SL_CVR):
            _run(conn, f"""
                UPDATE C SET C.[SALE_COVER_SRC]='ST_ART',
                    C.[SAL_D] = ISNULL(S.[{COL_INT_DAYS}],0)+ISNULL(S.[{COL_PRD_DAYS}],0)+ISNULL(C.[{COL_SL_CVR}],0)
                FROM [{CALC}] C
                INNER JOIN [{ST_MAST}] S WITH (NOLOCK) ON C.[ST_CD]=S.[ST_CD]
                WHERE C.[{COL_SL_CVR}] IS NOT NULL AND C.[{COL_SL_CVR}] > 0
            """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [SAL_D]>0")).scalar()
        steps.append({"step": "ART SAL_D", "detail": f"{cnt} rows", "status": "ok"})
    except Exception as e:
        steps.append({"step": "ART SAL_D", "detail": str(e)[:150], "status": "error"})


def _step_art_sal_pd(conn, steps):
    """Step A4: SAL_PD for article level — joined from MASTER_GEN_ART_SALE (separate source).

    Formula mirrors MAJ_CAT SAL_PD. CM_REM_D / NM_REM_D come from ST_MAJ_CAT
    (via left join on ST_CD+MAJ_CAT), CM_SAL_Q / NM_SAL_Q come from ART_SALE
    (via join on ST_CD+GEN_ART_NUMBER[+CLR]).
    """
    CALC     = ART_TABLES["CALC_ART"]
    ART_SALE = ART_TABLES["ART_SALE"]
    ST_MAJ   = TABLES["ST_MAJ"]

    _ensure_col(conn, CALC, "SAL_PD")

    if not _exists(conn, ART_SALE):
        # Fallback: use same formula as MAJ_CAT if CM_SAL_Q columns exist in CALC
        needed = [COL_CM_SAL_Q, COL_CM_REM_D, COL_NM_SAL_D, COL_NM_REM_D, "SAL_D"]
        missing = [c for c in needed if not _col_exists(conn, CALC, c)]
        if missing:
            steps.append({"step": "ART SAL_PD", "detail": f"{ART_SALE} not found, fallback missing: {missing}", "status": "skip"})
            return
        try:
            _run(conn, f"""
                UPDATE [{CALC}] SET [SAL_PD] =
                    CASE
                        WHEN ISNULL([{COL_CM_REM_D}],0)=0 THEN 0
                        WHEN [{COL_CM_REM_D}] >= ISNULL([SAL_D],0) THEN
                            CAST([{COL_CM_SAL_Q}] AS FLOAT) / [{COL_CM_REM_D}]
                        WHEN ISNULL([SAL_D],0)=0 THEN 0
                        ELSE
                            CASE WHEN ISNULL([{COL_NM_REM_D}],0)=0 THEN
                                CAST([{COL_CM_SAL_Q}] AS FLOAT) / [{COL_CM_REM_D}]
                            ELSE
                                (CAST([{COL_CM_SAL_Q}] AS FLOAT)
                                 + (CAST([{COL_NM_SAL_D}] AS FLOAT) / [{COL_NM_REM_D}])
                                   * ([SAL_D] - [{COL_CM_REM_D}])
                                ) / [SAL_D]
                            END
                    END
            """)
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [SAL_PD]>0")).scalar()
            steps.append({"step": "ART SAL_PD (fallback)", "detail": f"{cnt} rows", "status": "ok"})
        except Exception as e:
            steps.append({"step": "ART SAL_PD (fallback)", "detail": str(e)[:150], "status": "error"})
        return

    # Join ART_SALE for CM_SAL_Q/NM_SAL_Q and ST_MAJ_CAT for CM_REM_D/NM_REM_D
    if not _exists(conn, ART_SALE):
        steps.append({"step": "ART SAL_PD", "detail": f"{ART_SALE} not found", "status": "skip"})
        return

    sale_cols = {r[0].upper() for r in conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :t"
    ), {"t": ART_SALE}).fetchall()}

    # ART_SALE must have ST_CD + GEN_ART_NUMBER + CM_SAL_Q
    required_sale = [COL_CM_SAL_Q.upper()]
    if not all(c in sale_cols for c in required_sale):
        steps.append({"step": "ART SAL_PD", "detail": f"{ART_SALE} missing {required_sale}", "status": "skip"})
        return

    # Build ART_SALE join
    sale_join_parts = []
    if "ST_CD" in sale_cols and _col_exists(conn, CALC, "ST_CD"):
        sale_join_parts.append("C.[ST_CD] = SA.[ST_CD]")
    if "GEN_ART_NUMBER" in sale_cols and _col_exists(conn, CALC, "GEN_ART_NUMBER"):
        sale_join_parts.append("C.[GEN_ART_NUMBER] = SA.[GEN_ART_NUMBER]")
    if "CLR" in sale_cols and _col_exists(conn, CALC, "CLR"):
        sale_join_parts.append("C.[CLR] = SA.[CLR]")
    if not sale_join_parts:
        steps.append({"step": "ART SAL_PD", "detail": "No join keys for ART_SALE", "status": "skip"})
        return
    sale_join = " AND ".join(sale_join_parts)

    # Build ST_MAJ_CAT join for REM_D values (may not be available)
    has_maj = _exists(conn, ST_MAJ)
    cm_rem_expr = f"ISNULL(MJ.[{COL_CM_REM_D}], 0)" if has_maj else "0"
    nm_rem_expr = f"ISNULL(MJ.[{COL_NM_REM_D}], 0)" if has_maj else "0"
    maj_join_clause = f"LEFT JOIN [{ST_MAJ}] MJ WITH (NOLOCK) ON C.[ST_CD] = MJ.[ST_CD] AND C.[MAJ_CAT] = MJ.[MAJ_CAT]" if has_maj else ""

    cm_sal = COL_CM_SAL_Q if COL_CM_SAL_Q.upper() in sale_cols else None
    nm_sal = COL_NM_SAL_D if COL_NM_SAL_D.upper() in sale_cols else None
    nm_sal_expr = f"SA.[{nm_sal}]" if nm_sal else f"SA.[{cm_sal}]"

    try:
        _run(conn, f"""
            UPDATE C SET C.[SAL_PD] =
                CASE
                    WHEN {cm_rem_expr}=0 THEN 0
                    WHEN {cm_rem_expr} >= ISNULL(C.[SAL_D],0) THEN
                        CAST(SA.[{cm_sal}] AS FLOAT) / {cm_rem_expr}
                    WHEN ISNULL(C.[SAL_D],0)=0 THEN 0
                    ELSE
                        CASE WHEN {nm_rem_expr}=0 THEN
                            CAST(SA.[{cm_sal}] AS FLOAT) / {cm_rem_expr}
                        ELSE
                            (CAST(SA.[{cm_sal}] AS FLOAT)
                             + (CAST({nm_sal_expr} AS FLOAT) / {nm_rem_expr})
                               * (C.[SAL_D] - {cm_rem_expr})
                            ) / C.[SAL_D]
                        END
                END
            FROM [{CALC}] C
            INNER JOIN [{ART_SALE}] SA WITH (NOLOCK) ON {sale_join}
            {maj_join_clause}
        """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{CALC}] WHERE [SAL_PD]>0")).scalar()
        detail = f"{cnt} rows from {ART_SALE}"
        if has_maj:
            detail += f" (REM_D from {ST_MAJ})"
        steps.append({"step": "ART SAL_PD", "detail": detail, "status": "ok"})
    except Exception as e:
        steps.append({"step": "ART SAL_PD", "detail": str(e)[:150], "status": "error"})


# ==========================================================================
# STEP M: SAL_PD directly on MASTER_GEN_ART_SALE
# ==========================================================================
# Rationale: MASTER_GEN_ART_SALE carries the full planned-sales universe
# (~21L rows) while ARS_CALC_ST_ART only covers the ST_ART master sample.
# Listing's AUTO_GEN_ART_SALE needs option-level coverage, so compute
# per-day-sale in place on the master table and have listing join it.
#
# Formula mirrors MAJ_CAT SAL_PD:
#   - CM_SAL_Q / NM_SAL_Q  → from MASTER_GEN_ART_SALE (row itself)
#   - CM_REM_D / NM_REM_D / SAL_D → from ARS_CALC_ST_MAJ_CAT (ST_CD + MAJ_CAT)
# ==========================================================================
def _step_master_sale_sal_pd(conn, steps):
    SALE_T = "MASTER_GEN_ART_SALE"
    MAJ_T  = TABLES["CALC"]   # ARS_CALC_ST_MAJ_CAT

    if not _exists(conn, SALE_T):
        steps.append({"step": "MASTER SAL_PD", "detail": f"{SALE_T} not found", "status": "skip"})
        return
    if not _exists(conn, MAJ_T):
        steps.append({"step": "MASTER SAL_PD", "detail": f"{MAJ_T} not found", "status": "skip"})
        return

    sale_cols = {
        r[0].upper() for r in conn.execute(text(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=:t"
        ), {"t": SALE_T}).fetchall()
    }
    needed_sale = {COL_CM_SAL_Q.upper(), COL_NM_SAL_D.upper(), "ST_CD", "MAJ_CAT"}
    missing = needed_sale - sale_cols
    if missing:
        steps.append({"step": "MASTER SAL_PD", "detail": f"{SALE_T} missing: {missing}", "status": "skip"})
        return

    maj_cols = {c.upper() for c in [COL_CM_REM_D, COL_NM_REM_D, COL_SAL_D] if _col_exists(conn, MAJ_T, c)}
    if not {COL_CM_REM_D.upper(), COL_SAL_D.upper()}.issubset(maj_cols):
        steps.append({"step": "MASTER SAL_PD", "detail": f"{MAJ_T} missing CM_REM_D or SAL_D", "status": "skip"})
        return

    _ensure_col(conn, SALE_T, COL_SAL_PD)

    try:
        _run(conn, f"""
            UPDATE S SET S.[{COL_SAL_PD}] =
                CASE
                    WHEN ISNULL(MJ.[{COL_CM_REM_D}],0)=0 THEN 0
                    WHEN MJ.[{COL_CM_REM_D}] >= ISNULL(MJ.[{COL_SAL_D}],0) THEN
                        CAST(S.[{COL_CM_SAL_Q}] AS FLOAT) / MJ.[{COL_CM_REM_D}]
                    WHEN ISNULL(MJ.[{COL_SAL_D}],0)=0 THEN 0
                    ELSE
                        CASE WHEN ISNULL(MJ.[{COL_NM_REM_D}],0)=0 THEN
                            CAST(S.[{COL_CM_SAL_Q}] AS FLOAT) / MJ.[{COL_CM_REM_D}]
                        ELSE
                            (CAST(S.[{COL_CM_SAL_Q}] AS FLOAT)
                             + (CAST(S.[{COL_NM_SAL_D}] AS FLOAT) / MJ.[{COL_NM_REM_D}])
                               * (MJ.[{COL_SAL_D}] - MJ.[{COL_CM_REM_D}])
                            ) / MJ.[{COL_SAL_D}]
                        END
                END
            FROM [{SALE_T}] S
            INNER JOIN [{MAJ_T}] MJ WITH (NOLOCK)
                ON S.[ST_CD] = MJ.[ST_CD] AND S.[MAJ_CAT] = MJ.[MAJ_CAT]
        """)
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{SALE_T}] WHERE [{COL_SAL_PD}]>0")).scalar()
        steps.append({"step": "MASTER SAL_PD", "detail": f"{cnt} rows in {SALE_T}", "status": "ok"})
    except Exception as e:
        steps.append({"step": "MASTER SAL_PD", "detail": str(e)[:150], "status": "error"})


# ==========================================================================
# MAIN: Run all pre-grid calculations
# ==========================================================================
def calculate_per_day_sale(conn) -> List[Dict[str, Any]]:
    """
    Full pre-grid calculation pipeline:
      MAJ_CAT level:
        1. Copy Master → ARS_CALC_ST_MAJ_CAT
        2. Merge CO_MAJ_CAT values
        3. Apply defaults
        4. SAL_D (total sale days)
        5. SAL_PD (per day sale)
      MASTER_GEN_ART_SALE:
        M. SAL_PD computed in place (~21L rows — full option coverage
           for listing AUTO_GEN_ART_SALE; REM_D/SAL_D from MAJ_CAT calc)
      ART level (mirrors MAJ_CAT flow):
        A1. Copy ST_ART → ARS_CALC_ST_ART (if ST_ART empty, cross-join stores × CO_ART)
        A2. Merge CO_ART values (CO overrides ST where CO has data)
        A3. SAL_D (from ST_MASTER)
        A4. SAL_PD (JOIN MASTER_GEN_ART_SALE for CM_SAL_Q/NM_SAL_Q + ST_MAJ_CAT for CM_REM_D/NM_REM_D)
    """
    steps = []

    # Ensure PKs
    for msg in ensure_primary_keys(conn):
        steps.append({"step": "Ensure PK", "detail": msg, "status": "ok"})

    # ── MAJ_CAT level ──
    if not _step_create_calc(conn, steps):
        return steps
    _step_merge_co_values(conn, steps)
    _step_defaults(conn, steps)
    _step_sal_d(conn, steps)
    _step_sal_pd(conn, steps)

    # ── MASTER_GEN_ART_SALE in-place SAL_PD (full option coverage) ──
    _step_master_sale_sal_pd(conn, steps)

    # ── ART level ──
    if _step_create_calc_art(conn, steps):
        _step_merge_co_art(conn, steps)
        _step_art_sal_d(conn, steps)
        _step_art_sal_pd(conn, steps)

    return steps
