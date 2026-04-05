"""
Grid Builder API
================
Manages dynamic pivot-grid definitions and executes them against ET_STORE_STOCK.

Tables (Rep_data):
  ARS_GRID_BUILDER  – grid metadata (name, hierarchy cols, kpi filter, output table …)

Endpoints:
  GET  /grid-builder/columns              – list columns from vw_master_product
  GET  /grid-builder/grids                – list all grids
  POST /grid-builder/grids                – create a grid
  PUT  /grid-builder/grids/{id}           – update a grid
  DELETE /grid-builder/grids/{id}         – delete a grid
  POST /grid-builder/grids/{id}/run       – run one grid
  POST /grid-builder/run-all              – run all Active grids
"""

import json
from datetime import datetime
from typing import List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, validator
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.services.grid_calculations import calculate_per_day_sale, ensure_primary_keys
from app.models.rbac import User

router      = APIRouter(prefix="/grid-builder", tags=["Grid Builder"])
GRID_TABLE  = "ARS_GRID_BUILDER"
VALID_STATUS = {"Active", "Inactive"}


# ── Schemas ──────────────────────────────────────────────────────────────────

class GridCreate(BaseModel):
    grid_name:         str
    description:       Optional[str] = None
    hierarchy_columns: List[str]           # columns from vw_master_product
    kpi_filter:        Optional[str] = None # e.g. 'STK'  – filters on sloc KPI
    output_table:      str                 # e.g. ARS_GRID_STK_RESULT
    status:            str = "Active"

    @validator("status")
    def _chk(cls, v):
        if v not in VALID_STATUS:
            raise ValueError("status must be Active or Inactive")
        return v

    @validator("grid_name")
    def _chk_name(cls, v):
        if not v.strip():
            raise ValueError("grid_name cannot be empty")
        return v.strip()

    @validator("output_table")
    def _chk_table(cls, v):
        # Only allow safe table name characters
        import re
        if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', v.strip()):
            raise ValueError("output_table must start with a letter and contain only letters, numbers, underscores")
        return v.strip().upper()

class GridUpdate(BaseModel):
    grid_name:         Optional[str]       = None
    description:       Optional[str]       = None
    hierarchy_columns: Optional[List[str]] = None
    kpi_filter:        Optional[str]       = None
    output_table:      Optional[str]       = None
    status:            Optional[str]       = None

    @validator("status")
    def _chk(cls, v):
        if v is not None and v not in VALID_STATUS:
            raise ValueError("status must be Active or Inactive")
        return v


# ── DDL / helpers ─────────────────────────────────────────────────────────────

def _run(conn, sql: str, params: dict = None):
    conn.execute(text(sql), params or {})
    conn.commit()


def _ensure_grid_table(engine):
    """Auto-create ARS_GRID_BUILDER in Rep_data."""
    with engine.connect() as c:
        _run(c, f"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{GRID_TABLE}')
            BEGIN
                CREATE TABLE {GRID_TABLE} (
                    id                INT IDENTITY(1,1) PRIMARY KEY,
                    grid_name         NVARCHAR(100) NOT NULL,
                    description       NVARCHAR(500) NULL,
                    hierarchy_columns NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                    kpi_filter        NVARCHAR(200) NULL,
                    output_table      NVARCHAR(200) NOT NULL,
                    status            NVARCHAR(20)  NOT NULL DEFAULT 'Active',
                    seq               INT           NOT NULL DEFAULT 0,
                    created_at        DATETIME      NOT NULL DEFAULT GETDATE(),
                    updated_at        DATETIME      NOT NULL DEFAULT GETDATE(),
                    last_run_at       DATETIME      NULL,
                    last_run_status   NVARCHAR(50)  NULL,
                    last_run_rows     INT           NULL,
                    last_run_error    NVARCHAR(MAX) NULL,
                    CONSTRAINT UQ_{GRID_TABLE}_name UNIQUE (grid_name)
                )
            END
        """)
        # Add seq column if missing (for existing tables)
        _run(c, f"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME='{GRID_TABLE}' AND COLUMN_NAME='seq')
            BEGIN
                ALTER TABLE {GRID_TABLE} ADD seq INT NOT NULL DEFAULT 0
            END
        """)
        # Add duration_sec column if missing
        _run(c, f"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME='{GRID_TABLE}' AND COLUMN_NAME='duration_sec')
            BEGIN
                ALTER TABLE {GRID_TABLE} ADD duration_sec FLOAT NULL
            END
        """)
        # Auto-assign sequence where seq=0 based on id order
        _run(c, f"""
            ;WITH CTE AS (
                SELECT id, seq, ROW_NUMBER() OVER (ORDER BY id) AS rn
                FROM {GRID_TABLE} WHERE seq = 0
            )
            UPDATE CTE SET seq = rn WHERE seq = 0
        """)
        # Reset stuck "Running" status (from crashed/stopped server)
        _run(c, f"""
            UPDATE {GRID_TABLE}
            SET last_run_status = 'Interrupted', last_run_error = 'Server stopped during run'
            WHERE last_run_status = 'Running'
        """)


def _row_to_dict(r) -> dict:
    """Convert a row tuple to dict. Column order must match SELECT statements."""
    hier = r[3]
    try:
        hier = json.loads(hier) if hier else []
    except Exception:
        hier = []
    return {
        "id":                r[0],
        "grid_name":         r[1],
        "description":       r[2],
        "hierarchy_columns": hier,
        "kpi_filter":        r[4],
        "output_table":      r[5],
        "status":            r[6],
        "seq":               r[7],
        "created_at":        r[8].isoformat() if r[8] else None,
        "updated_at":        r[9].isoformat() if r[9] else None,
        "last_run_at":       r[10].isoformat() if r[10] else None,
        "last_run_status":   r[11],
        "last_run_rows":     r[12],
        "last_run_error":    r[13],
        "duration_sec":      r[14] if len(r) > 14 else None,
    }


def _get_table_columns(engine, table_name: str) -> List[str]:
    """Return column names for a table/view from INFORMATION_SCHEMA."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = :tbl
                ORDER BY ORDINAL_POSITION
            """), {"tbl": table_name}).fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        logger.warning(f"Could not read {table_name} columns: {e}")
        return []


def _get_master_product_columns(engine) -> List[str]:
    """Return column names from vw_master_product."""
    return _get_table_columns(engine, "vw_master_product")


# ==========================================================================
# POST-GRID CALCULATIONS — imported from app.services.grid_calculations
# Function: calculate_per_day_sale(conn) → returns step logs
# Edit column names/logic in: app/services/grid_calculations.py
# ==========================================================================




# ==========================================================================
# POST-PIVOT LOOKUP CONFIG
# ==========================================================================
# Each entry defines a lookup join to run after the pivot INSERT.
# To add a new lookup: copy an entry and edit the fields.
#
#   lookup_table : source table name — supports templates:
#                  {HIER_LAST}  = last hierarchy column name (e.g. MAJ_CAT, RNG_SEG)
#                  {HIER_2}     = 2nd hierarchy column
#                  {HIER_3}     = 3rd hierarchy column
#                  Example: "Master_CONT_{HIER_LAST}" → "Master_CONT_RNG_SEG"
#   columns      : list of columns to pull (empty [] = filter only, ["*"] = all non-key cols)
#   join_on      : dict mapping {output_table_col: lookup_table_col}
#                  supports {HIER_LAST} in values too
#   requires     : list of hierarchy columns that must be present (uppercase)
#   filter       : (optional) {"column": "COL", "value": "1"}
#                  After join, DELETE rows where column != value
#
POST_PIVOT_LOOKUPS = [
    # 1. Filter: keep only stores where LISTING=1 in Master_ALC_INPUT_ST_MASTER
    {
        "lookup_table": "Master_ALC_INPUT_ST_MASTER",
        "columns":      ["LISTING"],
        "join_on":      {"WERKS": "ST_CD"},
        "requires":     ["WERKS"],
        "filter":       {"column": "LISTING", "value": "1"},
    },
    # 2. Lookup from ARS_CALC_ST_MAJ_CAT (has SAL_D, SAL_PD — MBQ/OPT_CNT calculated after CONT join)
    {
        "lookup_table": "ARS_CALC_ST_MAJ_CAT",
        "columns":      ["DISP_Q", "DPN", "SAL_D", "SAL_PD", "DISP_GR_DGR", "LW_ACT_SL_GR_DGR", "BGT_SL_GR_DGR", "MANUAL_MBQ"],
        "join_on":      {"WERKS": "ST_CD", "MAJ_CAT": "MAJ_CAT"},
        "requires":     ["WERKS", "MAJ_CAT"],
    },
    # 3. Dynamic: join contribution data from Master_CONT_{last hierarchy col}
    #    Grid MJ (WERKS, MAJ_CAT)                        → join on ST_CD + MAJ_CAT
    #    Grid MJ_MACRO_MVGR (WERKS, MAJ_CAT, MACRO_MVGR) → join on ST_CD + MAJ_CAT + MACRO_MVGR
    {
        "lookup_table": "Master_CONT_{HIER_LAST}",
        "columns":      ["CONT"],
        "join_on":      {"WERKS": "ST_CD", "MAJ_CAT": "MAJ_CAT", "{HIER_LAST}": "{HIER_LAST}"},
        "requires":     ["WERKS", "MAJ_CAT"],
    },
    # ── Add more lookups below ──────────────────────────────────────────
]


def _resolve_template(template: str, hier_cols: List[str]) -> str:
    """Resolve {HIER_LAST}, {HIER_2}, {HIER_3} etc. in config strings."""
    result = template
    if "{HIER_LAST}" in result and hier_cols:
        result = result.replace("{HIER_LAST}", hier_cols[-1])
    for i, col in enumerate(hier_cols):
        result = result.replace(f"{{HIER_{i}}}", col)
        result = result.replace(f"{{HIER_{i+1}}}", col)  # 1-based
    return result


def _get_col_type_sql(conn, table_name: str, col_name: str) -> str:
    """Get SQL type string for a column from INFORMATION_SCHEMA."""
    row = conn.execute(text(
        "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "
        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tbl AND COLUMN_NAME = :col"
    ), {"tbl": table_name, "col": col_name}).fetchone()
    if not row:
        return "NVARCHAR(255)"
    dt = row[0].upper()
    if dt in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR"):
        ml = row[1]
        return f"{dt}({ml})" if ml and ml > 0 else f"{dt}(MAX)"
    elif dt in ("DECIMAL", "NUMERIC"):
        return f"{dt}({row[2] or 18},{row[3] or 2})"
    return dt


def _apply_post_lookups(conn, out_table: str, hier_cols: List[str]) -> List[str]:
    """
    After pivot INSERT, join lookup tables and add extra columns.
    Returns list of warning messages (e.g. missing tables).
    """
    hier_upper = {c.upper(): c for c in hier_cols}
    warnings = []

    for cfg in POST_PIVOT_LOOKUPS:
        # Check all required hierarchy columns are present
        if not all(r in hier_upper for r in cfg["requires"]):
            continue

        # Resolve template in table name
        lookup_table = _resolve_template(cfg["lookup_table"], hier_cols)

        # Check lookup table exists in DB
        exists = conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tn"
        ), {"tn": lookup_table}).scalar() > 0
        if not exists:
            msg = f"Lookup table '{lookup_table}' not found in DB"
            logger.warning(f"Post-lookup: {msg}")
            warnings.append(msg)

            # If CONT table missing, calculate CONT = 1/COUNT per WERKS+MAJ_CAT group
            if "Master_CONT_" in cfg.get("lookup_table", ""):
                default_cols = [c for c in cfg.get("columns", []) if c != "*"]
                if default_cols:
                    existing_out = {r[0].upper() for r in conn.execute(text(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tbl"
                    ), {"tbl": out_table}).fetchall()}
                    for col in default_cols:
                        if col.upper() not in existing_out:
                            try:
                                _run(conn, f"ALTER TABLE [{out_table}] ADD [{col}] FLOAT NULL")
                            except Exception:
                                pass

                    # Calculate 1/COUNT of unique rows per WERKS+MAJ_CAT
                    werks_col = hier_upper.get("WERKS", "WERKS")
                    majcat_col = hier_upper.get("MAJ_CAT", "MAJ_CAT")
                    for col in default_cols:
                        _run(conn, f"""
                            ;WITH GrpCount AS (
                                SELECT [{werks_col}], [{majcat_col}],
                                       COUNT(*) AS cnt
                                FROM [{out_table}]
                                GROUP BY [{werks_col}], [{majcat_col}]
                            )
                            UPDATE O SET O.[{col}] = CAST(1.0 / G.cnt AS FLOAT)
                            FROM [{out_table}] O
                            INNER JOIN GrpCount G
                                ON O.[{werks_col}] = G.[{werks_col}]
                                AND O.[{majcat_col}] = G.[{majcat_col}]
                        """)
                    msg2 = f"Column(s) {default_cols} set to 1/COUNT(WERKS+MAJ_CAT) ('{lookup_table}' not found)"
                    logger.info(msg2)
                    warnings.append(msg2)
            continue

        # Resolve template in join_on keys and values
        join_on = {}
        for out_col, lkp_col in cfg["join_on"].items():
            resolved_out = _resolve_template(out_col, hier_cols)
            resolved_lkp = _resolve_template(lkp_col, hier_cols)
            join_on[resolved_out] = resolved_lkp

        # Resolve columns: ["*"] = all columns from lookup except join-target columns
        columns = cfg["columns"]
        if columns == ["*"]:
            all_lkp_cols = conn.execute(text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME = :tbl ORDER BY ORDINAL_POSITION"
            ), {"tbl": lookup_table}).fetchall()
            join_target_cols = {v.upper() for v in join_on.values()}
            columns = [r[0] for r in all_lkp_cols if r[0].upper() not in join_target_cols]

        if not columns:
            logger.info(f"Post-lookup skipped: no columns to add from {lookup_table}")
            continue

        # Get existing output columns to avoid duplicates
        existing_out_cols = {r[0].upper() for r in conn.execute(text(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tbl"
        ), {"tbl": out_table}).fetchall()}

        # Add missing columns to output table (using real types from lookup)
        for col in columns:
            if col.upper() not in existing_out_cols:
                col_type = _get_col_type_sql(conn, lookup_table, col)
                try:
                    _run(conn, f"ALTER TABLE [{out_table}] ADD [{col}] {col_type} NULL")
                    existing_out_cols.add(col.upper())  # track newly added
                except Exception:
                    pass  # column may already exist from a prior run

        # Build join condition
        join_parts = " AND ".join(
            f"O.[{hier_upper.get(ok.upper(), ok)}] = L.[{lv}]"
            for ok, lv in join_on.items()
        )

        # UPDATE output table with lookup columns
        set_parts = ", ".join(f"O.[{c}] = L.[{c}]" for c in columns)
        _run(conn, f"""
            UPDATE O SET {set_parts}
            FROM [{out_table}] O
            INNER JOIN [{lookup_table}] L WITH (NOLOCK) ON {join_parts}
        """)
        logger.info(f"Post-lookup: joined {len(columns)} cols from {lookup_table} into {out_table}")

        # Apply filter: DELETE rows that don't match criteria
        flt = cfg.get("filter")
        if flt:
            fcol = flt["column"]
            fval = flt["value"]
            before = conn.execute(text(f"SELECT COUNT(*) FROM [{out_table}]")).scalar()
            _run(conn, f"""
                DELETE FROM [{out_table}]
                WHERE ISNULL(CAST([{fcol}] AS NVARCHAR(50)), '') <> :fval
            """, {"fval": str(fval)})
            after = conn.execute(text(f"SELECT COUNT(*) FROM [{out_table}]")).scalar()
            logger.info(f"Post-lookup filter: [{fcol}]={fval} → kept {after}/{before} rows")

    return warnings


# ==========================================================================
# GRID-LEVEL CALCULATIONS (run on output table after lookups)
# ==========================================================================
# MBQ     = (SAL_PD * BGT_SL_GR_DGR) * SAL_D + (DISP_Q * DISP_GR_DGR)
#           Default 1 if BGT_SL_GR_DGR or DISP_GR_DGR is blank/null
#           Then: MBQ = ROUND(MBQ * CONT, 1)
# OPT_CNT = ROUND(DISP_Q * CONT / DPN, 1)
# ==========================================================================

def _col_exists_in(conn, tbl, col):
    return conn.execute(text(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tbl AND COLUMN_NAME = :col"
    ), {"tbl": tbl, "col": col}).scalar() > 0


def _ensure_output_col(conn, tbl, col, col_type="FLOAT"):
    if not _col_exists_in(conn, tbl, col):
        try:
            _run(conn, f"ALTER TABLE [{tbl}] ADD [{col}] {col_type} NULL")
        except Exception:
            pass


def _calculate_grid_columns(conn, out_table: str) -> List[str]:
    """
    Calculate MBQ and OPT_CNT in the grid output table.
    Returns list of warning messages.
    """
    warnings = []

    # ── MBQ = (SAL_PD * BGT_SL_GR_DGR) * SAL_D + (DISP_Q * DISP_GR_DGR) ──
    mbq_required = ["SAL_PD", "SAL_D", "DISP_Q", "DISP_GR_DGR", "BGT_SL_GR_DGR"]
    mbq_missing = [c for c in mbq_required if not _col_exists_in(conn, out_table, c)]
    if mbq_missing:
        warnings.append(f"MBQ skipped: missing {mbq_missing}")
    else:
        _ensure_output_col(conn, out_table, "MBQ")
        try:
            # Step 1: Calculate raw MBQ
            _run(conn, f"""
                UPDATE [{out_table}] SET [MBQ] =
                    (ISNULL(TRY_CAST([SAL_PD] AS FLOAT), 0)
                     * CASE WHEN ISNULL(TRY_CAST([BGT_SL_GR_DGR] AS FLOAT), 0) = 0 THEN 1
                            ELSE TRY_CAST([BGT_SL_GR_DGR] AS FLOAT) END)
                    * ISNULL(TRY_CAST([SAL_D] AS FLOAT), 0)
                    + (ISNULL(TRY_CAST([DISP_Q] AS FLOAT), 0)
                       * CASE WHEN ISNULL(TRY_CAST([DISP_GR_DGR] AS FLOAT), 0) = 0 THEN 1
                              ELSE TRY_CAST([DISP_GR_DGR] AS FLOAT) END)
            """)
            # Step 2: MBQ = ROUND(MBQ * CONT, 0) — if CONT is 0 or NULL, MBQ = 0
            if _col_exists_in(conn, out_table, "CONT"):
                _run(conn, f"""
                    UPDATE [{out_table}] SET [MBQ] =
                        CASE WHEN ISNULL(TRY_CAST([CONT] AS FLOAT), 0) = 0 THEN 0
                             ELSE ROUND([MBQ] * TRY_CAST([CONT] AS FLOAT), 0)
                        END
                """)
            logger.info(f"MBQ calculated in {out_table}")
        except Exception as e:
            warnings.append(f"MBQ error: {str(e)[:150]}")

    # ── OPT_CNT = ROUND(DISP_Q * CONT / DPN, 1) ───────────────────────────
    opt_required = ["DISP_Q", "CONT", "DPN"]
    opt_missing = [c for c in opt_required if not _col_exists_in(conn, out_table, c)]
    if opt_missing:
        warnings.append(f"OPT_CNT skipped: missing {opt_missing}")
    else:
        _ensure_output_col(conn, out_table, "OPT_CNT")
        try:
            _run(conn, f"""
                UPDATE [{out_table}] SET [OPT_CNT] =
                    CASE
                        WHEN ISNULL(TRY_CAST([CONT] AS FLOAT), 0) = 0 THEN 0
                        WHEN ISNULL(TRY_CAST([DPN] AS FLOAT), 0) = 0 THEN 0
                        ELSE ROUND(ISNULL(TRY_CAST([DISP_Q] AS FLOAT), 0)
                                 * TRY_CAST([CONT] AS FLOAT)
                                 / TRY_CAST([DPN] AS FLOAT), 0)
                    END
            """)
            logger.info(f"OPT_CNT calculated in {out_table}")
        except Exception as e:
            warnings.append(f"OPT_CNT error: {str(e)[:150]}")

    return warnings


def _build_and_run_grid(engine, grid: dict) -> dict:
    """
    Execute the dynamic pivot SQL for a grid and store results in output_table.
    Returns {"rows": N, "error": None|str}
    """
    hier_cols:  List[str] = grid["hierarchy_columns"] or ["MATNR", "WERKS"]
    kpi_filter: Optional[str] = grid["kpi_filter"]
    out_table:  str = grid["output_table"]

    # ── 1. Get active SLOCs (optionally filtered by KPI) ────────────────────
    kpi_clause = ""
    if kpi_filter:
        kpi_clause = f" AND UPPER(S.KPI) = '{kpi_filter.upper().replace(chr(39), '')}'"

    with engine.connect() as conn:
        sloc_rows = conn.execute(text(f"""
            SELECT DISTINCT STK.SLOC, S.KPI
            FROM dbo.ET_STORE_STOCK STK WITH (NOLOCK)
            INNER JOIN ARS_STORE_SLOC_SETTINGS S WITH (NOLOCK) ON STK.SLOC = S.SLOC
            WHERE UPPER(S.STATUS) = 'ACTIVE'{kpi_clause}
            ORDER BY STK.SLOC ASC
        """)).fetchall()

        # Include PEND_ALC if active in settings and table exists
        pend_row = conn.execute(text(
            "SELECT S.SLOC, S.KPI FROM ARS_STORE_SLOC_SETTINGS S WITH (NOLOCK) WHERE S.SLOC='PEND_ALC' AND UPPER(S.STATUS)='ACTIVE'"
        )).fetchone()
        if pend_row:
            pend_table_exists = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ARS_pend_alc'"
            )).scalar() > 0
            if pend_table_exists:
                sloc_rows = list(sloc_rows) + [(pend_row[0], pend_row[1])]

    sloc_kpi_pairs = [(r[0], (r[1] or '').upper()) for r in sloc_rows if r[0]]
    if not sloc_kpi_pairs:
        return {"rows": 0, "error": "No ACTIVE SLOCs found matching the criteria"}

    # Sort SLOCs by KPI group → same KPI SLOCs are together, then alphabetical within group
    sloc_kpi_pairs.sort(key=lambda x: (x[1] or 'ZZZ', x[0]))
    slocs = [s for s, _ in sloc_kpi_pairs]

    # SLOCs where KPI = 'STK' → used for STK_TTL calculation
    stk_slocs = [s for s, k in sloc_kpi_pairs if k == "STK"]

    # ── 2. Build quoted column lists ─────────────────────────────────────────
    q_slocs      = ", ".join(f"[{s}]" for s in slocs)
    isnull_cols  = ", ".join(f"ISNULL([{s}],0) AS [{s}]" for s in slocs)
    sum_expr     = " + ".join(f"ISNULL([{s}],0)" for s in stk_slocs) if stk_slocs else "0"

    # ── 3. Hierarchy columns SELECT & JOIN ────────────────────────────────────
    # Determine which columns come from vw_master_product vs ET_STORE_STOCK
    mp_cols = _get_master_product_columns(engine)
    mp_cols_upper = {c.upper(): c for c in mp_cols}   # upper→actual name
    stk_cols = _get_table_columns(engine, "ET_STORE_STOCK")
    stk_cols_upper = {c.upper() for c in stk_cols}

    hier_select_parts = []
    has_mp_cols       = False
    for col in hier_cols:
        if col.upper() in mp_cols_upper:
            actual = mp_cols_upper[col.upper()]
            # Alias to ensure consistent column name after PIVOT
            hier_select_parts.append(f"MP.[{actual}] AS [{col}]")
            has_mp_cols = True
        elif col.upper() in stk_cols_upper:
            hier_select_parts.append(f"STK.[{col}]")
        else:
            hier_select_parts.append(f"STK.[{col}]")

    hier_select = ", ".join(hier_select_parts)
    mp_join     = ""
    if has_mp_cols:
        mp_join = "LEFT JOIN dbo.vw_master_product MP WITH (NOLOCK) ON STK.MATNR = MP.ARTICLE_NUMBER"

    # ── 4. Determine output columns & types for CREATE TABLE ─────────────────
    col_defs = ", ".join(f"[{c}] NVARCHAR(200) NULL" for c in hier_cols)
    col_defs += ", " + ", ".join(f"[{s}] NUMERIC(18,4) NULL" for s in slocs)
    col_defs += ", [STK_TTL] NUMERIC(18,4) NULL"

    all_cols  = ", ".join(f"[{c}]" for c in hier_cols) + \
                ", " + q_slocs + ", [STK_TTL]"

    # Expected columns: hierarchy cols + active SLOC cols + STK_TTL
    expected_cols_upper = {c.upper() for c in hier_cols} | {s.upper() for s in slocs} | {"STK_TTL"}

    with engine.connect() as conn:
        # Create output table if it doesn't exist
        _run(conn, f"""
            IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{out_table}')
            CREATE TABLE [{out_table}] ({col_defs})
        """)

        # Get existing columns in the output table
        existing_rows = conn.execute(text("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = :tbl
        """), {"tbl": out_table}).fetchall()
        existing_cols = {r[0].upper(): r[0] for r in existing_rows}

        # Add columns for newly active SLOCs (and any missing hierarchy/STK_TTL cols)
        for s in slocs:
            if s.upper() not in existing_cols:
                _run(conn, f"ALTER TABLE [{out_table}] ADD [{s}] NUMERIC(18,4) NULL")
        for c in hier_cols:
            if c.upper() not in existing_cols:
                _run(conn, f"ALTER TABLE [{out_table}] ADD [{c}] NVARCHAR(200) NULL")
        if "STK_TTL" not in existing_cols:
            _run(conn, f"ALTER TABLE [{out_table}] ADD [STK_TTL] NUMERIC(18,4) NULL")

        # Drop columns for inactive SLOCs (columns not in expected set)
        for col_upper, col_actual in existing_cols.items():
            if col_upper not in expected_cols_upper:
                _run(conn, f"ALTER TABLE [{out_table}] DROP COLUMN [{col_actual}]")

        # Truncate before inserting fresh data
        _run(conn, f"TRUNCATE TABLE [{out_table}]")

        # Check if ARS_pend_alc exists and PEND_ALC is an active SLOC
        pend_active = 'PEND_ALC' in [s.upper() for s in slocs]
        has_pend_table = False
        if pend_active:
            has_pend_table = conn.execute(text(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ARS_pend_alc'"
            )).scalar() > 0

        pend_union = ""
        if pend_active and has_pend_table:
            # Build hierarchy select for pend_alc — join MATNR with vw_master_product
            pend_hier_parts = []
            for col in hier_cols:
                cu = col.upper()
                if cu == 'WERKS':
                    pend_hier_parts.append(f"PA.[ST_CD] AS [{col}]")
                elif cu in mp_cols_upper:
                    actual = mp_cols_upper[cu]
                    pend_hier_parts.append(f"MP2.[{actual}] AS [{col}]")
                elif cu in stk_cols_upper:
                    pend_hier_parts.append(f"NULL AS [{col}]")
                else:
                    pend_hier_parts.append(f"NULL AS [{col}]")
            pend_hier_select = ", ".join(pend_hier_parts)

            pend_union = f"""
    UNION ALL
    SELECT
        {pend_hier_select},
        'PEND_ALC' AS SLOC,
        PA.QTY AS PARTICULARS_VALUE
    FROM dbo.ARS_pend_alc PA
    LEFT JOIN dbo.vw_master_product MP2 ON CAST(PA.MATNR AS NVARCHAR(50)) = MP2.ARTICLE_NUMBER
"""

        insert_sql = f""";
WITH Stock_CTE AS (
    SELECT
        {hier_select},
        STK.SLOC,
        STK.PARTICULARS_VALUE
    FROM dbo.ET_STORE_STOCK STK WITH (NOLOCK)
    {mp_join}
    INNER JOIN ARS_STORE_SLOC_SETTINGS S WITH (NOLOCK) ON STK.SLOC = S.SLOC
    WHERE UPPER(S.STATUS) = 'ACTIVE'{kpi_clause}
    {pend_union}
)
INSERT INTO [{out_table}] ({all_cols})
SELECT
    {', '.join(f'[{c}]' for c in hier_cols)},
    {isnull_cols},
    {sum_expr} AS STK_TTL
FROM Stock_CTE
PIVOT (
    SUM(PARTICULARS_VALUE)
    FOR SLOC IN ({q_slocs})
) AS P
ORDER BY {', '.join(f'[{c}]' for c in hier_cols)};
"""
        conn.execute(text(insert_sql))
        conn.commit()

        # ── 6. Post-pivot lookups ───────────────────────────────────────────
        lookup_warnings = _apply_post_lookups(conn, out_table, hier_cols)

        # ── 7. Grid-level MBQ & OPT_CNT (needs CONT from step 6)
        grid_calc_warnings = _calculate_grid_columns(conn, out_table)
        lookup_warnings.extend(grid_calc_warnings)

        # Count inserted rows
        count_row = conn.execute(text(f"SELECT COUNT(*) FROM [{out_table}]")).fetchone()
        row_count = count_row[0] if count_row else 0

    return {"rows": row_count, "error": None, "warnings": lookup_warnings}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/columns", response_model=APIResponse)
def get_columns(current_user: User = Depends(get_current_user)):
    """Return available columns from vw_master_product for hierarchy selection."""
    de = get_data_engine()
    cols = _get_master_product_columns(de)

    # Always include fallback columns even if view missing
    fallback = ["MATNR", "WERKS"]
    all_cols = list(dict.fromkeys(fallback + cols))  # deduplicate, preserve order

    return APIResponse(success=True, message=f"{len(all_cols)} columns available",
                       data={"columns": all_cols})


@router.get("/grids", response_model=APIResponse)
def list_grids(current_user: User = Depends(get_current_user)):
    de = get_data_engine()
    _ensure_grid_table(de)
    with de.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, grid_name, description, hierarchy_columns,
                   kpi_filter, output_table, status, seq,
                   created_at, updated_at,
                   last_run_at, last_run_status, last_run_rows, last_run_error, duration_sec
            FROM {GRID_TABLE}
            ORDER BY seq ASC, id ASC
        """)).fetchall()
    grids = [_row_to_dict(r) for r in rows]
    return APIResponse(success=True, message=f"{len(grids)} grid(s) found",
                       data={"grids": grids, "total": len(grids)})


def _validate_lookups(conn, hier_cols: List[str]) -> List[str]:
    """Check if all required lookup tables exist for the given hierarchy. Returns warnings."""
    warnings = []
    for cfg in POST_PIVOT_LOOKUPS:
        if not all(r in {c.upper() for c in hier_cols} for r in cfg["requires"]):
            continue
        lookup_table = _resolve_template(cfg["lookup_table"], hier_cols)
        exists = conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :tn"
        ), {"tn": lookup_table}).scalar() > 0
        if not exists:
            warnings.append(f"Lookup table '{lookup_table}' not found in DB")
            if "Master_CONT_" in cfg.get("lookup_table", ""):
                warnings.append(f"Contribution data will default to 1 ('{lookup_table}' missing)")
    return warnings


@router.post("/grids", response_model=APIResponse)
def create_grid(payload: GridCreate, current_user: User = Depends(get_current_user)):
    de = get_data_engine()
    _ensure_grid_table(de)
    hier_json = json.dumps(payload.hierarchy_columns)
    with de.connect() as conn:
        # Validate lookup tables for this hierarchy
        warnings = _validate_lookups(conn, payload.hierarchy_columns)
        warn_msg = ("⚠ " + "; ".join(warnings)) if warnings else None

        # Auto-assign next sequence
        max_seq = conn.execute(text(f"SELECT ISNULL(MAX(seq),0) FROM {GRID_TABLE}")).scalar() or 0
        conn.execute(text(f"""
            INSERT INTO {GRID_TABLE}
                (grid_name, description, hierarchy_columns, kpi_filter, output_table, status, seq, last_run_error, created_at, updated_at)
            VALUES
                (:name, :desc, :hier, :kpi, :out, :status, :seq, :warn, GETDATE(), GETDATE())
        """), {
            "name":   payload.grid_name,
            "desc":   payload.description,
            "hier":   hier_json,
            "kpi":    payload.kpi_filter,
            "out":    payload.output_table,
            "status": payload.status,
            "seq":    max_seq + 1,
            "warn":   warn_msg,
        })
        conn.commit()
    return APIResponse(success=True, message=f"Grid '{payload.grid_name}' created.",
                       data={"grid_name": payload.grid_name, "warnings": warnings})


@router.put("/grids/{grid_id}", response_model=APIResponse)
def update_grid(grid_id: int, payload: GridUpdate, current_user: User = Depends(get_current_user)):
    de = get_data_engine()
    _ensure_grid_table(de)

    # Build dynamic SET clause from non-None fields
    sets, params = [], {"id": grid_id}
    if payload.grid_name         is not None: sets.append("grid_name=:grid_name");         params["grid_name"]         = payload.grid_name
    if payload.description       is not None: sets.append("description=:description");     params["description"]       = payload.description
    if payload.hierarchy_columns is not None: sets.append("hierarchy_columns=:hier");      params["hier"]              = json.dumps(payload.hierarchy_columns)
    if payload.kpi_filter        is not None: sets.append("kpi_filter=:kpi_filter");       params["kpi_filter"]        = payload.kpi_filter
    if payload.output_table      is not None: sets.append("output_table=:output_table");   params["output_table"]      = payload.output_table.upper()
    if payload.status            is not None: sets.append("status=:status");               params["status"]            = payload.status
    if not sets:
        raise HTTPException(400, "No fields to update")
    sets.append("updated_at=GETDATE()")

    # Re-validate lookups if hierarchy changed
    hier_cols = payload.hierarchy_columns
    warnings = []
    if hier_cols:
        with de.connect() as conn:
            warnings = _validate_lookups(conn, hier_cols)
            warn_msg = ("⚠ " + "; ".join(warnings)) if warnings else None
            sets.append("last_run_error=:warn")
            params["warn"] = warn_msg

    with de.connect() as conn:
        conn.execute(text(f"UPDATE {GRID_TABLE} SET {', '.join(sets)} WHERE id=:id"), params)
        conn.commit()
    return APIResponse(success=True, message=f"Grid {grid_id} updated.", data={"id": grid_id, "warnings": warnings})


@router.delete("/grids/{grid_id}", response_model=APIResponse)
def delete_grid(grid_id: int, current_user: User = Depends(get_current_user)):
    de = get_data_engine()
    _ensure_grid_table(de)

    # Fetch the grid to get its output_table name
    with de.connect() as conn:
        row = conn.execute(text(f"SELECT output_table FROM {GRID_TABLE} WHERE id=:id"),
                           {"id": grid_id}).fetchone()
    if not row:
        raise HTTPException(404, f"Grid {grid_id} not found")

    out_table = row[0]

    with de.connect() as conn:
        # Drop the output table if it exists
        conn.execute(text(f"IF OBJECT_ID(:tbl, 'U') IS NOT NULL DROP TABLE [{out_table}]"),
                     {"tbl": out_table})
        # Delete the grid record
        conn.execute(text(f"DELETE FROM {GRID_TABLE} WHERE id=:id"), {"id": grid_id})
        conn.commit()

    return APIResponse(success=True,
        message=f"Grid {grid_id} and table [{out_table}] deleted.",
        data={"id": grid_id, "dropped_table": out_table})


@router.post("/grids/{grid_id}/run", response_model=APIResponse)
def run_grid(grid_id: int, current_user: User = Depends(get_current_user)):
    de = get_data_engine()
    _ensure_grid_table(de)

    with de.connect() as conn:
        row = conn.execute(text(f"""
            SELECT id, grid_name, description, hierarchy_columns,
                   kpi_filter, output_table, status, seq,
                   created_at, updated_at,
                   last_run_at, last_run_status, last_run_rows, last_run_error, duration_sec
            FROM {GRID_TABLE} WHERE id=:id
        """), {"id": grid_id}).fetchone()

    if not row:
        raise HTTPException(404, f"Grid {grid_id} not found")

    grid = _row_to_dict(row)

    # Build calc table ONCE before grid run
    calc_warns, calc_duration = _build_calc_table_once()

    res = _run_single_grid(grid)
    all_warns = calc_warns + res.get("warnings", [])

    if res["status"] == "Failed":
        raise HTTPException(500, detail=f"Grid run failed: {res['error']}")

    return APIResponse(success=True,
        message=f"Grid '{grid['grid_name']}' ran in {res.get('duration',0)}s. {res['rows']} rows → [{grid['output_table']}].",
        data={"rows_inserted": res["rows"], "output_table": grid["output_table"], "status": res["status"], "warnings": all_warns, "duration": res.get("duration", 0)})


@router.get("/calculation-preview", response_model=APIResponse)
def preview_calculations(current_user: User = Depends(get_current_user)):
    """Run the pre-grid calculation and return step-by-step logs with timing."""
    import time as _time
    de = get_data_engine()
    start = _time.time()
    with de.connect() as conn:
        steps = calculate_per_day_sale(conn)
    duration = round(_time.time() - start, 1)
    return APIResponse(success=True, message=f"{len(steps)} steps in {duration}s",
                       data={"steps": steps, "duration": duration})


@router.put("/reorder", response_model=APIResponse)
def reorder_grids(body: dict, current_user: User = Depends(get_current_user)):
    """Update sequence order for grids. Body: {sequence: [{id, seq}, ...]}"""
    seq_list = body.get("sequence", [])
    if not seq_list:
        raise HTTPException(400, detail="sequence list is required")
    de = get_data_engine()
    _ensure_grid_table(de)
    with de.connect() as conn:
        for item in seq_list:
            conn.execute(text(f"UPDATE {GRID_TABLE} SET seq=:seq, updated_at=GETDATE() WHERE id=:id"),
                         {"seq": item["seq"], "id": item["id"]})
        conn.commit()
    return APIResponse(success=True, message=f"Sequence updated for {len(seq_list)} grid(s)")


def _build_calc_table_once():
    """Build ARS_CALC_ST_MAJ_CAT once. Called before grid runs."""
    import time as _time
    de = get_data_engine()
    warnings = []
    start = _time.time()
    try:
        with de.connect() as conn:
            steps = calculate_per_day_sale(conn)
            for s in steps:
                logger.info(f"[Calc] {s['step']}: {s['detail']} ({s['status']})")
                if s["status"] == "error":
                    warnings.append(f"{s['step']}: {s['detail']}")
    except Exception as e:
        logger.warning(f"Calc table build failed: {e}")
        warnings.append(f"Calc table: {e}")
    duration = round(_time.time() - start, 1)
    logger.info(f"Pre-grid calc completed in {duration}s")
    return warnings, duration


def _run_single_grid(grid: dict) -> dict:
    """Run a single grid — used by both individual run and parallel run-all."""
    import time as _time
    de = get_data_engine()
    start = _time.time()

    # Mark running
    with de.connect() as conn:
        _run(conn, f"UPDATE {GRID_TABLE} SET last_run_status='Running', updated_at=GETDATE() WHERE id=:id",
             {"id": grid["id"]})

    try:
        result = _build_and_run_grid(de, grid)
        status = "Success" if not result["error"] else "Failed"
        err_msg = result["error"]
        n_rows = result["rows"]
        warn_list = result.get("warnings", [])

    except Exception as e:
        status = "Failed"
        err_msg = str(e)
        n_rows = 0
        warn_list = []
        logger.error(f"Grid {grid['id']} failed: {e}")

    # Store warnings in last_run_error
    stored_msg = err_msg
    if not stored_msg and warn_list:
        stored_msg = "⚠ " + "; ".join(warn_list)

    duration = round(_time.time() - start, 1)

    with de.connect() as conn:
        _run(conn, f"""
            UPDATE {GRID_TABLE}
            SET last_run_at=GETDATE(), last_run_status=:status,
                last_run_rows=:rows, last_run_error=:err, duration_sec=:dur, updated_at=GETDATE()
            WHERE id=:id
        """, {"status": status, "rows": n_rows, "err": stored_msg, "dur": duration, "id": grid["id"]})
    logger.info(f"Grid '{grid['grid_name']}' completed in {duration}s")

    return {"grid_name": grid["grid_name"], "status": status,
            "rows": n_rows, "error": err_msg, "warnings": warn_list, "duration": duration}


@router.post("/run-all", response_model=APIResponse)
def run_all_active(current_user: User = Depends(get_current_user)):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    de = get_data_engine()
    _ensure_grid_table(de)

    with de.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, grid_name, description, hierarchy_columns,
                   kpi_filter, output_table, status, seq,
                   created_at, updated_at,
                   last_run_at, last_run_status, last_run_rows, last_run_error, duration_sec
            FROM {GRID_TABLE} WHERE status='Active' ORDER BY seq ASC, id ASC
        """)).fetchall()

    active_grids = [_row_to_dict(r) for r in rows]
    if not active_grids:
        return APIResponse(success=True, message="No Active grids to run.", data={"results": []})

    # Build calc table ONCE before all grids
    calc_warns, calc_duration = _build_calc_table_once()
    logger.info(f"Calc table built once for {len(active_grids)} grids")

    # Run all grids in parallel (max 4 threads)
    results = []
    with ThreadPoolExecutor(max_workers=min(4, len(active_grids))) as pool:
        futures = {pool.submit(_run_single_grid, grid): grid for grid in active_grids}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                grid = futures[future]
                results.append({"grid_name": grid["grid_name"], "status": "Failed",
                                "rows": 0, "error": str(e), "warnings": []})

    # Sort results by original sequence order
    name_order = {g["grid_name"]: i for i, g in enumerate(active_grids)}
    results.sort(key=lambda r: name_order.get(r["grid_name"], 999))

    success_count = sum(1 for r in results if r["status"] == "Success")
    return APIResponse(success=True,
        message=f"Run All complete: {success_count}/{len(results)} grids succeeded.",
        data={"results": results})
