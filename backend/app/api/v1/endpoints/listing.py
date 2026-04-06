"""
Listing Module — Build ARS_LISTING master table
Combines MSA gen-art data with grid stock data and store-RDC mapping.

Endpoints:
  GET  /listing/config       — Get available RDCs, stores, tables
  POST /listing/generate     — Build ARS_LISTING table
  GET  /listing/preview      — Preview current ARS_LISTING data
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_engine
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/listing", tags=["Listing"])

LISTING_TABLE = "ARS_LISTING"


# ── Models ───────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    rdc_mode: str = "all"              # "own" | "cross" | "all"
    rdc_values: List[str] = []         # specific RDC(s) to filter
    msa_table: str = "ARS_MSA_GEN_ART" # source of gen-art master
    grid_table: str = "ARS_GRID_MJ_GEN_ART"  # source of stock data
    st_master_table: str = "Master_ALC_INPUT_ST_MASTER"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _run(conn, sql, params=None):
    conn.execute(text(sql), params or {})
    conn.commit()


def _table_exists(conn, tbl):
    return conn.execute(text(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :t"
    ), {"t": tbl}).scalar() > 0


def _get_columns(conn, tbl):
    rows = conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME = :t ORDER BY ORDINAL_POSITION"
    ), {"t": tbl}).fetchall()
    return [r[0] for r in rows]


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/config")
def get_config(current_user: User = Depends(get_current_user)):
    """Return available RDCs, store count, and table status."""
    de = get_data_engine()
    result = {"rdcs": [], "store_count": 0, "msa_gen_art_rows": 0,
              "grid_gen_art_rows": 0, "listing_rows": 0, "listing_exists": False}

    with de.connect() as conn:
        # Get RDC values from MSA table (ST_CD renamed to RDC)
        if _table_exists(conn, "ARS_MSA_GEN_ART"):
            cols = _get_columns(conn, "ARS_MSA_GEN_ART")
            rdc_col = "RDC" if "RDC" in cols else "ST_CD" if "ST_CD" in cols else None
            if rdc_col:
                rdcs = conn.execute(text(
                    f"SELECT DISTINCT [{rdc_col}] FROM [ARS_MSA_GEN_ART] "
                    f"WHERE [{rdc_col}] IS NOT NULL ORDER BY [{rdc_col}]"
                )).fetchall()
                result["rdcs"] = [r[0] for r in rdcs]
            result["msa_gen_art_rows"] = conn.execute(text(
                "SELECT COUNT(*) FROM [ARS_MSA_GEN_ART]"
            )).scalar()

        # Grid table rows
        if _table_exists(conn, "ARS_GRID_MJ_GEN_ART"):
            result["grid_gen_art_rows"] = conn.execute(text(
                "SELECT COUNT(*) FROM [ARS_GRID_MJ_GEN_ART]"
            )).scalar()

        # Store count from ST_MASTER
        if _table_exists(conn, "Master_ALC_INPUT_ST_MASTER"):
            result["store_count"] = conn.execute(text(
                "SELECT COUNT(DISTINCT [ST_CD]) FROM [Master_ALC_INPUT_ST_MASTER] "
                "WHERE ISNULL([LISTING], 1) = 1"
            )).scalar()

        # Listing table
        if _table_exists(conn, LISTING_TABLE):
            result["listing_exists"] = True
            result["listing_rows"] = conn.execute(text(
                f"SELECT COUNT(*) FROM [{LISTING_TABLE}]"
            )).scalar()

    return {"success": True, "data": result}


@router.post("/generate")
def generate_listing(req: GenerateRequest, current_user: User = Depends(get_current_user)):
    """Build ARS_LISTING table from MSA gen-art + grid stock + store master."""
    import time
    start = time.time()
    de = get_data_engine()

    with de.connect() as conn:
        # ── Validate source tables exist ────────────────────────────────
        for tbl in [req.msa_table, req.grid_table, req.st_master_table]:
            if not _table_exists(conn, tbl):
                raise HTTPException(400, f"Table '{tbl}' not found in database")

        msa_cols = _get_columns(conn, req.msa_table)
        grid_cols = _get_columns(conn, req.grid_table)
        st_cols = _get_columns(conn, req.st_master_table)

        # Detect column names
        msa_rdc_col = "RDC" if "RDC" in msa_cols else "ST_CD"
        msa_majcat = "MAJ_CAT" if "MAJ_CAT" in msa_cols else None
        msa_genart = "GEN_ART_NUMBER" if "GEN_ART_NUMBER" in msa_cols else None
        msa_clr = "CLR" if "CLR" in msa_cols else None

        if not all([msa_majcat, msa_genart, msa_clr]):
            raise HTTPException(400, f"MSA table missing required columns (MAJ_CAT, GEN_ART_NUMBER, CLR)")

        # Grid key columns
        grid_has_werks = "WERKS" in grid_cols
        grid_has_majcat = "MAJ_CAT" in grid_cols
        grid_has_genart = "GEN_ART_NUMBER" in grid_cols
        grid_has_clr = "CLR" in grid_cols

        if not all([grid_has_werks, grid_has_majcat, grid_has_genart, grid_has_clr]):
            raise HTTPException(400, f"Grid table missing required columns (WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR)")

        # Stock columns from grid (everything except hierarchy keys and system cols)
        grid_key_cols = {"WERKS", "MAJ_CAT", "GEN_ART_NUMBER", "CLR", "STK_TTL", "IS_NEW",
                         "CONT", "MBQ", "OPT_CNT", "LISTING"}
        stock_cols = [c for c in grid_cols if c not in grid_key_cols]

        # ST_MASTER RDC column detection
        # Check for RDC-like columns in ST_MASTER
        st_rdc_col = None
        for candidate in ["RDC", "WAREHOUSE", "HUB", "WH_CD"]:
            if candidate in st_cols:
                st_rdc_col = candidate
                break

        logger.info(f"Listing config: MSA RDC={msa_rdc_col}, ST_MASTER RDC={st_rdc_col}, "
                     f"stock_cols={len(stock_cols)}")

        # ── Step 1: Get unique gen-art combos from MSA ──────────────────
        # Distinct MAJ_CAT + GEN_ART_NUMBER + CLR (one row per RDC combo)
        msa_genart_sql = f"""
            SELECT DISTINCT [{msa_rdc_col}] AS RDC, [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
            FROM [{req.msa_table}]
            WHERE [MAJ_CAT] IS NOT NULL AND [GEN_ART_NUMBER] IS NOT NULL
        """

        # ── Step 2: Get active stores ───────────────────────────────────
        has_listing_col = "LISTING" in st_cols
        listing_filter = ""
        if has_listing_col:
            listing_filter = " WHERE ISNULL(CAST([LISTING] AS NVARCHAR(10)), '1') NOT IN ('0', 'N', 'n')"

        stores_sql = f"""
            SELECT DISTINCT [ST_CD]
                   {f', [{st_rdc_col}] AS STORE_RDC' if st_rdc_col else ''}
            FROM [{req.st_master_table}]{listing_filter}
        """

        # ── Step 3: Build listing table ─────────────────────────────────
        # Drop and recreate
        _run(conn, f"IF OBJECT_ID('{LISTING_TABLE}','U') IS NOT NULL DROP TABLE [{LISTING_TABLE}]")

        # Build stock column definitions (FLOAT for all)
        stock_col_defs = ", ".join(f"[{c}] FLOAT NULL DEFAULT 0" for c in stock_cols)
        stock_col_defs_str = f", {stock_col_defs}" if stock_col_defs else ""

        create_sql = f"""
            CREATE TABLE [{LISTING_TABLE}] (
                [WERKS] NVARCHAR(50),
                [RDC] NVARCHAR(50),
                {f'[STORE_RDC] NVARCHAR(50),' if st_rdc_col else ''}
                [MAJ_CAT] NVARCHAR(100),
                [GEN_ART_NUMBER] NVARCHAR(100),
                [CLR] NVARCHAR(100)
                {stock_col_defs_str},
                [STK_TTL] FLOAT NULL DEFAULT 0,
                [IS_NEW] BIT NOT NULL DEFAULT 1
            )
        """
        try:
            _run(conn, create_sql)
        except Exception as e:
            logger.error(f"CREATE TABLE failed: {e}\nSQL: {create_sql}")
            raise HTTPException(500, f"Failed to create listing table: {str(e)[:200]}")
        logger.info(f"Created {LISTING_TABLE} with {len(stock_cols)} stock columns")

        # ── Step 4: Cross join stores x MSA gen-arts, insert ────────────
        stock_isnull = ", ".join(f"ISNULL(G.[{c}], 0) AS [{c}]" for c in stock_cols)
        stock_isnull_str = f", {stock_isnull}" if stock_isnull else ""

        stk_ttl_expr = " + ".join(f"ISNULL(G.[{c}], 0)" for c in stock_cols) if stock_cols else "0"

        stock_insert_cols = ", ".join(f"[{c}]" for c in stock_cols)
        stock_insert_cols_str = f", {stock_insert_cols}" if stock_insert_cols else ""

        all_insert_cols = f"[WERKS], [RDC], {'[STORE_RDC], ' if st_rdc_col else ''}[MAJ_CAT], [GEN_ART_NUMBER], [CLR]{stock_insert_cols_str}, [STK_TTL], [IS_NEW]"

        insert_sql = f"""
            INSERT INTO [{LISTING_TABLE}] ({all_insert_cols})
            SELECT
                S.[ST_CD] AS WERKS,
                M.[RDC],
                {f'S.[STORE_RDC],' if st_rdc_col else ''}
                M.[MAJ_CAT],
                M.[GEN_ART_NUMBER],
                M.[CLR]
                {stock_isnull_str},
                {stk_ttl_expr} AS STK_TTL,
                CASE WHEN G.[WERKS] IS NULL THEN 1 ELSE 0 END AS IS_NEW
            FROM (
                {msa_genart_sql}
            ) M
            CROSS JOIN (
                {stores_sql}
            ) S
            LEFT JOIN [{req.grid_table}] G WITH (NOLOCK)
                ON G.[WERKS] = S.[ST_CD]
                AND G.[MAJ_CAT] = M.[MAJ_CAT]
                AND G.[GEN_ART_NUMBER] = M.[GEN_ART_NUMBER]
                AND G.[CLR] = M.[CLR]
        """

        # Apply RDC filter
        if req.rdc_mode == "own" and req.rdc_values and st_rdc_col:
            rdc_list = ", ".join(f"'{v}'" for v in req.rdc_values)
            insert_sql += f"\n            WHERE S.[STORE_RDC] IN ({rdc_list})"
        elif req.rdc_mode == "cross" and req.rdc_values and st_rdc_col:
            rdc_list = ", ".join(f"'{v}'" for v in req.rdc_values)
            insert_sql += f"\n            WHERE S.[STORE_RDC] NOT IN ({rdc_list})"
        elif req.rdc_values:
            rdc_list = ", ".join(f"'{v}'" for v in req.rdc_values)
            insert_sql += f"\n            WHERE M.[RDC] IN ({rdc_list})"

        logger.info(f"Executing listing insert SQL ({len(insert_sql)} chars)...")
        logger.debug(f"INSERT SQL:\n{insert_sql}")
        try:
            _run(conn, insert_sql)
        except Exception as e:
            logger.error(f"Listing INSERT failed: {e}")
            logger.error(f"SQL was:\n{insert_sql}")
            raise HTTPException(500, f"Listing generation failed: {str(e)[:300]}")

        # Count results
        total = conn.execute(text(f"SELECT COUNT(*) FROM [{LISTING_TABLE}]")).scalar()
        new_count = conn.execute(text(f"SELECT COUNT(*) FROM [{LISTING_TABLE}] WHERE [IS_NEW] = 1")).scalar()
        existing = total - new_count

        # Add indexes for performance
        try:
            _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_WERKS ON [{LISTING_TABLE}]([WERKS])")
            _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_RDC ON [{LISTING_TABLE}]([RDC])")
            _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_GENART ON [{LISTING_TABLE}]([MAJ_CAT],[GEN_ART_NUMBER],[CLR])")
        except Exception:
            pass

    duration = round(time.time() - start, 1)
    logger.info(f"ARS_LISTING built: {total} rows ({existing} existing + {new_count} new) in {duration}s")

    return {
        "success": True,
        "message": f"Listing generated: {total:,} rows ({existing:,} with stock + {new_count:,} new from MSA) in {duration}s",
        "data": {
            "total_rows": total,
            "existing_rows": existing,
            "new_rows": new_count,
            "duration_sec": duration,
            "stock_columns": len(stock_cols),
        }
    }


@router.get("/preview")
def preview_listing(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=5000),
    rdc: Optional[str] = None,
    werks: Optional[str] = None,
    maj_cat: Optional[str] = None,
    is_new: Optional[int] = None,
    current_user: User = Depends(get_current_user),
):
    """Preview ARS_LISTING data with filters and pagination."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, LISTING_TABLE):
            raise HTTPException(404, "ARS_LISTING table not found. Generate it first.")

        cols = _get_columns(conn, LISTING_TABLE)

        where = []
        params = {}
        if rdc:
            where.append("[RDC] = :rdc"); params["rdc"] = rdc
        if werks:
            where.append("[WERKS] = :werks"); params["werks"] = werks
        if maj_cat:
            where.append("[MAJ_CAT] = :maj_cat"); params["maj_cat"] = maj_cat
        if is_new is not None:
            where.append("[IS_NEW] = :is_new"); params["is_new"] = is_new

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        total = conn.execute(text(
            f"SELECT COUNT(*) FROM [{LISTING_TABLE}]{where_sql}"
        ), params).scalar()

        col_list = ", ".join(f"[{c}]" for c in cols)
        offset = (page - 1) * page_size
        rows = conn.execute(text(f"""
            SELECT {col_list}
            FROM [{LISTING_TABLE}]{where_sql}
            ORDER BY [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
            OFFSET :off ROWS FETCH NEXT :ps ROWS ONLY
        """), {**params, "off": offset, "ps": page_size}).fetchall()

        data = [dict(zip(cols, row)) for row in rows]

    return {
        "success": True,
        "data": {"data": data, "total": total, "columns": cols, "page": page, "page_size": page_size}
    }


@router.get("/summary")
def listing_summary(current_user: User = Depends(get_current_user)):
    """Summary stats for ARS_LISTING."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, LISTING_TABLE):
            return {"success": True, "data": None}

        summary = {}

        # By RDC
        rows = conn.execute(text(f"""
            SELECT [RDC], COUNT(*) AS cnt,
                   SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END) AS new_cnt,
                   SUM(CASE WHEN [IS_NEW] = 0 THEN 1 ELSE 0 END) AS existing_cnt
            FROM [{LISTING_TABLE}]
            GROUP BY [RDC] ORDER BY [RDC]
        """)).fetchall()
        summary["by_rdc"] = [{"rdc": r[0], "total": r[1], "new": r[2], "existing": r[3]} for r in rows]

        # By MAJ_CAT
        rows = conn.execute(text(f"""
            SELECT TOP 20 [MAJ_CAT], COUNT(*) AS cnt,
                   SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END) AS new_cnt
            FROM [{LISTING_TABLE}]
            GROUP BY [MAJ_CAT] ORDER BY cnt DESC
        """)).fetchall()
        summary["by_maj_cat"] = [{"maj_cat": r[0], "total": r[1], "new": r[2]} for r in rows]

        # Totals
        row = conn.execute(text(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END) AS new_cnt,
                   COUNT(DISTINCT [WERKS]) AS stores,
                   COUNT(DISTINCT [RDC]) AS rdcs,
                   COUNT(DISTINCT [MAJ_CAT] + '|' + [GEN_ART_NUMBER] + '|' + [CLR]) AS gen_arts
            FROM [{LISTING_TABLE}]
        """)).fetchone()
        summary["totals"] = {
            "total": row[0], "new": row[1], "existing": row[0] - row[1],
            "stores": row[2], "rdcs": row[3], "gen_arts": row[4]
        }

    return {"success": True, "data": summary}
