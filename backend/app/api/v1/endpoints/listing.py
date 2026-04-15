"""
Listing Module — Build ARS_LISTING master table (Data Preparation)
Combines MSA gen-art data with grid stock data and store-RDC mapping.
Includes BOTH MSA-recommended gen-arts AND existing grid gen-arts.

RDC Modes:
  All       — all stores, all RDC options
  Own RDC   — stores tagged to selected RDC, unique options from that RDC only
  Cross RDC — take options FROM one RDC, send TO stores of another RDC

Endpoints:
  GET  /listing/config       — RDCs (from ST_MASTER), stores, MAJ_CATs, table status
  POST /listing/generate     — Build ARS_LISTING (MSA + grid unique options)
  GET  /listing/preview      — Preview with column filters & pagination
  GET  /listing/summary      — Summary stats
  GET  /listing/export       — Export to Excel
"""
import io
import json
import time

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import text
from loguru import logger

from app.database.session import get_data_engine
from app.security.dependencies import get_current_user
from app.models.rbac import User
from app.utils.db_helpers import (
    run_sql, table_exists, get_columns, msa_expr, msa_col,
)

router = APIRouter(prefix="/listing", tags=["Listing"])

LISTING_TABLE = "ARS_LISTING"
FINAL_TABLE   = "ARS_LISTING_WORKING"
ALLOC_TABLE   = "ARS_ALLOC_WORKING"

# Columns to KEEP in the final table (identity + calculated outputs).
# Everything else (SLOC stock columns, Part 4 grid-prefix columns) is skipped.
_FINAL_KEEP_COLS = {
    "WERKS", "RDC", "MAJ_CAT", "GEN_ART_NUMBER", "CLR", "GEN_ART_DESC",
    "STK_TTL", "IS_NEW", "OPT_TYPE",
    "DPN", "SAL_D", "AUTO_GEN_ART_SALE", "AGE",
    "MSA_FNL_Q", "VAR_COUNT", "VAR_FNL_COUNT",
    "PER_OPT_SALE", "OPT_MBQ", "OPT_REQ", "OPT_MBQ_WH", "OPT_REQ_WH", "EXCESS_STK",
    "ST_RANK", "MAX_DAILY_SALE",
}
# Pattern: columns ending with _REQ are always kept (MJ_REQ, RNG_SEG_REQ, etc.)
_FINAL_KEEP_SUFFIX = {"_REQ"}


# ── Models ───────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    rdc_mode: str = "all"              # "own" | "cross" | "all"
    rdc_values: List[str] = []         # Own RDC: selected RDC(s)
    cross_from: List[str] = []         # Cross RDC: take options FROM these RDCs
    cross_to: List[str] = []           # Cross RDC: send TO stores of these RDCs
    store_codes: List[str] = []        # selected stores (empty = all active)
    maj_cat_values: List[str] = []     # selected MAJ_CATs (empty = all)
    run_mode: str = "listing"          # "listing" | "full" (full = MSA+Grid+Listing)
    # MIX aggregation mode:
    #   "st_maj_rng" = 1 line per (WERKS, MAJ_CAT, RNG_SEG) — DEFAULT (finer)
    #   "st_maj"     = 1 line per (WERKS, MAJ_CAT)          — coarser, rolls everything together
    #   "each"       = keep each MIX row as-is (only tag, no aggregation)
    mix_mode: str = "st_maj_rng"
    # Configurable variables (editable from UI):
    stock_threshold_pct: float = 0.6   # OPT_TYPE: RL when STK >= X% of DPN (default 60%)
    excess_multiplier: float = 2.0     # EXCESS: STK > X × OPT_MBQ is excess (default 2×)
    hold_days: int = 0                 # OPT_MBQ_WH: extra days added to SAL_D for IS_NEW=1 only
    age_threshold: int = 15            # Articles with AGE < X use PER_OPT_SALE in OPT_MBQ
    req_weight: float = 0.4            # Store ranking: weight for requirement rank
    fill_weight: float = 0.6           # Store ranking: weight for fill rate rank
    # Source tables:
    msa_table: str = "ARS_MSA_GEN_ART"
    grid_table: str = "ARS_GRID_MJ_GEN_ART"
    st_master_table: str = "Master_ALC_INPUT_ST_MASTER"


# ── Helpers — delegating to shared db_helpers ───────────────────────────────

_run = run_sql
_table_exists = table_exists
_get_columns = get_columns
_msa_expr = msa_expr
_msa_col = msa_col


def _build_filter_where(filters_json, valid_cols, existing_where_parts=None):
    """Parse column filters JSON and build WHERE clauses."""
    where = list(existing_where_parts or [])
    params = {}
    if not filters_json:
        return where, params
    try:
        filters = json.loads(filters_json)
    except Exception:
        return where, params
    for col, val in filters.items():
        if col in valid_cols and val:
            safe_key = col.replace(" ", "_").replace("-", "_")
            where.append(f"CAST([{col}] AS NVARCHAR(MAX)) LIKE :f_{safe_key}")
            params[f"f_{safe_key}"] = f"%{val}%"
    return where, params


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/config")
def get_config(current_user: User = Depends(get_current_user)):
    """Return available RDCs (from ST_MASTER), stores, MAJ_CATs, and table status."""
    de = get_data_engine()
    result = {"rdcs": [], "stores": [], "maj_cats": [], "store_count": 0,
              "msa_gen_art_rows": 0, "grid_gen_art_rows": 0,
              "listing_rows": 0, "listing_exists": False}

    with de.connect() as conn:
        if _table_exists(conn, "Master_ALC_INPUT_ST_MASTER"):
            st_cols = _get_columns(conn, "Master_ALC_INPUT_ST_MASTER")
            st_rdc_col = None
            for candidate in ["RDC", "WAREHOUSE", "HUB", "WH_CD"]:
                if candidate in st_cols:
                    st_rdc_col = candidate
                    break

            has_listing_col = "LISTING" in st_cols
            listing_filter = ""
            if has_listing_col:
                listing_filter = " WHERE ISNULL(CAST([LISTING] AS NVARCHAR(10)), '1') NOT IN ('0', 'N', 'n')"

            if st_rdc_col:
                rdcs = conn.execute(text(
                    f"SELECT DISTINCT [{st_rdc_col}] FROM [Master_ALC_INPUT_ST_MASTER] "
                    f"WHERE [{st_rdc_col}] IS NOT NULL ORDER BY [{st_rdc_col}]"
                )).fetchall()
                result["rdcs"] = [str(r[0]).strip() for r in rdcs if r[0]]

            result["store_count"] = conn.execute(text(
                f"SELECT COUNT(DISTINCT [ST_CD]) FROM [Master_ALC_INPUT_ST_MASTER]{listing_filter}"
            )).scalar()

            stores = conn.execute(text(
                f"SELECT DISTINCT [ST_CD] FROM [Master_ALC_INPUT_ST_MASTER]{listing_filter} ORDER BY [ST_CD]"
            )).fetchall()
            result["stores"] = [str(r[0]).strip() for r in stores if r[0]]

            # Store → RDC mapping (for auto RDC detection in frontend)
            if st_rdc_col:
                store_rdc_rows = conn.execute(text(
                    f"SELECT DISTINCT [ST_CD], [{st_rdc_col}] FROM [Master_ALC_INPUT_ST_MASTER]{listing_filter}"
                )).fetchall()
                result["store_rdc_map"] = {str(r[0]).strip(): str(r[1]).strip() for r in store_rdc_rows if r[0] and r[1]}

        if _table_exists(conn, "ARS_MSA_GEN_ART"):
            result["msa_gen_art_rows"] = conn.execute(text(
                "SELECT COUNT(*) FROM [ARS_MSA_GEN_ART]"
            )).scalar()
            maj_cats = conn.execute(text(
                "SELECT DISTINCT [MAJ_CAT] FROM [ARS_MSA_GEN_ART] "
                "WHERE [MAJ_CAT] IS NOT NULL ORDER BY [MAJ_CAT]"
            )).fetchall()
            result["maj_cats"] = [str(r[0]).strip() for r in maj_cats if r[0]]

        if _table_exists(conn, "ARS_GRID_MJ_GEN_ART"):
            result["grid_gen_art_rows"] = conn.execute(text(
                "SELECT COUNT(*) FROM [ARS_GRID_MJ_GEN_ART]"
            )).scalar()

        if _table_exists(conn, LISTING_TABLE):
            result["listing_exists"] = True
            result["listing_rows"] = conn.execute(text(
                f"SELECT COUNT(*) FROM [{LISTING_TABLE}]"
            )).scalar()

        # Load saved listing variables from AppSettings
        result["settings"] = _load_listing_settings(conn)

    return {"success": True, "data": result}


# ── Listing Settings (persisted in AppSettings table) ──────────────────────

_SETTING_DEFAULTS = {
    "stock_threshold_pct": "0.6",
    "excess_multiplier": "2.0",
    "hold_days": "0",
    "age_threshold": "15",
    "mix_mode": "st_maj_rng",
    "rdc_mode": "all",
    "run_mode": "listing",
    "req_weight": "0.4",
    "fill_weight": "0.6",
}
_SETTING_PREFIX = "listing."


def _load_listing_settings(conn) -> dict:
    """Load listing_* keys from AppSettings, return as dict with defaults."""
    settings = dict(_SETTING_DEFAULTS)
    if not table_exists(conn, "AppSettings"):
        return settings
    rows = conn.execute(text(
        "SELECT setting_key, setting_value FROM AppSettings WHERE setting_key LIKE :pfx"
    ), {"pfx": f"{_SETTING_PREFIX}%"}).fetchall()
    for key, val in rows:
        short = key.replace(_SETTING_PREFIX, "", 1)
        if short in settings:
            settings[short] = val
    return settings


def _save_listing_settings(conn, settings: dict):
    """Upsert listing_* keys into AppSettings."""
    if not table_exists(conn, "AppSettings"):
        return
    for key, val in settings.items():
        if key not in _SETTING_DEFAULTS:
            continue
        full_key = f"{_SETTING_PREFIX}{key}"
        existing = conn.execute(text(
            "SELECT COUNT(*) FROM AppSettings WHERE setting_key = :k"
        ), {"k": full_key}).scalar()
        if existing:
            conn.execute(text(
                "UPDATE AppSettings SET setting_value = :v, updated_at = GETDATE() WHERE setting_key = :k"
            ), {"k": full_key, "v": str(val)})
        else:
            conn.execute(text(
                "INSERT INTO AppSettings (setting_key, setting_value, updated_at) VALUES (:k, :v, GETDATE())"
            ), {"k": full_key, "v": str(val)})
    conn.commit()


@router.post("/settings")
def save_listing_settings(body: dict, current_user: User = Depends(get_current_user)):
    """Save listing variables to AppSettings for persistence."""
    de = get_data_engine()
    with de.connect() as conn:
        _save_listing_settings(conn, body)
    return {"success": True, "data": body}


@router.post("/generate")
def generate_listing(req: GenerateRequest, current_user: User = Depends(get_current_user)):
    """Build ARS_LISTING = Grid data + MSA missing options.

    run_mode: "listing" = generate listing only, "full" = MSA calc → Grid build → Listing
    """
    start = time.time()
    de = get_data_engine()

    # Auto-save current variables to DB for next session
    try:
        with de.connect() as sc:
            _save_listing_settings(sc, {
                "stock_threshold_pct": str(req.stock_threshold_pct),
                "excess_multiplier": str(req.excess_multiplier),
                "hold_days": str(req.hold_days),
                "age_threshold": str(req.age_threshold),
                "mix_mode": req.mix_mode,
                "rdc_mode": req.rdc_mode,
                "run_mode": req.run_mode,
                "req_weight": str(req.req_weight),
                "fill_weight": str(req.fill_weight),
            })
    except Exception:
        pass  # non-critical

    # ── Full pipeline: MSA calc → Grid build → Listing ──────────────
    pipeline_msg = ""
    if req.run_mode == "full":
        try:
            from app.services.grid_calculations import calculate_per_day_sale
            from app.api.v1.endpoints.grid_builder import _build_and_run_grid
            from concurrent.futures import ThreadPoolExecutor

            # Step A: Pre-grid calculations
            with de.connect() as pc:
                calc_result = calculate_per_day_sale(pc)
                logger.info(f"Full pipeline: pre-grid calc done")

            # Step B: Run all active grids in parallel
            with de.connect() as gc:
                if _table_exists(gc, "ARS_GRID_BUILDER"):
                    grids = gc.execute(text(
                        "SELECT * FROM [ARS_GRID_BUILDER] WHERE UPPER(status)='ACTIVE' ORDER BY seq"
                    )).fetchall()
                    grid_cols_meta = [d[0] for d in gc.execute(text(
                        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='ARS_GRID_BUILDER' ORDER BY ORDINAL_POSITION"
                    )).fetchall()]
                    grid_dicts = [dict(zip(grid_cols_meta, g)) for g in grids]

                    def run_grid(g):
                        return _build_and_run_grid(de, g)

                    with ThreadPoolExecutor(max_workers=4) as pool:
                        results = list(pool.map(run_grid, grid_dicts))
                    logger.info(f"Full pipeline: {len(results)} grids completed")
                    pipeline_msg = f"Full pipeline: calc + {len(results)} grids | "
        except Exception as e:
            logger.error(f"Full pipeline error: {e}")
            pipeline_msg = f"Pipeline partial (error: {str(e)[:80]}) | "

    # Step timing collector
    step_timings = []
    def _time_step(label, t0):
        dt = round(time.time() - t0, 1)
        step_timings.append({"step": label, "seconds": dt})
        logger.info(f"⏱ {label}: {dt}s")
        return time.time()

    with de.connect() as conn:
        for tbl in [req.msa_table, req.grid_table, req.st_master_table]:
            if not _table_exists(conn, tbl):
                raise HTTPException(400, f"Table '{tbl}' not found")

        msa_cols = _get_columns(conn, req.msa_table)
        grid_cols = _get_columns(conn, req.grid_table)
        st_cols = _get_columns(conn, req.st_master_table)

        msa_rdc_col = "RDC" if "RDC" in msa_cols else "ST_CD"
        if not all(c in msa_cols for c in ["MAJ_CAT", "GEN_ART_NUMBER", "CLR"]):
            raise HTTPException(400, "MSA table missing MAJ_CAT, GEN_ART_NUMBER, CLR")
        if not all(c in grid_cols for c in ["WERKS", "MAJ_CAT", "GEN_ART_NUMBER", "CLR"]):
            raise HTTPException(400, "Grid table missing WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR")

        # Stock columns = pivot data only (exclude system/calc cols)
        skip_cols = {"WERKS", "MAJ_CAT", "GEN_ART_NUMBER", "CLR", "STK_TTL", "IS_NEW",
                     "CONT", "MBQ", "OPT_CNT", "LISTING"}
        stock_cols = [c for c in grid_cols if c not in skip_cols]

        # ST_MASTER RDC column
        st_rdc_col = None
        for c in ["RDC", "WAREHOUSE", "HUB", "WH_CD"]:
            if c in st_cols:
                st_rdc_col = c
                break
        if not st_rdc_col:
            raise HTTPException(400, "ST_MASTER missing RDC column")

        # ── Filters ─────────────────────────────────────────────────────
        mc_where = ""
        if req.maj_cat_values:
            mc_list = ", ".join(f"'{v}'" for v in req.maj_cat_values)
            mc_where = f" AND [MAJ_CAT] IN ({mc_list})"

        # Active stores
        has_listing = "LISTING" in st_cols
        st_parts = []
        if has_listing:
            st_parts.append("ISNULL(CAST([LISTING] AS NVARCHAR(10)), '1') NOT IN ('0', 'N', 'n')")
        if req.store_codes:
            st_parts.append(f"[ST_CD] IN ({', '.join(f'{chr(39)}{v}{chr(39)}' for v in req.store_codes)})")

        # ── Stores SQL based on RDC mode ────────────────────────────────
        def _stores_sql(rdc_filter_list=None):
            parts = list(st_parts)
            if rdc_filter_list:
                rl = ", ".join(f"'{v}'" for v in rdc_filter_list)
                parts.append(f"[{st_rdc_col}] IN ({rl})")
            w = (" WHERE " + " AND ".join(parts)) if parts else ""
            return f"SELECT DISTINCT [ST_CD], [{st_rdc_col}] AS RDC FROM [{req.st_master_table}]{w}"

        # ── MSA option filter based on RDC mode ─────────────────────────
        # MSA stores all columns as VARCHAR(MAX) — must TRIM RDC for matching
        if req.rdc_mode == "own" and req.rdc_values:
            stores_sql = _stores_sql(req.rdc_values)
            rl = ", ".join(f"'{v}'" for v in req.rdc_values)
            msa_rdc_filter = f" AND LTRIM(RTRIM(CAST([{msa_rdc_col}] AS NVARCHAR(100)))) IN ({rl})"
        elif req.rdc_mode == "cross" and req.cross_from:
            stores_sql = _stores_sql(req.cross_to if req.cross_to else None)
            fl = ", ".join(f"'{v}'" for v in req.cross_from)
            msa_rdc_filter = f" AND LTRIM(RTRIM(CAST([{msa_rdc_col}] AS NVARCHAR(100)))) IN ({fl})"
        else:
            stores_sql = _stores_sql()
            msa_rdc_filter = ""

        # ── MSA unique options (proper types + RDC filtered) ──────────────
        msa_sql = f"""
            SELECT DISTINCT {_msa_col('MAJ_CAT')}, {_msa_col('GEN_ART_NUMBER')}, {_msa_col('CLR')}
            FROM [{req.msa_table}]
            WHERE [MAJ_CAT] IS NOT NULL AND [GEN_ART_NUMBER] IS NOT NULL{mc_where}{msa_rdc_filter}
        """

        # ── Create listing table ────────────────────────────────────────
        _run(conn, f"IF OBJECT_ID('{LISTING_TABLE}','U') IS NOT NULL DROP TABLE [{LISTING_TABLE}]")

        stk_defs = ", ".join(f"[{c}] FLOAT NULL DEFAULT 0" for c in stock_cols)
        stk_defs_str = f", {stk_defs}" if stk_defs else ""
        _run(conn, f"""
            CREATE TABLE [{LISTING_TABLE}] (
                [WERKS] NVARCHAR(50),
                [RDC] NVARCHAR(50),
                [MAJ_CAT] NVARCHAR(100),
                [GEN_ART_NUMBER] BIGINT NULL,
                [CLR] NVARCHAR(100)
                {stk_defs_str},
                [STK_TTL] FLOAT NULL DEFAULT 0,
                [IS_NEW] BIT NOT NULL DEFAULT 0,
                [OPT_TYPE] NVARCHAR(10) NULL
            )
        """)

        # ── SQL fragments ───────────────────────────────────────────────
        stk_sel = ", ".join(f"ISNULL(G.[{c}], 0) AS [{c}]" for c in stock_cols)
        stk_sel_str = f", {stk_sel}" if stk_sel else ""
        stk_ttl = " + ".join(f"ISNULL(G.[{c}], 0)" for c in stock_cols) if stock_cols else "0"
        stk_ins = ", ".join(f"[{c}]" for c in stock_cols)
        stk_ins_str = f", {stk_ins}" if stk_ins else ""
        stk_zeros = ", ".join("0" for _ in stock_cols)
        stk_zeros_str = f", {stk_zeros}" if stk_zeros else ""

        all_cols = f"[WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]{stk_ins_str}, [STK_TTL], [IS_NEW], [OPT_TYPE]"

        # ── Diagnostic: source counts ───────────────────────────────────
        diag_stores = conn.execute(text(f"SELECT COUNT(*) FROM ({stores_sql}) X")).scalar()
        grid_mc = f"AND [MAJ_CAT] IN ({', '.join(f'{chr(39)}{v}{chr(39)}' for v in req.maj_cat_values)})" if req.maj_cat_values else ""
        diag_grid = conn.execute(text(
            f"SELECT COUNT(*) FROM [{req.grid_table}] WITH (NOLOCK) "
            f"WHERE [WERKS] IN (SELECT [ST_CD] FROM ({stores_sql}) X) {grid_mc}"
        )).scalar()
        diag_msa = conn.execute(text(f"SELECT COUNT(*) FROM ({msa_sql}) X")).scalar()
        logger.info(f"Diagnostic: stores={diag_stores}, grid_rows={diag_grid}, msa_options={diag_msa}")

        # ── PART 1: Grid data (existing stock) → IS_NEW = 0 ────────────
        t0 = time.time()
        grid_mc_g = f"AND G.[MAJ_CAT] IN ({', '.join(f'{chr(39)}{v}{chr(39)}' for v in req.maj_cat_values)})" if req.maj_cat_values else ""

        _run(conn, f"""
            INSERT INTO [{LISTING_TABLE}] ({all_cols})
            SELECT
                G.[WERKS], S.[RDC],
                LTRIM(RTRIM(G.[MAJ_CAT])),
                TRY_CAST(G.[GEN_ART_NUMBER] AS BIGINT),
                LTRIM(RTRIM(G.[CLR]))
                {stk_sel_str}, {stk_ttl} AS STK_TTL, 0 AS IS_NEW, NULL AS OPT_TYPE
            FROM [{req.grid_table}] G WITH (NOLOCK)
            INNER JOIN ({stores_sql}) S ON G.[WERKS] = S.[ST_CD]
            WHERE 1=1 {grid_mc_g}
        """)
        grid_count = conn.execute(text(f"SELECT COUNT(*) FROM [{LISTING_TABLE}]")).scalar()
        logger.info(f"Part 1 (Grid data): {grid_count} rows")
        t0 = _time_step("Part 1 (Grid data INSERT)", t0)

        # ── PART 2: MSA missing options → IS_NEW = 1 ───────────────────
        msa_rdc_join = ""
        if req.rdc_mode == "own":
            msa_rdc_join = f"AND M.[RDC] = S.[RDC]"
        # MSA base (with RDC column preserved for joining)
        msa_with_rdc = f"""
            SELECT DISTINCT
                LTRIM(RTRIM(CAST([{msa_rdc_col}] AS NVARCHAR(50)))) AS RDC,
                {_msa_col('MAJ_CAT')}, {_msa_col('GEN_ART_NUMBER')}, {_msa_col('CLR')}
            FROM [{req.msa_table}]
            WHERE [MAJ_CAT] IS NOT NULL AND [GEN_ART_NUMBER] IS NOT NULL{mc_where}{msa_rdc_filter}
        """
        _run(conn, f"""
            INSERT INTO [{LISTING_TABLE}] ({all_cols})
            SELECT
                S.[ST_CD] AS WERKS, S.[RDC],
                M.[MAJ_CAT], M.[GEN_ART_NUMBER], M.[CLR]
                {stk_zeros_str}, 0 AS STK_TTL, 1 AS IS_NEW, NULL AS OPT_TYPE
            FROM ({msa_with_rdc}) M
            INNER JOIN ({stores_sql}) S ON 1=1 {msa_rdc_join}
            WHERE NOT EXISTS (
                SELECT 1 FROM [{LISTING_TABLE}] E
                WHERE E.[WERKS] = S.[ST_CD]
                  AND E.[MAJ_CAT] = M.[MAJ_CAT]
                  AND E.[GEN_ART_NUMBER] = M.[GEN_ART_NUMBER]
                  AND E.[CLR] = M.[CLR]
            )
        """)
        total = conn.execute(text(f"SELECT COUNT(*) FROM [{LISTING_TABLE}]")).scalar()
        new_count = total - grid_count
        logger.info(f"Part 2 (MSA missing): {new_count} rows")
        t0 = _time_step("Part 2 (MSA missing INSERT)", t0)

        # ── PART 2.5: Create indexes on listing BEFORE Part 4 (skip for tiny listings) ──
        # Index creation has significant fixed overhead (~1-2s); only worth it
        # when the listing has enough rows that Part 4 scans become expensive.
        if total >= 5000:
            try:
                _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_WERKS_MJ ON [{LISTING_TABLE}]([WERKS], [MAJ_CAT]) INCLUDE ([GEN_ART_NUMBER], [CLR], [STK_TTL])")
            except Exception:
                pass
            try:
                _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_GENART ON [{LISTING_TABLE}]([GEN_ART_NUMBER]) INCLUDE ([WERKS], [MAJ_CAT], [CLR])")
            except Exception:
                pass
            t0 = _time_step("Part 2.5 (Indexes before Part 4)", t0)
        else:
            logger.info(f"Part 2.5: skipped indexes (listing has only {total} rows, < 5000 threshold)")
            t0 = _time_step("Part 2.5 (skipped — small listing)", t0)

        # ── PART 3: OPT_TYPE tagging — REMOVED (new logic will be added later)
        # OPT_TYPE column remains in the table (populated as NULL) for
        # backward compatibility with preview/summary endpoints.
        rl_count = 0
        nl_count = 0
        mixl_count = 0
        tbl_count = 0
        toc_count = 0
        untagged = total

        # ── PART 3.5: Populate DPN + SAL_D from ARS_CALC_ST_MAJ_CAT ─────
        # Needed BEFORE MIX tagging (for the STK_TTL < 60% * DPN rule)
        # and for Part 4 PER_OPT_SALE / Part 5 OPT_MBQ.
        for col in ["DPN", "SAL_D", "AUTO_GEN_ART_SALE", "AGE"]:
            try:
                _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{col}] FLOAT NULL")
            except Exception:
                pass
        if _table_exists(conn, "ARS_CALC_ST_MAJ_CAT"):
            calc_cols = _get_columns(conn, "ARS_CALC_ST_MAJ_CAT")
            upd_parts = []
            if "DPN" in calc_cols:
                upd_parts.append("L.[DPN] = TRY_CAST(C.[DPN] AS FLOAT)")
            if "SAL_D" in calc_cols:
                upd_parts.append("L.[SAL_D] = TRY_CAST(C.[SAL_D] AS FLOAT)")
            if upd_parts:
                _run(conn, f"""
                    UPDATE L SET {', '.join(upd_parts)}
                    FROM [{LISTING_TABLE}] L
                    INNER JOIN [ARS_CALC_ST_MAJ_CAT] C WITH (NOLOCK)
                        ON L.[WERKS] = C.[ST_CD] AND L.[MAJ_CAT] = C.[MAJ_CAT]
                """)
                logger.info("Part 3.5: DPN, SAL_D from ARS_CALC_ST_MAJ_CAT")

        # Part 3.5b: Populate AUTO_GEN_ART_SALE from MASTER_GEN_ART_SALE.SAL_PD
        # Option grain: (ST_CD, MAJ_CAT, GEN_ART_NUMBER, CLR). The master table
        # carries the full planned-sales universe (~21L rows) — much broader
        # than ARS_CALC_ST_ART, so we source AUTO_GEN_ART_SALE directly from it.
        # SAL_PD is precomputed in grid_calculations._step_master_sale_sal_pd.
        if _table_exists(conn, "MASTER_GEN_ART_SALE"):
            sale_cols = _get_columns(conn, "MASTER_GEN_ART_SALE")
            if "SAL_PD" in sale_cols:
                # Direct equality (no ISNULL) so SQL Server can use index seek.
                # CLR sentinel 'NA' matches what grid_builder already populates.
                _run(conn, f"""
                    UPDATE L SET L.[AUTO_GEN_ART_SALE] = TRY_CAST(S.[SAL_PD] AS FLOAT)
                    FROM [{LISTING_TABLE}] L
                    INNER JOIN [MASTER_GEN_ART_SALE] S WITH (NOLOCK)
                        ON  L.[WERKS]          = S.[ST_CD]
                        AND L.[MAJ_CAT]        = S.[MAJ_CAT]
                        AND L.[GEN_ART_NUMBER] = S.[GEN_ART_NUMBER]
                        AND L.[CLR]            = S.[CLR]
                """)
                logger.info("Part 3.5b: AUTO_GEN_ART_SALE from MASTER_GEN_ART_SALE.SAL_PD")
            else:
                logger.warning("Part 3.5b: MASTER_GEN_ART_SALE.SAL_PD not yet computed — run Contribution calc pipeline")

        # Part 3.5c: Populate AGE (option age in days) from MASTER_GEN_ART_AGE
        # An "option" = (ST_CD + MAJ_CAT + GEN_ART_NUMBER + CLR) — store-level grain.
        # This is the single authoritative source for option age.
        if _table_exists(conn, "MASTER_GEN_ART_AGE"):
            age_cols = _get_columns(conn, "MASTER_GEN_ART_AGE")
            required = {"ST_CD", "MAJ_CAT", "GEN_ART_NUMBER", "CLR", "AGE"}
            missing = required - set(age_cols)
            if not missing:
                _run(conn, f"""
                    UPDATE L SET L.[AGE] = TRY_CAST(M.[AGE] AS FLOAT)
                    FROM [{LISTING_TABLE}] L
                    INNER JOIN [MASTER_GEN_ART_AGE] M WITH (NOLOCK)
                        ON  L.[WERKS]          = M.[ST_CD]
                        AND L.[MAJ_CAT]        = M.[MAJ_CAT]
                        AND L.[GEN_ART_NUMBER] = M.[GEN_ART_NUMBER]
                        AND L.[CLR]            = M.[CLR]
                """)
                logger.info("Part 3.5c: AGE from MASTER_GEN_ART_AGE (ST_CD+MAJ_CAT+GEN_ART_NUMBER+CLR)")
            else:
                logger.warning(f"Part 3.5c: MASTER_GEN_ART_AGE missing columns: {missing}")
        else:
            logger.warning("Part 3.5c: MASTER_GEN_ART_AGE table not found — AGE will remain NULL")
        t0 = _time_step("Part 3.5 (DPN/SAL_D/AUTO_GEN_ART_SALE/AGE)", t0)

        # ── PART 3.55: Populate MSA_FNL_Q early (needed by Part 3.6 for TBL/TBC tagging)
        # Part 5c re-populates the same value later — idempotent.
        try:
            _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [MSA_FNL_Q] FLOAT NULL")
        except Exception:
            pass
        if _table_exists(conn, req.msa_table):
            _pre_msa_cols = _get_columns(conn, req.msa_table)
            if "FNL_Q" in _pre_msa_cols:
                _has_msa_rdc = msa_rdc_col in _pre_msa_cols
                _rdc_select = f", LTRIM(RTRIM(CAST([{msa_rdc_col}] AS NVARCHAR(50)))) AS MSA_RDC" if _has_msa_rdc else ""
                _rdc_group  = f", LTRIM(RTRIM(CAST([{msa_rdc_col}] AS NVARCHAR(50))))" if _has_msa_rdc else ""
                _rdc_join   = "AND L.[RDC] = M.[MSA_RDC]" if _has_msa_rdc and req.rdc_mode == "own" else ""
                try:
                    _run(conn, f"""
                        UPDATE L SET L.[MSA_FNL_Q] = TRY_CAST(M.[FNL_Q] AS FLOAT)
                        FROM [{LISTING_TABLE}] L
                        INNER JOIN (
                            SELECT {_msa_col('MAJ_CAT')}, {_msa_col('GEN_ART_NUMBER')}, {_msa_col('CLR')}
                                   {_rdc_select},
                                   SUM(TRY_CAST([FNL_Q] AS FLOAT)) AS FNL_Q
                            FROM [{req.msa_table}]
                            WHERE [MAJ_CAT] IS NOT NULL AND [GEN_ART_NUMBER] IS NOT NULL{msa_rdc_filter}
                            GROUP BY {_msa_expr('MAJ_CAT')}, {_msa_expr('GEN_ART_NUMBER')}, {_msa_expr('CLR')}{_rdc_group}
                        ) M ON L.[MAJ_CAT] = M.[MAJ_CAT]
                            AND L.[GEN_ART_NUMBER] = M.[GEN_ART_NUMBER]
                            AND L.[CLR] = M.[CLR] {_rdc_join}
                    """)
                    logger.info(f"Part 3.55: MSA_FNL_Q pre-populated from {req.msa_table} (for OPT_TYPE tagging)")
                except Exception as e:
                    logger.warning(f"Part 3.55: MSA_FNL_Q pre-populate failed: {str(e)[:150]}")

        # Also populate VAR_COUNT + VAR_FNL_COUNT alongside MSA_FNL_Q
        for col in ["VAR_COUNT", "VAR_FNL_COUNT"]:
            try:
                _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{col}] FLOAT NULL")
            except Exception:
                pass
        if _table_exists(conn, "ARS_MSA_VAR_ART"):
            var_cols = _get_columns(conn, "ARS_MSA_VAR_ART")
            if all(c in var_cols for c in ["MAJ_CAT", "GEN_ART_NUMBER", "CLR"]):
                has_fnl = "FNL_Q" in var_cols
                has_var_rdc = "RDC" in var_cols
                fnl_expr = f", SUM(CASE WHEN TRY_CAST([FNL_Q] AS FLOAT) > 0 THEN 1 ELSE 0 END) AS fnl_cnt" if has_fnl else ", 0 AS fnl_cnt"
                vrdc_select = f", LTRIM(RTRIM(CAST([RDC] AS NVARCHAR(50)))) AS MSA_RDC" if has_var_rdc else ""
                vrdc_group = f", LTRIM(RTRIM(CAST([RDC] AS NVARCHAR(50))))" if has_var_rdc else ""
                vrdc_join = "AND L.[RDC] = V.[MSA_RDC]" if has_var_rdc and req.rdc_mode == "own" else ""
                var_rdc_where = msa_rdc_filter.replace(f"[{msa_rdc_col}]", "[RDC]") if has_var_rdc else ""
                try:
                    _run(conn, f"""
                        UPDATE L SET L.[VAR_COUNT] = V.var_cnt, L.[VAR_FNL_COUNT] = V.fnl_cnt
                        FROM [{LISTING_TABLE}] L
                        INNER JOIN (
                            SELECT {_msa_col('MAJ_CAT')}, {_msa_col('GEN_ART_NUMBER')}, {_msa_col('CLR')}
                                   {vrdc_select},
                                   COUNT(*) AS var_cnt{fnl_expr}
                            FROM [ARS_MSA_VAR_ART]
                            WHERE [MAJ_CAT] IS NOT NULL AND [GEN_ART_NUMBER] IS NOT NULL{mc_where}{var_rdc_where}
                            GROUP BY {_msa_expr('MAJ_CAT')}, {_msa_expr('GEN_ART_NUMBER')}, {_msa_expr('CLR')}{vrdc_group}
                        ) V ON L.[MAJ_CAT] = V.[MAJ_CAT]
                            AND L.[GEN_ART_NUMBER] = V.[GEN_ART_NUMBER]
                            AND L.[CLR] = V.[CLR] {vrdc_join}
                    """)
                    logger.info(f"Part 3.55: VAR_COUNT + VAR_FNL_COUNT from ARS_MSA_VAR_ART")
                except Exception as e:
                    logger.warning(f"Part 3.55: VAR_COUNT/FNL_COUNT failed: {str(e)[:150]}")
        t0 = _time_step("Part 3.55 (MSA_FNL_Q + VAR_COUNT)", t0)

        # ── PART 3.6: Populate GEN_ART_DESC + tag OPT_TYPE (4-way classification) ──
        # Rules (IS_NEW = 0 only — existing store data):
        #   (a) GEN_ART_DESC contains: SEST, SEDC, -NB, MIX  (keyword match)
        #   (b) STK_TTL < 60% * DPN                          (low stock)
        try:
            _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [GEN_ART_DESC] NVARCHAR(500) NULL")
        except Exception:
            pass
        if _table_exists(conn, "vw_master_product"):
            try:
                _run(conn, f"""
                    UPDATE L SET L.[GEN_ART_DESC] = MP.[GEN_ART_DESC]
                    FROM [{LISTING_TABLE}] L
                    INNER JOIN [vw_master_product] MP WITH (NOLOCK)
                        ON L.[GEN_ART_NUMBER] = MP.[ARTICLE_NUMBER]
                    WHERE MP.[GEN_ART_DESC] IS NOT NULL
                """)
                logger.info("Part 3.6: GEN_ART_DESC populated from vw_master_product")
            except Exception as e:
                logger.warning(f"Part 3.6: GEN_ART_DESC population failed: {str(e)[:150]}")

        # OPT_TYPE classification — evaluated top-to-bottom, first match wins.
        # Order: MIX first (catch bad options early) → RL → TBC → TBL.
        #
        #   MIX — (a) low stock + no MSA:  STK < threshold%*DPN AND MSA_FNL_Q = 0
        #         (b) poor color fill:     VAR_FNL_COUNT / VAR_COUNT < threshold%
        #         Either condition → MIX (option is not viable for replenishment)
        #   RL  — STK_TTL >= threshold% * DPN  (adequate stock, regardless of MSA)
        #   TBC — 0 < STK < threshold%*DPN AND MSA_FNL_Q > 0  (To Be Check)
        #   TBL — STK_TTL <= 0 AND MSA_FNL_Q > 0              (To Be Listed)
        threshold = req.stock_threshold_pct
        def _classify_opt_type(label="OPT_TYPE"):
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_TYPE] = CASE
                    -- MIX (a): low stock + no MSA backup
                    WHEN ISNULL([DPN], 0) > 0
                     AND ISNULL([STK_TTL], 0) < {threshold} * [DPN]
                     AND ISNULL([MSA_FNL_Q], 0) = 0
                        THEN 'MIX'
                    -- MIX (b): poor color fill (existing rows only — IS_NEW=1 are new
                    --          MSA recommendations with inherently low VAR ratios)
                    WHEN [IS_NEW] = 0
                     AND ISNULL([VAR_COUNT], 0) > 0
                     AND CAST(ISNULL([VAR_FNL_COUNT], 0) AS FLOAT) / [VAR_COUNT] < {threshold}
                        THEN 'MIX'
                    -- RL: adequate stock
                    WHEN ISNULL([DPN], 0) > 0
                     AND ISNULL([STK_TTL], 0) >= {threshold} * [DPN]
                        THEN 'RL'
                    -- TBC: low stock but MSA available → check
                    WHEN ISNULL([DPN], 0) > 0
                     AND ISNULL([STK_TTL], 0) > 0
                     AND [STK_TTL] < {threshold} * [DPN]
                     AND ISNULL([MSA_FNL_Q], 0) > 0
                        THEN 'TBC'
                    -- TBL: zero/negative stock + MSA available → list
                    WHEN ISNULL([STK_TTL], 0) <= 0
                     AND ISNULL([MSA_FNL_Q], 0) > 0
                        THEN 'TBL'
                    ELSE [OPT_TYPE]
                END
            """)

        try:
            _classify_opt_type("Part 3.6")
            # Per-type counts (split by IS_NEW for visibility)
            type_counts = {}
            for row in conn.execute(text(
                f"SELECT [OPT_TYPE], [IS_NEW], COUNT(*) FROM [{LISTING_TABLE}] "
                f"GROUP BY [OPT_TYPE], [IS_NEW]"
            )).fetchall():
                key = (row[0] or "(null)", int(row[1]) if row[1] is not None else 0)
                type_counts[key] = row[2]
            def _sum(t):
                return type_counts.get((t, 0), 0) + type_counts.get((t, 1), 0)
            mixl_count = _sum("MIX")
            tbl_count  = _sum("TBL")
            toc_count  = _sum("TBC")
            rl_count   = _sum("RL")
            tagged_total = mixl_count + tbl_count + toc_count + rl_count
            untagged = total - tagged_total
            logger.info(
                f"Part 3.6: OPT_TYPE tagged — "
                f"MIX={mixl_count}, TBL={tbl_count}, TBC={toc_count}, RL={rl_count}, "
                f"untagged={untagged} "
                f"[IS_NEW=1 breakdown: TBL={type_counts.get(('TBL',1),0)}, "
                f"MIX={type_counts.get(('MIX',1),0)}, "
                f"TBC={type_counts.get(('TBC',1),0)}, "
                f"RL={type_counts.get(('RL',1),0)}]"
            )
        except Exception as e:
            tbl_count = 0
            toc_count = 0
            logger.warning(f"Part 3.6: OPT_TYPE tagging failed: {str(e)[:150]}")

        # VAR ratio override: if VAR_FNL_COUNT/VAR_COUNT < threshold (poor color availability)
        # → low stock + poor colors = MIX, adequate stock + poor colors = RL
        try:
            vt = req.stock_threshold_pct
            # Poor color + low stock → MIX (even if MSA says TBL/TBC)
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_TYPE] = 'MIX'
                WHERE ISNULL([VAR_COUNT], 0) > 0
                  AND CAST(ISNULL([VAR_FNL_COUNT], 0) AS FLOAT) / [VAR_COUNT] < {vt}
                  AND ISNULL([MSA_FNL_Q], 0) > 0
                  AND ISNULL([DPN], 0) > 0
                  AND ISNULL([STK_TTL], 0) < {vt} * [DPN]
            """)
            # Poor color + adequate stock → RL
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_TYPE] = 'RL'
                WHERE ISNULL([VAR_COUNT], 0) > 0
                  AND CAST(ISNULL([VAR_FNL_COUNT], 0) AS FLOAT) / [VAR_COUNT] < {vt}
                  AND ISNULL([DPN], 0) > 0
                  AND ISNULL([STK_TTL], 0) >= {vt} * [DPN]
            """)
            logger.info(f"Part 3.6: VAR ratio override (threshold={vt})")
        except Exception as ve:
            logger.warning(f"Part 3.6: VAR ratio override failed: {str(ve)[:150]}")
        t0 = _time_step("Part 3.6 (OPT_TYPE + VAR ratio override)", t0)

        # ── PART 3.7: MIX handling ─────────────────────────────────────────
        # Three modes (controlled by req.mix_mode):
        #   "st_maj_rng" (DEFAULT) → 1 line per (WERKS, MAJ_CAT, RNG_SEG)
        #   "st_maj"               → 1 line per (WERKS, MAJ_CAT) — coarser
        #   "each"                 → no aggregation, keep each MIX art row
        # Legacy value "aggregate" → "st_maj"; "mark" → "each"
        # Only EXISTING store rows (IS_NEW = 0) are aggregated either way.
        mix_before = conn.execute(text(
            f"SELECT COUNT(*) FROM [{LISTING_TABLE}] WHERE [OPT_TYPE] = 'MIX' AND [IS_NEW] = 0"
        )).scalar() or 0

        _alias = {"aggregate": "st_maj", "mark": "each"}
        mix_mode = (req.mix_mode or "st_maj_rng").lower()
        mix_mode = _alias.get(mix_mode, mix_mode)
        if mix_mode not in ("st_maj_rng", "st_maj", "each"):
            logger.warning(f"Part 3.7: unknown mix_mode={req.mix_mode!r}, defaulting to 'st_maj_rng'")
            mix_mode = "st_maj_rng"

        if mix_mode == "each":
            logger.info(f"Part 3.7: mix_mode=each — keeping all {mix_before} MIX rows as individual lines")
        elif mix_before > 0:
            try:
                all_cols_rows = conn.execute(text("""
                    SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :t
                    ORDER BY ORDINAL_POSITION
                """), {"t": LISTING_TABLE}).fetchall()

                numeric_types = {'float','real','int','bigint','smallint','tinyint','decimal','numeric','money','smallmoney'}
                preserve_cols = {"WERKS", "RDC", "MAJ_CAT", "GEN_ART_NUMBER", "CLR",
                                 "GEN_ART_DESC", "IS_NEW", "OPT_TYPE",
                                 "DPN", "SAL_D", "RNG_SEG"}  # store-majcat/rng attrs (not summed)
                sum_cols = []
                for cname, ctype in all_cols_rows:
                    if cname.upper() in {c.upper() for c in preserve_cols}:
                        continue
                    if ctype.lower() in numeric_types:
                        sum_cols.append(cname)

                sum_select = ", ".join(f"SUM(ISNULL([L].[{c}], 0)) AS [{c}]" for c in sum_cols)
                sum_select_clause = f", {sum_select}" if sum_cols else ""

                has_calc = _table_exists(conn, "ARS_CALC_ST_MAJ_CAT")
                if has_calc:
                    calc_cols = _get_columns(conn, "ARS_CALC_ST_MAJ_CAT")
                    dpn_expr  = "MAX(TRY_CAST(C.[DPN] AS FLOAT))"  if "DPN"  in calc_cols else "NULL"
                    sald_expr = "MAX(TRY_CAST(C.[SAL_D] AS FLOAT))" if "SAL_D" in calc_cols else "NULL"
                    calc_join = """
                        LEFT JOIN [ARS_CALC_ST_MAJ_CAT] C WITH (NOLOCK)
                            ON L.[WERKS] = C.[ST_CD] AND L.[MAJ_CAT] = C.[MAJ_CAT]
                    """
                else:
                    dpn_expr = "NULL"; sald_expr = "NULL"; calc_join = ""

                # For st_maj_rng mode: also join vw_master_product for RNG_SEG
                if mix_mode == "st_maj_rng":
                    has_mp = _table_exists(conn, "vw_master_product")
                    if has_mp:
                        mp_cols = _get_columns(conn, "vw_master_product")
                        if "RNG_SEG" not in mp_cols:
                            logger.warning("Part 3.7: RNG_SEG not in vw_master_product — falling back to st_maj")
                            mix_mode = "st_maj"
                    else:
                        logger.warning("Part 3.7: vw_master_product missing — falling back to st_maj")
                        mix_mode = "st_maj"

                if mix_mode == "st_maj_rng":
                    mp_join = """
                        LEFT JOIN [vw_master_product] MP WITH (NOLOCK)
                            ON L.[GEN_ART_NUMBER] = MP.[ARTICLE_NUMBER]
                    """
                    rng_expr       = "ISNULL(LTRIM(RTRIM(MP.[RNG_SEG])), 'NA')"
                    rng_select_col = f"{rng_expr} AS [RNG_SEG]"
                    group_by       = f"L.[WERKS], L.[MAJ_CAT], {rng_expr}"
                    mode_label     = "per (WERKS, MAJ_CAT, RNG_SEG)"
                else:  # st_maj
                    mp_join = ""
                    rng_select_col = "CAST(NULL AS NVARCHAR(100)) AS [RNG_SEG]"
                    group_by       = "L.[WERKS], L.[MAJ_CAT]"
                    mode_label     = "per (WERKS, MAJ_CAT)"

                staging = "#mix_agg"
                _run(conn, f"IF OBJECT_ID('tempdb..{staging}') IS NOT NULL DROP TABLE {staging}")
                _run(conn, f"""
                    SELECT
                        L.[WERKS], MAX(L.[RDC]) AS [RDC], L.[MAJ_CAT],
                        CAST(0 AS BIGINT) AS [GEN_ART_NUMBER],
                        CAST('MIX' AS NVARCHAR(100)) AS [CLR],
                        CAST('MIX' AS NVARCHAR(500)) AS [GEN_ART_DESC],
                        CAST(0 AS BIT) AS [IS_NEW],
                        CAST('MIX' AS NVARCHAR(10)) AS [OPT_TYPE],
                        CAST({dpn_expr} AS FLOAT) AS [DPN],
                        CAST({sald_expr} AS FLOAT) AS [SAL_D],
                        {rng_select_col}
                        {sum_select_clause}
                    INTO {staging}
                    FROM [{LISTING_TABLE}] L
                    {calc_join}
                    {mp_join}
                    WHERE L.[OPT_TYPE] = 'MIX' AND L.[IS_NEW] = 0
                    GROUP BY {group_by}
                """)
                agg_rows = conn.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar() or 0

                _run(conn, f"DELETE FROM [{LISTING_TABLE}] WHERE [OPT_TYPE] = 'MIX' AND [IS_NEW] = 0")

                # Build INSERT — only include RNG_SEG if listing has that column
                listing_cols_upper = {c.upper() for c in _get_columns(conn, LISTING_TABLE)}
                ins_cols = ["WERKS", "RDC", "MAJ_CAT", "GEN_ART_NUMBER", "CLR",
                            "GEN_ART_DESC", "IS_NEW", "OPT_TYPE", "DPN", "SAL_D"]
                if "RNG_SEG" in listing_cols_upper:
                    ins_cols.append("RNG_SEG")
                else:
                    # RNG_SEG was staged but doesn't exist on listing yet — add it
                    if mix_mode == "st_maj_rng":
                        try:
                            _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [RNG_SEG] NVARCHAR(100) NULL")
                            ins_cols.append("RNG_SEG")
                        except Exception:
                            pass
                ins_cols += sum_cols
                ins_cols_sql = ", ".join(f"[{c}]" for c in ins_cols)
                _run(conn, f"""
                    INSERT INTO [{LISTING_TABLE}] ({ins_cols_sql})
                    SELECT {ins_cols_sql} FROM {staging}
                """)
                _run(conn, f"DROP TABLE {staging}")

                logger.info(f"Part 3.7: aggregated {mix_before} MIX rows → {agg_rows} MIX lines "
                            f"[{mode_label}], summed {len(sum_cols)} numeric cols "
                            f"(DPN/SAL_D fetched from ARS_CALC_ST_MAJ_CAT, not summed)")
            except Exception as e:
                logger.warning(f"Part 3.7 MIX aggregation failed: {str(e)[:200]}")
        else:
            logger.info("Part 3.7: no MIX rows to aggregate")
        t0 = _time_step(f"Part 3.7 (MIX handling, mode={mix_mode})", t0)

        # ── PART 4: Add CONT, MBQ, OPT_CNT, DISP_Q from ALL grid tables ─────
        # Each grid adds prefixed columns: MJ_CONT, CLR_CONT, RNG_SEG_MBQ, etc.
        # DISP_Q is stored as DISP_Q * CONT (pre-computed in grid_builder).
        # Skip pivot_only grids (GEN_ART, VAR_ART — no CONT/MBQ/OPT_CNT/DISP_Q).
        # Also adds: {prefix}_GRID_GROUP, {prefix}_WEIGHTAGE, {prefix}_PER_OPT_SALE
        src_cols = ["STK_TTL", "CONT", "MBQ", "OPT_CNT", "DISP_Q"]

        if _table_exists(conn, "ARS_GRID_BUILDER"):
            grid_rows = conn.execute(text("""
                SELECT grid_name, output_table, hierarchy_columns
                FROM [ARS_GRID_BUILDER]
                WHERE UPPER(status) = 'ACTIVE'
                  AND ISNULL(pivot_only, 0) = 0
                ORDER BY seq ASC
            """)).fetchall()
        else:
            grid_rows = []

        listing_direct_cols = {"WERKS", "MAJ_CAT", "GEN_ART_NUMBER", "CLR"}
        # Get vw_master_product columns for resolving hierarchy via GEN_ART_NUMBER
        mp_cols_set = set()
        if _table_exists(conn, "vw_master_product"):
            mp_cols_set = {r[0].upper() for r in conn.execute(text(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='vw_master_product'"
            )).fetchall()}

        # ── OPTIMIZATION: Pre-resolve MP attributes ONCE onto listing ─────
        # Previously each grid using MP cols (MACRO_MVGR, MICRO_MVGR, FAB,
        # M_VND_CD, RNG_SEG, etc.) re-joined vw_master_product for 5M rows.
        # Now we add those columns to listing + populate ONCE. Then every
        # Part 4 grid becomes a DIRECT join → saves N × (5M-row MP join).
        mp_needed_cols = set()
        for grow in grid_rows:
            try:
                _h = json.loads(grow[2]) if isinstance(grow[2], str) else grow[2]
                for hc in (_h or []):
                    hcu = hc.upper()
                    if hcu not in listing_direct_cols and hcu in mp_cols_set:
                        mp_needed_cols.add(hcu)
            except Exception:
                pass

        if mp_needed_cols and _table_exists(conn, "vw_master_product"):
            # Add columns to listing (NVARCHAR as default, BIGINT for known numeric)
            from app.utils.db_helpers import get_columns as _gc
            mp_type_rows = conn.execute(text(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_NAME='vw_master_product'"
            )).fetchall()
            mp_type_map = {r[0].upper(): r[1].lower() for r in mp_type_rows}
            mp_actual_map = {r[0].upper(): r[0] for r in mp_type_rows}
            _NUM = {'bigint','int','smallint','tinyint','float','real','decimal','numeric','money','smallmoney'}

            existing_listing_cols = {c.upper() for c in _gc(conn, LISTING_TABLE)}
            set_parts = []
            for mc in mp_needed_cols:
                if mc in existing_listing_cols:
                    continue
                is_num = mp_type_map.get(mc, '') in _NUM
                dtype = "BIGINT NULL" if is_num else "NVARCHAR(200) NULL"
                try:
                    _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{mc}] {dtype}")
                except Exception:
                    pass
            # Build single UPDATE that populates all MP cols at once
            for mc in mp_needed_cols:
                actual = mp_actual_map.get(mc, mc)
                is_num = mp_type_map.get(mc, '') in _NUM
                if is_num:
                    set_parts.append(f"L.[{mc}] = ISNULL(TRY_CAST(MP.[{actual}] AS BIGINT), 0)")
                else:
                    set_parts.append(f"L.[{mc}] = ISNULL(LTRIM(RTRIM(CAST(MP.[{actual}] AS NVARCHAR(200)))), 'NA')")
            if set_parts:
                try:
                    _run(conn, f"""
                        UPDATE L SET {', '.join(set_parts)}
                        FROM [{LISTING_TABLE}] L
                        INNER JOIN [vw_master_product] MP WITH (NOLOCK)
                            ON L.[GEN_ART_NUMBER] = MP.[ARTICLE_NUMBER]
                    """)
                    logger.info(f"Part 4 pre-resolve: populated {len(mp_needed_cols)} MP cols on listing: {sorted(mp_needed_cols)}")
                except Exception as e:
                    logger.warning(f"Part 4 pre-resolve failed: {str(e)[:150]}")
            # Now treat all MP cols as "direct" — no more MP join needed
            listing_direct_cols = listing_direct_cols | mp_needed_cols
        t0 = _time_step("Part 4 pre-resolve (MP → listing cols)", t0)

        mapped_grids = []
        for grow in grid_rows:
            gname, gtable = grow[0], grow[1]
            try:
                ghier = json.loads(grow[2]) if isinstance(grow[2], str) else grow[2]
            except Exception:
                continue
            if not _table_exists(conn, gtable):
                continue

            gcols = _get_columns(conn, gtable)
            available = [c for c in src_cols if c in gcols]
            if not available:
                continue

            # Build join: hierarchy cols from listing directly (MP cols now
            # pre-populated onto listing in pre-resolve step, so always direct)
            join_parts = []
            can_join = True
            for hc in ghier:
                hcu = hc.upper()
                if hcu in listing_direct_cols:
                    # Both sides are BIGINT → no TRY_CAST needed (preserves index seek)
                    join_parts.append(f"L.[{hcu}] = G.[{hc}]")
                else:
                    can_join = False
                    break

            if not can_join:
                logger.info(f"Part 4: {gname} skipped — {ghier} not resolvable")
                continue

            join_sql = " AND ".join(join_parts)

            # Prefix: MJ_RNG_SEG→RNG_SEG, MJ_CLR→CLR, MJ→MJ
            prefix = gname.upper()
            if prefix.startswith("MJ_"):
                prefix = prefix[3:]

            col_map = {}
            for c in available:
                new_col = f"{prefix}_{c}"
                col_map[new_col] = c
                try:
                    _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{new_col}] FLOAT NULL")
                except Exception:
                    pass

            set_parts = ", ".join(f"L.[{nc}] = TRY_CAST(G.[{oc}] AS FLOAT)" for nc, oc in col_map.items())

            update_sql = f"""
                UPDATE L SET {set_parts}
                FROM [{LISTING_TABLE}] L
                INNER JOIN [{gtable}] G WITH (NOLOCK) ON {join_sql}
            """

            g_t0 = time.time()
            try:
                _run(conn, update_sql)
                dt = round(time.time() - g_t0, 1)
                mapped_grids.append(gname)
                logger.info(f"Part 4: {gname} → {list(col_map.keys())} [direct, join on {ghier}] — {dt}s")
                step_timings.append({"step": f"Part 4 [{gname}]", "seconds": dt})
            except Exception as e:
                dt = round(time.time() - g_t0, 1)
                logger.warning(f"Part 4: {gname} failed in {dt}s: {str(e)[:200]}")
                step_timings.append({"step": f"Part 4 [{gname}] FAILED", "seconds": dt})

        # ── Part 4b: PER_OPT_SALE from the grid flagged use_for_opt_sale ──
        listing_cols = _get_columns(conn, LISTING_TABLE)
        has_dpn  = "DPN"  in listing_cols
        has_sald = "SAL_D" in listing_cols
        opt_grid_row = conn.execute(text("""
            SELECT TOP 1 grid_name FROM [ARS_GRID_BUILDER]
            WHERE ISNULL(use_for_opt_sale, 0) = 1 AND UPPER(status) = 'ACTIVE'
            ORDER BY seq ASC
        """)).fetchone()
        if opt_grid_row and has_dpn and has_sald:
            opt_prefix = opt_grid_row[0].upper()
            if opt_prefix.startswith("MJ_"):
                opt_prefix = opt_prefix[3:]
            opt_mbq  = f"{opt_prefix}_MBQ"
            opt_disp = f"{opt_prefix}_DISP_Q"
            if opt_mbq in listing_cols and opt_disp in listing_cols:
                if "PER_OPT_SALE" not in listing_cols:
                    try:
                        _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [PER_OPT_SALE] FLOAT NULL")
                    except Exception:
                        pass
                _run(conn, f"""
                    UPDATE [{LISTING_TABLE}] SET [PER_OPT_SALE] = CASE
                        WHEN ISNULL([{opt_disp}],0) = 0 OR ISNULL([SAL_D],0) = 0 THEN 0
                        ELSE ((ISNULL([{opt_mbq}],0) - ISNULL([{opt_disp}],0))
                               / NULLIF([{opt_disp}],0) * ISNULL([DPN],0)) / NULLIF([SAL_D],0)
                    END
                """)
                logger.info(f"Part 4b: PER_OPT_SALE from {opt_grid_row[0]}")

        # ── Part 4c: OPT_MBQ + OPT_REQ (moved here from Part 5 — needed for excess calc) ──
        listing_cols = _get_columns(conn, LISTING_TABLE)
        sale_col = None
        for c in listing_cols:
            if ("L-7" in c.upper() or "L_7" in c.upper()) and "SALE" in c.upper() and "7" in c:
                sale_col = c
                break
        if not sale_col:
            for c in listing_cols:
                if c.upper().startswith("L-7") or c.upper().startswith("L_7"):
                    sale_col = c
                    break

        for col in ["OPT_MBQ", "OPT_REQ", "EXCESS_STK"]:
            try:
                _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{col}] FLOAT NULL")
            except Exception:
                pass

        listing_cols = _get_columns(conn, LISTING_TABLE)
        has_auto    = "AUTO_GEN_ART_SALE" in listing_cols
        has_age     = "AGE" in listing_cols
        has_per_opt = "PER_OPT_SALE" in listing_cols
        if sale_col and "DPN" in listing_cols:
            l7_daily   = f"(ISNULL(TRY_CAST([{sale_col}] AS FLOAT), 0) / 7.0)"
            auto_daily = "ISNULL([AUTO_GEN_ART_SALE], 0)" if has_auto else "0"
            per_opt    = "ISNULL([PER_OPT_SALE], 0)"    if has_per_opt else "0"

            def _sql_max(*exprs):
                values = ", ".join(f"({e})" for e in exprs)
                return f"(SELECT MAX(v) FROM (VALUES {values}) T(v))"

            default_rate = _sql_max(l7_daily, auto_daily) if has_auto else l7_daily
            new_rate = _sql_max(per_opt, l7_daily, auto_daily)

            # Use "new article" rate (includes PER_OPT_SALE) ONLY when AGE < 15
            if has_age:
                rate_expr = (
                    f"CASE WHEN [AGE] IS NOT NULL AND [AGE] < {int(req.age_threshold)} "
                    f"THEN {new_rate} ELSE {default_rate} END"
                )
            else:
                rate_expr = default_rate

            # OPT_MBQ = DPN + rate × SAL_D
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_MBQ] = ISNULL([DPN], 0) + ({rate_expr}) * ISNULL([SAL_D], 0)
            """)
            # OPT_REQ = MAX(0, OPT_MBQ - STK_TTL)
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_REQ] = CASE
                    WHEN ISNULL([OPT_MBQ], 0) - ISNULL([STK_TTL], 0) > 0
                    THEN ISNULL([OPT_MBQ], 0) - ISNULL([STK_TTL], 0)
                    ELSE 0 END
            """)

            # OPT_MBQ_WH = DPN + rate × (SAL_D + HOLD_DAYS) — "With Hold"
            # OPT_REQ_WH = MAX(0, OPT_MBQ_WH - STK_TTL)
            for col in ["OPT_MBQ_WH", "OPT_REQ_WH"]:
                try:
                    _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{col}] FLOAT NULL")
                except Exception:
                    pass
            # HOLD_DAYS applies ONLY to IS_NEW=1 (new options without store stock).
            # For existing options (IS_NEW=0), OPT_MBQ_WH = OPT_MBQ (same as without hold).
            hold = int(req.hold_days or 0)
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_MBQ_WH] = ISNULL([DPN], 0) + ({rate_expr})
                    * (ISNULL([SAL_D], 0) + CASE WHEN ISNULL([IS_NEW], 0) = 1 THEN {hold} ELSE 0 END)
            """)
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [OPT_REQ_WH] = CASE
                    WHEN ISNULL([OPT_MBQ_WH], 0) - ISNULL([STK_TTL], 0) > 0
                    THEN ISNULL([OPT_MBQ_WH], 0) - ISNULL([STK_TTL], 0)
                    ELSE 0 END
            """)
            # MAX_DAILY_SALE = MAX(L-7/7, AUTO_GEN_ART_SALE)
            try:
                _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [MAX_DAILY_SALE] FLOAT NULL")
            except Exception:
                pass
            auto_expr = "ISNULL([AUTO_GEN_ART_SALE], 0)" if has_auto else "0"
            _run(conn, f"""
                UPDATE [{LISTING_TABLE}]
                SET [MAX_DAILY_SALE] = (SELECT MAX(v) FROM (VALUES
                    ({l7_daily}), ({auto_expr})) T(v))
            """)

            logger.info(f"Part 4c: OPT_MBQ + OPT_REQ + OPT_MBQ_WH(hold={hold}d) + OPT_REQ_WH + MAX_DAILY_SALE")

        # ── Part 4d: ART_EXCESS = MAX(0, STK_TTL - 2*OPT_MBQ), skip MIX ──
        # This is the article-level excess used to deduct from each grid's stock.
        try:
            _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [ART_EXCESS] FLOAT NULL")
        except Exception:
            pass
        _run(conn, f"""
            UPDATE [{LISTING_TABLE}]
            SET [ART_EXCESS] = CASE
                WHEN ISNULL([OPT_TYPE],'') = 'MIX' THEN 0
                WHEN ISNULL([STK_TTL],0) - {req.excess_multiplier} * ISNULL([OPT_MBQ],0) > 0
                THEN ISNULL([STK_TTL],0) - {req.excess_multiplier} * ISNULL([OPT_MBQ],0)
                ELSE 0 END
        """)
        # Also set overall EXCESS_STK (same formula, visible in output)
        _run(conn, f"""
            UPDATE [{LISTING_TABLE}]
            SET [EXCESS_STK] = [ART_EXCESS]
        """)
        art_excess_sum = conn.execute(text(f"SELECT SUM([ART_EXCESS]) FROM [{LISTING_TABLE}]")).scalar() or 0
        logger.info(f"Part 4d: ART_EXCESS calculated (total excess={art_excess_sum:.0f}, MIX rows skipped)")

        # ── Part 4e: Per-grid REQ with aggregated excess deduction ──────
        # For each grid: aggregate ART_EXCESS by that grid's hierarchy,
        # then: REQ = MAX(0, MBQ - (STK_TTL - aggregated_excess))
        # No per-grid EXCESS column stored — calculated internally.
        listing_cols = _get_columns(conn, LISTING_TABLE)
        req_log = []
        for gname in mapped_grids:
            prefix = gname.upper()
            if prefix.startswith("MJ_"):
                prefix = prefix[3:]
            mbq_col = f"{prefix}_MBQ"
            stk_col = f"{prefix}_STK_TTL"
            req_col = f"{prefix}_REQ"

            if mbq_col not in listing_cols or stk_col not in listing_cols:
                continue
            if req_col not in listing_cols:
                try:
                    _run(conn, f"ALTER TABLE [{LISTING_TABLE}] ADD [{req_col}] FLOAT NULL")
                except Exception:
                    pass

            # Determine hierarchy columns for this grid (for GROUP BY)
            grid_row = conn.execute(text("""
                SELECT hierarchy_columns FROM [ARS_GRID_BUILDER]
                WHERE grid_name = :gn
            """), {"gn": gname}).fetchone()
            if not grid_row:
                continue
            try:
                ghier = json.loads(grid_row[0]) if isinstance(grid_row[0], str) else grid_row[0]
            except Exception:
                continue

            # Build GROUP BY keys from hierarchy (exclude WERKS-only grouping)
            group_cols = [h.upper() for h in ghier if h.upper() in {c.upper() for c in listing_cols}]
            if not group_cols:
                continue

            group_by = ", ".join(f"[{c}]" for c in group_cols)
            join_cond = " AND ".join(f"L.[{c}] = E.[{c}]" for c in group_cols)

            try:
                _run(conn, f"""
                    ;WITH ExcessAgg AS (
                        SELECT {group_by}, SUM(ISNULL([ART_EXCESS], 0)) AS total_excess
                        FROM [{LISTING_TABLE}]
                        WHERE ISNULL([OPT_TYPE],'') <> 'MIX'
                        GROUP BY {group_by}
                    )
                    UPDATE L SET L.[{req_col}] =
                        CASE WHEN ISNULL(L.[{mbq_col}],0) - (ISNULL(L.[{stk_col}],0) - ISNULL(E.total_excess,0)) > 0
                             THEN ISNULL(L.[{mbq_col}],0) - (ISNULL(L.[{stk_col}],0) - ISNULL(E.total_excess,0))
                             ELSE 0 END
                    FROM [{LISTING_TABLE}] L
                    LEFT JOIN ExcessAgg E ON {join_cond}
                """)
                req_log.append(f"{req_col}(by {','.join(group_cols)})")
            except Exception as e:
                logger.warning(f"Part 4e: {req_col} failed: {str(e)[:150]}")

        if req_log:
            logger.info(f"Part 4e: REQ with excess deduction: {req_log}")

        # Drop internal ART_EXCESS column (was only needed for aggregation)
        try:
            _run(conn, f"ALTER TABLE [{LISTING_TABLE}] DROP COLUMN [ART_EXCESS]")
        except Exception:
            pass

        logger.info(f"Part 4 complete: {len(mapped_grids)} grids: {', '.join(mapped_grids)}")
        t0 = _time_step("Part 4 (grid joins + OPT_MBQ + excess + REQ)", t0)

        # ── PART 5: All moved earlier ──────────────────────────────────
        # OPT_MBQ/OPT_REQ/EXCESS_STK → Part 4c/4d
        # MSA_FNL_Q + VAR_COUNT + VAR_FNL_COUNT → Part 3.55

        # Additional indexes (WERKS-only, RDC)
        try:
            _run(conn, f"CREATE NONCLUSTERED INDEX IX_{LISTING_TABLE}_RDC ON [{LISTING_TABLE}]([RDC])")
        except Exception:
            pass

    # ── Auto-create ARS_STORE_RANKING (before working table) ──────────
    RANK_TABLE = "ARS_STORE_RANKING"
    rank_rows = 0
    rw = float(req.req_weight or 0.4)
    fw = float(req.fill_weight or 0.6)
    try:
        with de.connect() as rc:
            _run(rc, f"IF OBJECT_ID('{RANK_TABLE}','U') IS NOT NULL DROP TABLE [{RANK_TABLE}]")
            _run(rc, f"""
                ;WITH StoreAgg AS (
                    SELECT
                        [MAJ_CAT], [WERKS], MAX([RDC]) AS RDC,
                        MAX(ISNULL([MJ_REQ], 0))     AS MJ_REQ,
                        MAX(ISNULL([MJ_MBQ], 0))     AS MJ_MBQ,
                        MAX(ISNULL([MJ_STK_TTL], 0))  AS MJ_STK,
                        MAX(ISNULL([DPN], 0))         AS DPN,
                        CASE WHEN MAX(ISNULL([MJ_MBQ],0)) = 0 THEN 0
                             ELSE ROUND(MAX(ISNULL([MJ_STK_TTL],0)) / NULLIF(MAX([MJ_MBQ]),0), 4)
                        END AS FILL_RATE
                    FROM [{LISTING_TABLE}]
                    WHERE ISNULL([OPT_TYPE],'') <> 'MIX'
                    GROUP BY [MAJ_CAT], [WERKS]
                ),
                Ranked AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY MAJ_CAT ORDER BY MJ_REQ ASC)   AS REQ_RANK,
                        ROW_NUMBER() OVER (PARTITION BY MAJ_CAT ORDER BY FILL_RATE DESC) AS FILL_RANK
                    FROM StoreAgg
                )
                SELECT *,
                    ROUND(REQ_RANK * {rw} + FILL_RANK * {fw}, 2) AS W_SCORE,
                    ROW_NUMBER() OVER (PARTITION BY MAJ_CAT
                        ORDER BY ROUND(REQ_RANK * {rw} + FILL_RANK * {fw}, 2) DESC) AS ST_RANK
                INTO [{RANK_TABLE}]
                FROM Ranked
            """)
            rank_rows = rc.execute(text(f"SELECT COUNT(*) FROM [{RANK_TABLE}]")).scalar()
            logger.info(f"{RANK_TABLE}: {rank_rows} rows (req_wt={rw}, fill_wt={fw})")

            # Populate ST_RANK back into ARS_LISTING for working table
            try:
                rc.execute(text(f"ALTER TABLE [{LISTING_TABLE}] ADD [ST_RANK] INT NULL"))
                rc.commit()
            except Exception:
                pass
            _run(rc, f"""
                UPDATE L SET L.[ST_RANK] = R.[ST_RANK]
                FROM [{LISTING_TABLE}] L
                INNER JOIN [{RANK_TABLE}] R
                    ON L.[WERKS] = R.[WERKS] AND L.[MAJ_CAT] = R.[MAJ_CAT]
            """)
            logger.info(f"ST_RANK populated into {LISTING_TABLE}")
    except Exception as e:
        logger.warning(f"{RANK_TABLE} creation failed: {e}")

    # ── Auto-create ARS_LISTING_WORKING (filtered copy) ───────────────
    working_rows = 0
    try:
        with de.connect() as wc:
            all_cols = _get_columns(wc, LISTING_TABLE)
            all_upper = {c.upper(): c for c in all_cols}
            keep = _FINAL_KEEP_COLS
            selected = [c for c in all_cols if c.upper() in keep or any(c.upper().endswith(s) for s in _FINAL_KEEP_SUFFIX)]
            if selected:
                col_list = ", ".join(f"[{c}]" for c in selected)
                _run(wc, f"IF OBJECT_ID('{FINAL_TABLE}','U') IS NOT NULL DROP TABLE [{FINAL_TABLE}]")

                where = []
                if "MSA_FNL_Q" in all_upper:
                    where.append("ISNULL([MSA_FNL_Q], 0) > 0")
                if "OPT_REQ_WH" in all_upper:
                    where.append("ISNULL([OPT_REQ_WH], 0) >= 1")
                # Exclude poor color availability: VAR_FNL_COUNT/VAR_COUNT < 60%
                if "VAR_COUNT" in all_upper and "VAR_FNL_COUNT" in all_upper:
                    where.append(
                        "(ISNULL([VAR_COUNT], 0) = 0 OR "
                        "CAST(ISNULL([VAR_FNL_COUNT], 0) AS FLOAT) / NULLIF([VAR_COUNT], 0) >= 0.6)"
                    )
                where_sql = (" WHERE " + " AND ".join(where)) if where else ""

                _run(wc, f"""
                    SELECT {col_list}
                    INTO [{FINAL_TABLE}]
                    FROM [{LISTING_TABLE}]
                    {where_sql}
                """)
                working_rows = wc.execute(text(f"SELECT COUNT(*) FROM [{FINAL_TABLE}]")).scalar()
                logger.info(f"{FINAL_TABLE}: {working_rows} rows (MSA_FNL_Q>0, OPT_REQ_WH>=1)")

                # ── Add ARS_GRID_HIERARCHY columns to working table ────────
                # For each hierarchy column (RNG_SEG, MACRO_MVGR, etc.):
                #   Add new column H_{name} = (1 if {name}_REQ > 0 else 0) × hierarchy value
                # Existing REQ columns are NOT modified — H_ columns are added alongside.
                HIER_TABLE = "ARS_GRID_HIERARCHY"
                if _table_exists(wc, HIER_TABLE):
                    hier_cols = _get_columns(wc, HIER_TABLE)
                    work_cols_upper = {c.upper() for c in _get_columns(wc, FINAL_TABLE)}

                    add_cols = []
                    set_parts = []

                    # Load grid_group (Primary/Secondary) for each hierarchy column
                    # Map: last hierarchy col → grid_group from ARS_GRID_BUILDER
                    grid_groups = {}  # {HIER_COL_UPPER: grid_group}
                    _SKIP_ART = {"GEN_ART_NUMBER", "ARTICLE_NUMBER", "GEN_ART", "VAR_ART"}
                    try:
                        gb_rows = wc.execute(text(
                            "SELECT grid_name, hierarchy_columns, ISNULL(grid_group, 'None') "
                            "FROM [ARS_GRID_BUILDER] WHERE UPPER(status)='ACTIVE' ORDER BY seq"
                        )).fetchall()
                        for gn, hj, gg in gb_rows:
                            try:
                                h = json.loads(hj) if isinstance(hj, str) else hj
                            except Exception:
                                continue
                            if not h or len(h) < 2:
                                continue
                            # Skip article-level grids (same rule as hierarchy table)
                            if any(x.upper() in _SKIP_ART for x in h):
                                continue
                            last = h[-1].upper()
                            if last not in ("WERKS", "MAJ_CAT"):
                                grid_groups[last] = gg
                        # MJ grid → MAJ_CAT level
                        grid_groups["MJ"] = next(
                            (gg for gn, hj, gg in gb_rows if gn.upper() == "MJ"), "Primary"
                        )
                    except Exception:
                        pass
                    logger.info(f"Grid group mapping: {grid_groups}")

                    pri_gh = []   # GH_ col names for Primary grids
                    sec_gh = []   # GH_ col names for Secondary grids
                    pri_h = []    # H_ col names for Primary grids
                    sec_h = []    # H_ col names for Secondary grids

                    # ── Step 1: GH_MJ + all GH_ columns (raw hierarchy 0/1) ──
                    # GH_MJ = 1 if MAJ_CAT matched in hierarchy
                    gh_mj = "GH_MJ"
                    if gh_mj not in work_cols_upper:
                        try: _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [{gh_mj}] INT NULL DEFAULT 0")
                        except Exception: pass
                    set_parts.append(f"W.[{gh_mj}] = 1")  # always 1 (MJ base grid)
                    add_cols.append(gh_mj)
                    if grid_groups.get("MJ", "Primary") == "Primary":
                        pri_gh.append(gh_mj)
                    elif grid_groups.get("MJ") == "Secondary":
                        sec_gh.append(gh_mj)

                    for hc in hier_cols:
                        if hc.upper() == "MAJ_CAT":
                            continue
                        col = f"GH_{hc.upper()}"
                        if col not in work_cols_upper:
                            try: _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [{col}] INT NULL DEFAULT 0")
                            except Exception: pass
                        # If MAJ_CAT not in hierarchy → default GH to 1 (assume all grids apply)
                        set_parts.append(
                            f"W.[{col}] = CASE WHEN H.[MAJ_CAT] IS NULL THEN 1 "
                            f"ELSE ISNULL(TRY_CAST(H.[{hc}] AS INT), 0) END")
                        add_cols.append(col)
                        grp = grid_groups.get(hc.upper(), "None")
                        if grp == "Primary": pri_gh.append(col)
                        elif grp == "Secondary": sec_gh.append(col)

                    # ── Step 2: H_MJ + all H_ columns (REQ>0 × hierarchy) ──
                    h_mj = "H_MJ"
                    if h_mj not in work_cols_upper:
                        try: _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [{h_mj}] INT NULL DEFAULT 0")
                        except Exception: pass
                    mj_req = "MJ_REQ"
                    if mj_req in work_cols_upper:
                        set_parts.append(f"W.[{h_mj}] = CASE WHEN ISNULL(W.[{mj_req}], 0) > 0 THEN 1 ELSE 0 END")
                    else:
                        set_parts.append(f"W.[{h_mj}] = 1")
                    add_cols.append(h_mj)
                    if grid_groups.get("MJ", "Primary") == "Primary": pri_h.append(h_mj)
                    elif grid_groups.get("MJ") == "Secondary": sec_h.append(h_mj)

                    for hc in hier_cols:
                        if hc.upper() == "MAJ_CAT":
                            continue
                        col = f"H_{hc.upper()}"
                        req_col = f"{hc.upper()}_REQ"
                        if col not in work_cols_upper:
                            try: _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [{col}] INT NULL DEFAULT 0")
                            except Exception: pass
                        # If MAJ_CAT not in hierarchy → treat hierarchy as 1
                        hier_val = (f"CASE WHEN H.[MAJ_CAT] IS NULL THEN 1 "
                                    f"ELSE ISNULL(TRY_CAST(H.[{hc}] AS INT), 0) END")
                        if req_col in work_cols_upper:
                            set_parts.append(
                                f"W.[{col}] = CASE WHEN ISNULL(W.[{req_col}], 0) > 0 THEN 1 ELSE 0 END "
                                f"* ({hier_val})")
                        else:
                            set_parts.append(f"W.[{col}] = ({hier_val})")
                        add_cols.append(col)
                        grp = grid_groups.get(hc.upper(), "None")
                        if grp == "Primary": pri_h.append(col)
                        elif grp == "Secondary": sec_h.append(col)

                    # ── UPDATE 1: Set all GH_ and H_ columns ──────────────
                    if set_parts:
                        try:
                            _run(wc, f"""
                                UPDATE W SET {', '.join(set_parts)}
                                FROM [{FINAL_TABLE}] W
                                LEFT JOIN [{HIER_TABLE}] H WITH (NOLOCK)
                                    ON W.[MAJ_CAT] = H.[MAJ_CAT]
                            """)
                            logger.info(f"{FINAL_TABLE}: set {len(add_cols)} GH/H cols: {add_cols}")
                        except Exception as he:
                            logger.warning(f"{FINAL_TABLE}: GH/H columns failed: {he}")

                    # ── UPDATE 2: PRI_CT% and SEC_CT% (SEPARATE UPDATE so it reads
                    #    the just-written H_/GH_ values, not pre-update zeros) ──
                    pct_sets = []
                    for pct_col, h_list, gh_list in [
                        ("PRI_CT%", pri_h, pri_gh),
                        ("SEC_CT%", sec_h, sec_gh),
                    ]:
                        if pct_col not in work_cols_upper:
                            try: _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [{pct_col}] FLOAT NULL DEFAULT 0")
                            except Exception: pass
                        if h_list and gh_list:
                            h_sum = " + ".join(f"ISNULL([{c}], 0)" for c in h_list)
                            gh_sum = " + ".join(f"ISNULL([{c}], 0)" for c in gh_list)
                            pct_sets.append(
                                f"[{pct_col}] = CASE WHEN ({gh_sum}) = 0 THEN 0 "
                                f"ELSE ROUND(CAST(({h_sum}) AS FLOAT) / ({gh_sum}) * 100, 1) END")
                        else:
                            pct_sets.append(f"[{pct_col}] = 0")
                    if pct_sets:
                        try:
                            _run(wc, f"UPDATE [{FINAL_TABLE}] SET {', '.join(pct_sets)}")
                            logger.info(f"{FINAL_TABLE}: PRI_CT%/SEC_CT% calculated (pri_h={len(pri_h)}, sec_h={len(sec_h)})")
                        except Exception as pe:
                            logger.warning(f"{FINAL_TABLE}: PRI/SEC CT% failed: {pe}")

                    # ALLOC_FLAG: 1 if PRI_CT% = 100 (eligible for allocation), 0 = fallback
                    try:
                        _run(wc, f"ALTER TABLE [{FINAL_TABLE}] ADD [ALLOC_FLAG] INT NULL DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        _run(wc, f"""
                            UPDATE [{FINAL_TABLE}]
                            SET [ALLOC_FLAG] = CASE WHEN ISNULL([PRI_CT%], 0) >= 100 THEN 1 ELSE 0 END
                        """)
                        logger.info(f"{FINAL_TABLE}: ALLOC_FLAG set (1=eligible, 0=fallback)")
                    except Exception as ae:
                        logger.warning(f"{FINAL_TABLE}: ALLOC_FLAG failed: {ae}")
                else:
                    logger.info(f"{FINAL_TABLE}: {HIER_TABLE} not found, skipping")

    except Exception as e:
        logger.warning(f"Auto-create {FINAL_TABLE} failed: {e}")

    # ── Auto-create ARS_ALLOC_WORKING (eligible options × variant articles) ──
    alloc_rows = 0
    try:
        with de.connect() as ac:
            if _table_exists(ac, FINAL_TABLE) and _table_exists(ac, "ARS_MSA_VAR_ART"):
                _run(ac, f"IF OBJECT_ID('{ALLOC_TABLE}','U') IS NOT NULL DROP TABLE [{ALLOC_TABLE}]")
                _run(ac, f"""
                    SELECT
                        W.[WERKS], W.[RDC], W.[MAJ_CAT], W.[GEN_ART_NUMBER], W.[CLR],
                        W.[GEN_ART_DESC], W.[OPT_TYPE], W.[ST_RANK],
                        W.[DPN], W.[SAL_D],
                        W.[OPT_MBQ], W.[OPT_REQ], W.[OPT_MBQ_WH], W.[OPT_REQ_WH],
                        W.[MAX_DAILY_SALE], W.[ALLOC_FLAG],
                        W.[PRI_CT%], W.[SEC_CT%],
                        V.[ARTICLE_NUMBER] AS VAR_ART,
                        V.[ARTICLE_DESC] AS VAR_DESC,
                        V.[SZ],
                        V.[MRP],
                        V.[PAK_SZ],
                        TRY_CAST(V.[FNL_Q] AS FLOAT) AS FNL_Q,
                        TRY_CAST(V.[STK_QTY] AS FLOAT) AS STK_QTY,
                        TRY_CAST(V.[PEND_QTY] AS FLOAT) AS PEND_QTY,
                        V.[RDC] AS VAR_RDC,
                        V.[FAB] AS VAR_FAB,
                        V.[SSN] AS VAR_SSN
                    INTO [{ALLOC_TABLE}]
                    FROM [{FINAL_TABLE}] W
                    INNER JOIN [ARS_MSA_VAR_ART] V WITH (NOLOCK)
                        ON  W.[MAJ_CAT] = LTRIM(RTRIM(CAST(V.[MAJ_CAT] AS NVARCHAR(200))))
                        AND W.[GEN_ART_NUMBER] = TRY_CAST(TRY_CAST(V.[GEN_ART_NUMBER] AS FLOAT) AS BIGINT)
                        AND W.[CLR] = LTRIM(RTRIM(CAST(V.[CLR] AS NVARCHAR(200))))
                        AND LTRIM(RTRIM(CAST(W.[RDC] AS NVARCHAR(50)))) = LTRIM(RTRIM(CAST(V.[RDC] AS NVARCHAR(50))))
                    WHERE W.[ALLOC_FLAG] = 1
                      AND TRY_CAST(V.[FNL_Q] AS FLOAT) > 0
                """)
                alloc_rows = ac.execute(text(f"SELECT COUNT(*) FROM [{ALLOC_TABLE}]")).scalar()
                logger.info(f"{ALLOC_TABLE}: {alloc_rows} rows (ALLOC_FLAG=1 × VAR_ART FNL_Q>0)")

                # ── Add STK_TTL fresh from variant-article-level grid ──────
                # Source: ARS_GRID_MJ_VAR_ART (hierarchy: WERKS, MAJ_CAT, VAR_ART).
                # The option-level STK_TTL was intentionally excluded above —
                # this column is added here as a fresh variant-level value.
                VAR_GRID = "ARS_GRID_MJ_VAR_ART"
                # Always create the column (defaults to 0 if grid is unavailable)
                try:
                    _run(ac, f"ALTER TABLE [{ALLOC_TABLE}] ADD [STK_TTL] FLOAT NULL")
                except Exception:
                    pass
                if _table_exists(ac, VAR_GRID):
                    try:
                        gcols = {c.upper() for c in _get_columns(ac, VAR_GRID)}
                        var_art_col = next((c for c in ("VAR_ART", "ARTICLE_NUMBER", "GEN_ART") if c in gcols), None)
                        if "STK_TTL" in gcols and "WERKS" in gcols and "MAJ_CAT" in gcols and var_art_col:
                            _run(ac, f"""
                                UPDATE A SET A.[STK_TTL] = TRY_CAST(G.[STK_TTL] AS FLOAT)
                                FROM [{ALLOC_TABLE}] A
                                INNER JOIN [{VAR_GRID}] G WITH (NOLOCK)
                                    ON  G.[WERKS] = A.[WERKS]
                                    AND G.[MAJ_CAT] = A.[MAJ_CAT]
                                    AND TRY_CAST(G.[{var_art_col}] AS BIGINT) = TRY_CAST(A.[VAR_ART] AS BIGINT)
                            """)
                            # Rows with no match in the variant-grid → 0 (no variant stock)
                            _run(ac, f"UPDATE [{ALLOC_TABLE}] SET [STK_TTL] = 0 WHERE [STK_TTL] IS NULL")
                            matched = ac.execute(text(
                                f"SELECT COUNT(*) FROM [{ALLOC_TABLE}] WHERE [STK_TTL] > 0"
                            )).scalar()
                            logger.info(f"{ALLOC_TABLE}: STK_TTL set from {VAR_GRID} (var_col={var_art_col}); {matched}/{alloc_rows} rows have stock>0")
                        else:
                            _run(ac, f"UPDATE [{ALLOC_TABLE}] SET [STK_TTL] = 0 WHERE [STK_TTL] IS NULL")
                            logger.warning(f"{ALLOC_TABLE}: {VAR_GRID} missing required cols (WERKS/MAJ_CAT/STK_TTL/variant-art); STK_TTL set to 0")
                    except Exception as se:
                        _run(ac, f"UPDATE [{ALLOC_TABLE}] SET [STK_TTL] = 0 WHERE [STK_TTL] IS NULL")
                        logger.warning(f"{ALLOC_TABLE}: variant-level STK_TTL load failed: {se}")
                else:
                    _run(ac, f"UPDATE [{ALLOC_TABLE}] SET [STK_TTL] = 0 WHERE [STK_TTL] IS NULL")
                    logger.info(f"{ALLOC_TABLE}: {VAR_GRID} not found; STK_TTL set to 0")

                # ── Size-level CONT from Master_CONT_SZ ────────────────────
                # Join on WERKS + MAJ_CAT + SZ (store-level), with CO-level fallback.
                # Then compute SZ_MBQ = OPT_MBQ × CONT
                #              SZ_REQ = ROUND(MAX(SZ_MBQ - STK_TTL, 0), 0)
                if _table_exists(ac, "Master_CONT_SZ"):
                    for col, typ in (("CONT", "FLOAT"), ("SZ_MBQ", "FLOAT"), ("SZ_REQ", "FLOAT")):
                        try:
                            _run(ac, f"ALTER TABLE [{ALLOC_TABLE}] ADD [{col}] {typ} NULL")
                        except Exception:
                            pass
                    try:
                        # Store-level CONT
                        _run(ac, f"""
                            UPDATE A SET A.[CONT] = TRY_CAST(M.[CONT] AS FLOAT)
                            FROM [{ALLOC_TABLE}] A
                            INNER JOIN [Master_CONT_SZ] M WITH (NOLOCK)
                                ON  LTRIM(RTRIM(CAST(M.[ST_CD] AS NVARCHAR(50)))) = LTRIM(RTRIM(CAST(A.[WERKS] AS NVARCHAR(50))))
                                AND LTRIM(RTRIM(CAST(M.[MAJ_CAT] AS NVARCHAR(200)))) = A.[MAJ_CAT]
                                AND LTRIM(RTRIM(CAST(M.[SZ] AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.[SZ] AS NVARCHAR(200))))
                        """)
                        # CO-level fallback for missing CONT
                        _run(ac, f"""
                            UPDATE A SET A.[CONT] = TRY_CAST(M.[CONT] AS FLOAT)
                            FROM [{ALLOC_TABLE}] A
                            INNER JOIN [Master_CONT_SZ] M WITH (NOLOCK)
                                ON  LTRIM(RTRIM(CAST(M.[ST_CD] AS NVARCHAR(50)))) = 'CO'
                                AND LTRIM(RTRIM(CAST(M.[MAJ_CAT] AS NVARCHAR(200)))) = A.[MAJ_CAT]
                                AND LTRIM(RTRIM(CAST(M.[SZ] AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.[SZ] AS NVARCHAR(200))))
                            WHERE A.[CONT] IS NULL
                        """)
                        # SZ_MBQ = OPT_MBQ × CONT;  SZ_REQ = ROUND(MAX(SZ_MBQ - STK_TTL, 0), 0)
                        _run(ac, f"""
                            UPDATE [{ALLOC_TABLE}]
                            SET [SZ_MBQ] = ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0),
                                [SZ_REQ] = CASE
                                    WHEN (ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0)) - ISNULL([STK_TTL], 0) > 0
                                        THEN ROUND((ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0)) - ISNULL([STK_TTL], 0), 0)
                                    ELSE 0
                                END
                        """)
                        cont_rows = ac.execute(text(
                            f"SELECT COUNT(*) FROM [{ALLOC_TABLE}] WHERE [CONT] IS NOT NULL"
                        )).scalar()
                        logger.info(f"{ALLOC_TABLE}: CONT applied ({cont_rows}/{alloc_rows} rows), SZ_MBQ + SZ_REQ calculated")
                    except Exception as ce:
                        logger.warning(f"{ALLOC_TABLE}: CONT/SZ_MBQ/SZ_REQ failed: {ce}")
                else:
                    logger.info(f"{ALLOC_TABLE}: Master_CONT_SZ not found, skipping SZ_MBQ/SZ_REQ")

                # ── ALLOC_QTY: WATERFALL allocation (sequential pool consumption) ──
                # FNL_Q is a SHARED pool per (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR,
                # VAR_ART, SZ) — every store with that RDC competes for the same
                # variant stock. Naive MIN(FNL_Q, SZ_REQ) double-counts: each
                # store sees the full pool. The fix:
                #   1. Order stores within the pool by ST_RANK ASC (best store first)
                #   2. Track cumulative demand of preceding stores (prev_demand)
                #   3. ALLOC_QTY = MIN(SZ_REQ, FNL_Q - prev_demand), floored at 0
                # Same fix at GEN_ART/option level handled by the SUM-reflection
                # downstream (since variant-level exhaustion implicitly caps the
                # option-level total per store).
                try:
                    try:
                        _run(ac, f"ALTER TABLE [{ALLOC_TABLE}] ADD [ALLOC_QTY] FLOAT NULL")
                    except Exception:
                        pass
                    for col in ("PREV_ALLOC", "FNL_Q_REM"):
                        try:
                            _run(ac, f"ALTER TABLE [{ALLOC_TABLE}] ADD [{col}] FLOAT NULL")
                        except Exception:
                            pass
                    _run(ac, f"""
                        ;WITH PoolDemand AS (
                            SELECT
                                [WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ],
                                ISNULL([FNL_Q], 0) AS FNL_Q,
                                ISNULL([SZ_REQ], 0) AS SZ_REQ,
                                SUM(ISNULL([SZ_REQ], 0)) OVER (
                                    PARTITION BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ]
                                    ORDER BY ISNULL([ST_RANK], 999999) ASC, [WERKS]
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                                ) AS prev_demand
                            FROM [{ALLOC_TABLE}]
                        ),
                        Allocated AS (
                            SELECT *,
                                CASE
                                    WHEN FNL_Q - ISNULL(prev_demand, 0) <= 0 THEN 0
                                    WHEN SZ_REQ <= FNL_Q - ISNULL(prev_demand, 0) THEN SZ_REQ
                                    ELSE FNL_Q - ISNULL(prev_demand, 0)
                                END AS new_alloc
                            FROM PoolDemand
                        )
                        UPDATE A SET
                            A.[PREV_ALLOC] = ISNULL(P.prev_demand, 0),
                            A.[ALLOC_QTY]  = P.new_alloc,
                            A.[FNL_Q_REM]  = CASE
                                WHEN P.FNL_Q - ISNULL(P.prev_demand, 0) - P.new_alloc < 0 THEN 0
                                ELSE P.FNL_Q - ISNULL(P.prev_demand, 0) - P.new_alloc
                            END
                        FROM [{ALLOC_TABLE}] A
                        INNER JOIN Allocated P
                            ON  A.[WERKS] = P.[WERKS]
                            AND A.[RDC] = P.[RDC]
                            AND A.[MAJ_CAT] = P.[MAJ_CAT]
                            AND A.[GEN_ART_NUMBER] = P.[GEN_ART_NUMBER]
                            AND A.[CLR] = P.[CLR]
                            AND A.[VAR_ART] = P.[VAR_ART]
                            AND A.[SZ] = P.[SZ]
                    """)
                    # Diagnostic: how many rows got 0 due to pool exhaustion?
                    starved = ac.execute(text(
                        f"SELECT COUNT(*) FROM [{ALLOC_TABLE}] "
                        f"WHERE ISNULL([SZ_REQ],0) > 0 AND ISNULL([ALLOC_QTY],0) = 0"
                    )).scalar()
                    logger.info(f"{ALLOC_TABLE}: ALLOC_QTY waterfall applied (ST_RANK order); "
                                f"{starved} rows starved (had demand but pool exhausted by higher-rank stores)")
                    # Reflect option-level ALLOC_QTY (SUM of size-level) into LISTING_WORKING
                    try:
                        _run(ac, f"ALTER TABLE [{FINAL_TABLE}] ADD [ALLOC_QTY] FLOAT NULL")
                    except Exception:
                        pass
                    _run(ac, f"""
                        UPDATE W SET W.[ALLOC_QTY] = A.[TOT_ALLOC]
                        FROM [{FINAL_TABLE}] W
                        INNER JOIN (
                            SELECT [WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                                   SUM(ISNULL([ALLOC_QTY], 0)) AS TOT_ALLOC
                            FROM [{ALLOC_TABLE}]
                            GROUP BY [WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
                        ) A
                            ON  W.[WERKS] = A.[WERKS]
                            AND W.[RDC] = A.[RDC]
                            AND W.[MAJ_CAT] = A.[MAJ_CAT]
                            AND W.[GEN_ART_NUMBER] = A.[GEN_ART_NUMBER]
                            AND W.[CLR] = A.[CLR]
                    """)
                    logger.info(f"{ALLOC_TABLE}: ALLOC_QTY (waterfall) reflected to {FINAL_TABLE} as option-level SUM")
                except Exception as qe:
                    logger.warning(f"{ALLOC_TABLE}: ALLOC_QTY failed: {qe}")
            else:
                logger.info(f"Skipped {ALLOC_TABLE}: missing {FINAL_TABLE} or ARS_MSA_VAR_ART")
    except Exception as e:
        logger.warning(f"Auto-create {ALLOC_TABLE} failed: {e}")

    duration = round(time.time() - start, 1)
    logger.info(f"ARS_LISTING: {total} rows (grid={grid_count}, new={new_count}) in {duration}s")

    # Summary of all step timings
    logger.info("="*60)
    logger.info("STEP TIMINGS SUMMARY:")
    for st in step_timings:
        logger.info(f"  {st['step']:<45} {st['seconds']:>7}s")
    logger.info(f"  {'TOTAL':<45} {duration:>7}s")
    logger.info("="*60)

    return {
        "success": True,
        "message": (f"{pipeline_msg}Listing: {total:,} rows ({grid_count:,} grid + {new_count:,} new) "
                    f"| Working: {working_rows:,} | Alloc: {alloc_rows:,} | MIX={mixl_count}, TBL={tbl_count}, TBC={toc_count}, RL={rl_count} in {duration}s"),
        "data": {
            "total_rows": total, "existing_rows": grid_count,
            "new_rows": new_count, "working_rows": working_rows, "alloc_rows": alloc_rows,
            "duration_sec": duration,
            "stock_columns": len(stock_cols),
            "opt_type": {
                "MIX": mixl_count, "TBL": tbl_count, "TBC": toc_count,
                "RL": rl_count, "NL": nl_count, "untagged": untagged,
            },
            "step_timings": step_timings,
        }
    }


# ===========================================================================
# FINAL TABLE — filtered + cleaned extract from ARS_LISTING
# ===========================================================================

@router.post("/create-final")
def create_final_table(
    body: dict = None,
    current_user: User = Depends(get_current_user),
):
    """
    Create ARS_LISTING_FINAL from ARS_LISTING:
      - Filter: MSA_FNL_Q > 0 AND OPT_REQ_WH >= 1
      - Columns: only identity + calculated outputs (no SLOC stock, no Part 4 grid-prefix)

    Optional body params:
      min_opt_req_wh: float (default 1) — minimum OPT_REQ_WH to include
      min_msa_fnl_q: float (default 0) — minimum MSA_FNL_Q (> this value)
      extra_keep_cols: list[str] — additional columns to keep beyond defaults
      extra_filters: dict — {column: {op: 'gte'|'gt'|'lte'|'lt'|'eq', value: N}}
    """
    import time as _t
    start = _t.time()
    body = body or {}
    min_req_wh = float(body.get("min_opt_req_wh", 1))
    min_fnl_q  = float(body.get("min_msa_fnl_q", 0))
    extra_keep = set(c.upper() for c in body.get("extra_keep_cols", []))
    extra_filters = body.get("extra_filters", {})

    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, LISTING_TABLE):
            raise HTTPException(404, f"{LISTING_TABLE} not found. Generate listing first.")

        # Get all listing columns
        all_cols = _get_columns(conn, LISTING_TABLE)
        all_upper = {c.upper(): c for c in all_cols}

        # Determine which columns to include
        keep = _FINAL_KEEP_COLS | extra_keep
        selected = [c for c in all_cols if c.upper() in keep]
        if not selected:
            raise HTTPException(400, "No columns selected for final table")

        col_list = ", ".join(f"[{c}]" for c in selected)

        # Build WHERE clause
        where_parts = []
        params = {}

        # MSA_FNL_Q > min_fnl_q
        if "MSA_FNL_Q" in all_upper:
            where_parts.append(f"ISNULL([MSA_FNL_Q], 0) > :min_fnl")
            params["min_fnl"] = min_fnl_q

        # OPT_REQ_WH >= min_req_wh
        if "OPT_REQ_WH" in all_upper:
            where_parts.append(f"ISNULL([OPT_REQ_WH], 0) >= :min_req")
            params["min_req"] = min_req_wh

        # Extra user-supplied filters
        for i, (col, flt) in enumerate(extra_filters.items()):
            if col.upper() not in all_upper:
                continue
            actual = all_upper[col.upper()]
            op_map = {"gte": ">=", "gt": ">", "lte": "<=", "lt": "<", "eq": "="}
            op = op_map.get(flt.get("op", "gte"), ">=")
            pname = f"ef{i}"
            where_parts.append(f"ISNULL([{actual}], 0) {op} :{pname}")
            params[pname] = float(flt.get("value", 0))

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # Drop + create final table
        _run(conn, f"IF OBJECT_ID('{FINAL_TABLE}','U') IS NOT NULL DROP TABLE [{FINAL_TABLE}]")
        _run(conn, f"""
            SELECT {col_list}
            INTO [{FINAL_TABLE}]
            FROM [{LISTING_TABLE}]
            {where_sql}
            ORDER BY [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        """, params)

        row_count = conn.execute(text(f"SELECT COUNT(*) FROM [{FINAL_TABLE}]")).scalar()
        src_count = conn.execute(text(f"SELECT COUNT(*) FROM [{LISTING_TABLE}]")).scalar()

    duration = round(_t.time() - start, 1)
    logger.info(f"ARS_LISTING_FINAL: {row_count} rows (from {src_count} listing rows) in {duration}s")

    return {
        "success": True,
        "message": f"Final: {row_count:,} rows from {src_count:,} listing (MSA_FNL_Q>{min_fnl_q}, OPT_REQ_WH>={min_req_wh}) in {duration}s",
        "data": {
            "table": FINAL_TABLE,
            "rows": row_count,
            "source_rows": src_count,
            "columns": selected,
            "filters_applied": where_parts,
            "duration_sec": duration,
        },
    }


@router.get("/final/preview")
def preview_final(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=5000),
    search: Optional[str] = None,
    current_user: User = Depends(get_current_user),
):
    """Preview ARS_LISTING_FINAL with pagination and search."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, FINAL_TABLE):
            raise HTTPException(404, f"{FINAL_TABLE} not found. Create it first.")

        cols = _get_columns(conn, FINAL_TABLE)
        where_parts = []
        params = {}

        if search and search.strip():
            search_conds = [f"CAST([{c}] AS NVARCHAR(MAX)) LIKE :_gs" for c in cols]
            where_parts.append(f"({' OR '.join(search_conds)})")
            params["_gs"] = f"%{search.strip()}%"

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        total = conn.execute(text(f"SELECT COUNT(*) FROM [{FINAL_TABLE}]{where_sql}"), params).scalar()

        col_list = ", ".join(f"[{c}]" for c in cols)
        offset = (page - 1) * page_size
        rows = conn.execute(text(f"""
            SELECT {col_list} FROM [{FINAL_TABLE}]{where_sql}
            ORDER BY [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
            OFFSET :off ROWS FETCH NEXT :ps ROWS ONLY
        """), {**params, "off": offset, "ps": page_size}).fetchall()

        data = [dict(zip(cols, r)) for r in rows]

    return {
        "success": True,
        "data": {"columns": cols, "data": data, "total": total, "page": page, "page_size": page_size},
    }


@router.get("/alloc-preview")
def preview_alloc(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=5000),
    search: Optional[str] = None,
    current_user: User = Depends(get_current_user),
):
    """Preview ARS_ALLOC_WORKING with pagination and search."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, ALLOC_TABLE):
            raise HTTPException(404, f"{ALLOC_TABLE} not found. Generate listing first.")
        cols = _get_columns(conn, ALLOC_TABLE)
        where_parts, params = [], {}
        if search and search.strip():
            conds = [f"CAST([{c}] AS NVARCHAR(MAX)) LIKE :_gs" for c in cols]
            where_parts.append(f"({' OR '.join(conds)})")
            params["_gs"] = f"%{search.strip()}%"
        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        total = conn.execute(text(f"SELECT COUNT(*) FROM [{ALLOC_TABLE}]{where_sql}"), params).scalar()
        col_list = ", ".join(f"[{c}]" for c in cols)
        offset = (page - 1) * page_size
        rows = conn.execute(text(f"""
            SELECT {col_list} FROM [{ALLOC_TABLE}]{where_sql}
            ORDER BY ISNULL([ST_RANK], 999999) ASC, [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [SZ]
            OFFSET :off ROWS FETCH NEXT :ps ROWS ONLY
        """), {**params, "off": offset, "ps": page_size}).fetchall()
        data = [dict(zip(cols, r)) for r in rows]
    return {
        "success": True,
        "data": {"columns": cols, "data": data, "total": total, "page": page, "page_size": page_size},
    }


@router.get("/store-ranking")
def preview_store_ranking(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=5000),
    search: Optional[str] = None,
    current_user: User = Depends(get_current_user),
):
    """Preview ARS_STORE_RANKING with pagination and search."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, "ARS_STORE_RANKING"):
            raise HTTPException(404, "ARS_STORE_RANKING not found. Generate listing first.")
        cols = _get_columns(conn, "ARS_STORE_RANKING")
        where_parts, params = [], {}
        if search and search.strip():
            conds = [f"CAST([{c}] AS NVARCHAR(MAX)) LIKE :_gs" for c in cols]
            where_parts.append(f"({' OR '.join(conds)})")
            params["_gs"] = f"%{search.strip()}%"
        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        total = conn.execute(text(f"SELECT COUNT(*) FROM [ARS_STORE_RANKING]{where_sql}"), params).scalar()
        col_list = ", ".join(f"[{c}]" for c in cols)
        offset = (page - 1) * page_size
        rows = conn.execute(text(f"""
            SELECT {col_list} FROM [ARS_STORE_RANKING]{where_sql}
            ORDER BY [MAJ_CAT], [ST_RANK] DESC
            OFFSET :off ROWS FETCH NEXT :ps ROWS ONLY
        """), {**params, "off": offset, "ps": page_size}).fetchall()
        data = [dict(zip(cols, r)) for r in rows]
    return {
        "success": True,
        "data": {"columns": cols, "data": data, "total": total, "page": page, "page_size": page_size},
    }


@router.get("/preview")
def preview_listing(
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=5000),
    filters: Optional[str] = None,
    search: Optional[str] = None,
    table: str = Query("working", pattern="^(listing|working|alloc)$"),
    current_user: User = Depends(get_current_user),
):
    """Preview ARS_LISTING, ARS_LISTING_WORKING, or ARS_ALLOC_WORKING with column filters, global search, and pagination."""
    tbl = {"working": FINAL_TABLE, "alloc": ALLOC_TABLE}.get(table, LISTING_TABLE)
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, tbl):
            raise HTTPException(404, f"{tbl} not found. Generate listing first.")

        cols = _get_columns(conn, tbl)
        where_parts, params = _build_filter_where(filters, set(cols))

        if search and search.strip():
            search_conds = [f"CAST([{c}] AS NVARCHAR(MAX)) LIKE :_gsearch" for c in cols]
            where_parts.append(f"({' OR '.join(search_conds)})")
            params["_gsearch"] = f"%{search.strip()}%"

        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        total = conn.execute(text(f"SELECT COUNT(*) FROM [{tbl}]{where_sql}"), params).scalar()

        col_list = ", ".join(f"[{c}]" for c in cols)
        offset = (page - 1) * page_size
        # Working: sort by ST_RANK + OPT_TYPE + OPT_REQ
        # Alloc:   sort by ST_RANK + MAJ_CAT + GEN_ART_NUMBER + CLR + SZ
        # Listing: sort by WERKS + MAJ_CAT + GEN_ART_NUMBER + CLR
        if table == "working":
            order = "ISNULL([ST_RANK], 999999) ASC, ISNULL([OPT_TYPE], 'ZZZ') ASC, ISNULL([SEC_CT%], 0) DESC, ISNULL([MAX_DAILY_SALE], 0) DESC, ISNULL([OPT_REQ], 0) DESC"
        elif table == "alloc":
            order = "ISNULL([ST_RANK], 999999) ASC, [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [SZ]"
        else:
            order = "[WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]"
        rows = conn.execute(text(f"""
            SELECT {col_list} FROM [{tbl}]{where_sql}
            ORDER BY {order}
            OFFSET :off ROWS FETCH NEXT :ps ROWS ONLY
        """), {**params, "off": offset, "ps": page_size}).fetchall()

        data = [dict(zip(cols, row)) for row in rows]

    return {
        "success": True,
        "data": {"data": data, "total": total, "columns": cols, "page": page, "page_size": page_size, "table": tbl}
    }


@router.get("/summary")
def listing_summary(current_user: User = Depends(get_current_user)):
    """Summary stats for ARS_LISTING."""
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, LISTING_TABLE):
            return {"success": True, "data": None}

        summary = {}

        rows = conn.execute(text(f"""
            SELECT [RDC], COUNT(*) AS cnt,
                   SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END) AS new_cnt,
                   SUM(CASE WHEN [IS_NEW] = 0 THEN 1 ELSE 0 END) AS existing_cnt
            FROM [{LISTING_TABLE}]
            GROUP BY [RDC] ORDER BY [RDC]
        """)).fetchall()
        summary["by_rdc"] = [{"rdc": r[0], "total": r[1], "new": r[2], "existing": r[3]} for r in rows]

        rows = conn.execute(text(f"""
            SELECT TOP 20 [MAJ_CAT], COUNT(*) AS cnt,
                   SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END) AS new_cnt
            FROM [{LISTING_TABLE}]
            GROUP BY [MAJ_CAT] ORDER BY cnt DESC
        """)).fetchall()
        summary["by_maj_cat"] = [{"maj_cat": r[0], "total": r[1], "new": r[2]} for r in rows]

        # GEN_ART_NUMBER is BIGINT — must CAST for string concatenation
        opt_key = "ISNULL([MAJ_CAT],'') + '|' + ISNULL(CAST([GEN_ART_NUMBER] AS NVARCHAR(50)),'') + '|' + ISNULL([CLR],'')"
        row = conn.execute(text(f"""
            SELECT COUNT(*) AS total,
                   ISNULL(SUM(CASE WHEN [IS_NEW] = 1 THEN 1 ELSE 0 END), 0) AS new_rows,
                   COUNT(DISTINCT [WERKS]) AS stores,
                   COUNT(DISTINCT [RDC]) AS rdcs,
                   COUNT(DISTINCT {opt_key}) AS options,
                   COUNT(DISTINCT CASE WHEN [IS_NEW] = 1 THEN {opt_key} END) AS new_options,
                   COUNT(DISTINCT CASE WHEN [IS_NEW] = 0 THEN {opt_key} END) AS existing_options
            FROM [{LISTING_TABLE}]
        """)).fetchone()
        total = row[0] or 0
        new_rows = row[1] or 0
        summary["totals"] = {
            "total": total, "new": new_rows, "existing": total - new_rows,
            "stores": row[2] or 0, "rdcs": row[3] or 0,
            "options": row[4] or 0,
            "new_options": row[5] or 0,
            "existing_options": row[6] or 0,
        }

        # OPT_TYPE breakdown
        cols = _get_columns(conn, LISTING_TABLE)
        if "OPT_TYPE" in cols:
            opt_rows = conn.execute(text(f"""
                SELECT ISNULL([OPT_TYPE], 'UNTAGGED') AS opt, COUNT(*) AS cnt
                FROM [{LISTING_TABLE}]
                GROUP BY [OPT_TYPE]
            """)).fetchall()
            summary["by_opt_type"] = {r[0]: r[1] for r in opt_rows}

    return {"success": True, "data": summary}


@router.get("/export")
def export_listing(
    filters: Optional[str] = None,
    table: str = Query("working", pattern="^(listing|working|alloc)$"),
    current_user: User = Depends(get_current_user),
):
    """Export active table (Working, Full Listing, or Alloc) to Excel."""
    import pandas as pd

    tbl = {"working": FINAL_TABLE, "alloc": ALLOC_TABLE}.get(table, LISTING_TABLE)
    de = get_data_engine()
    with de.connect() as conn:
        if not _table_exists(conn, tbl):
            raise HTTPException(404, f"{tbl} not found.")

        cols = _get_columns(conn, tbl)
        where_parts, params = _build_filter_where(filters, set(cols))
        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""

        col_list = ", ".join(f"[{c}]" for c in cols)
        if table == "working":
            order = "ISNULL([ST_RANK], 999999) ASC, ISNULL([OPT_TYPE], 'ZZZ') ASC, ISNULL([SEC_CT%], 0) DESC, ISNULL([MAX_DAILY_SALE], 0) DESC, ISNULL([OPT_REQ], 0) DESC"
        elif table == "alloc":
            order = "ISNULL([ST_RANK], 999999) ASC, [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [SZ]"
        else:
            order = "[WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]"
        sql = f"SELECT {col_list} FROM [{tbl}]{where_sql} ORDER BY {order}"
        df = pd.read_sql(text(sql), conn, params=params)

    sheet = {"working": "ARS_LISTING_WORKING", "alloc": "ARS_ALLOC_WORKING"}.get(table, "ARS_LISTING")
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=ARS_LISTING_{len(df)}_rows.xlsx"}
    )
