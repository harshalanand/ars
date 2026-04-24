"""
rule_engine_new.py — Stage A (List OPTs) + Stages B/C/D (Allocate VAR_ART × SZ).

Full spec: docs/NEW_RULE_ENGINE_SPEC.md

Entry point:
    run_listing_and_allocation(conn, working_table, listed_table, alloc_table, ...)

This module is self-contained. It does NOT import or call the old
`rule_engine.py` or `listing_allocator.py` — those are kept for reference only.

Feature-flag constants at the top let the user toggle individual rules on/off
without editing allocation SQL.
"""
from typing import Dict, List, Optional
from sqlalchemy import text
from loguru import logger
import json
import time

from app.utils.db_helpers import run_sql, table_exists, get_columns

_run = run_sql
_exists = table_exists
_cols = get_columns

# ───────────────────────────────────────────────────────────────
# FEATURE FLAGS — toggle rules here without touching SQL below.
# Defaults match docs/NEW_RULE_ENGINE_SPEC.md §7.
# ───────────────────────────────────────────────────────────────
RULE_R01_LISTING          = True
RULE_R02_NOT_MIX          = True
RULE_R03_NOT_NL           = True
RULE_R04_MSA_POS          = True
RULE_R05_REQ_POS          = True
RULE_R06_PRI_100          = True
RULE_R07_VAR_RATIO_TBL    = True
RULE_R09_TBL_TRIVIAL      = True

ENABLE_FOCUS_TIERING      = True
ENABLE_STORE_BROKEN       = True   # MJ_REQ_REM < factor × ACS_D → skip store in opt_type
ENABLE_GRID_OVERFLOW      = False
ENABLE_SIZE_COVERAGE_BREAK = False

ENABLE_PER_OPT_REVALIDATION = True   # revalidate after each band (requires BAND_SIZE=1)
ACS_SKIP_FACTOR           = 0.5     # MJ_REQ_REM < factor*ACS_D → skip; H_REM=0 if REQ_REM <= factor*ACS_D

OPT_TYPE_ORDER = ["RL", "TBC", "TBL"]
BAND_SIZE = 1  # rank band width; 1 = strict option-by-option (required for per-OPT revalidation)

POOL_TABLE = "#nre_pool"
_SKIP_ART = {"GEN_ART_NUMBER", "ARTICLE_NUMBER", "GEN_ART", "VAR_ART"}


# ───────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ───────────────────────────────────────────────────────────────
def run_listing_and_allocation(
    conn,
    working_table: str = "ARS_LISTING_WORKING",
    listed_table: str = "ARS_LISTED_OPT",
    alloc_table: str = "ARS_ALLOC_WORKING",
    msa_var_table: str = "ARS_MSA_VAR_ART",
    var_grid_table: str = "ARS_GRID_MJ_VAR_ART",
    cont_table: str = "Master_CONT_SZ",
    size_threshold: float = 0.6,
    min_size_count: int = 3,
    tbl_trivial_factor: float = 0.5,
    pri_ct_check_rl: bool = True,   # apply PRI_CT%>=100 gate to RL?
    pri_ct_check_tbc: bool = True,  # apply PRI_CT%>=100 gate to TBC?
) -> Dict:
    """
    Orchestrates Stages A–D. See docs/NEW_RULE_ENGINE_SPEC.md.
    """
    t0 = time.time()
    result = {
        "listed_opts": 0,
        "dropped_opts": 0,
        "alloc_rows": 0,
        "ship_qty_total": 0.0,
        "hold_qty_total": 0.0,
        "duration_sec": 0.0,
    }

    if not _exists(conn, working_table) or not _exists(conn, msa_var_table):
        logger.warning(
            f"rule_engine_new: missing {working_table} or {msa_var_table} — skipping"
        )
        return result

    # STAGE A — list OPTs
    _stage_a_add_columns(conn, working_table)
    _stage_a_apply_rules(conn, working_table, size_threshold, min_size_count,
                         tbl_trivial_factor,
                         pri_ct_check_rl=pri_ct_check_rl,
                         pri_ct_check_tbc=pri_ct_check_tbc)
    _stage_a_assign_tier(conn, working_table)
    _stage_a_assign_rank(conn, working_table)
    listed_count = _stage_a_materialize_listed(conn, working_table, listed_table)
    result["listed_opts"] = listed_count
    logger.info(f"[A] listed={listed_count}")

    if listed_count == 0:
        result["duration_sec"] = round(time.time() - t0, 1)
        return result

    # STAGE B — explode to VAR_ART × SZ
    base_rows = _stage_b_explode(conn, listed_table, alloc_table, msa_var_table)
    logger.info(f"[B] alloc rows = {base_rows}")
    if base_rows == 0:
        result["duration_sec"] = round(time.time() - t0, 1)
        return result

    _stage_b_fill_cont(conn, alloc_table, cont_table)
    _stage_b_fill_targets(conn, alloc_table, var_grid_table)
    _stage_b_indexes(conn, alloc_table)

    # Primary-grid map + _REM shadow columns (seeded from originals)
    grids = _discover_primary_grids(conn)
    logger.info(f"[C] primary grids = {list(grids.keys())}")
    if ENABLE_PER_OPT_REVALIDATION:
        _init_rem_columns(conn, working_table, grids)

    # STAGE C — allocate
    _stage_c_build_pool(conn, alloc_table)
    _stage_c_waterfall(conn, alloc_table, working_table, grids,
                        pri_ct_check_rl=pri_ct_check_rl,
                        pri_ct_check_tbc=pri_ct_check_tbc)

    # STAGE D — reflect back to listing working
    _stage_d_reflect(conn, working_table, alloc_table)

    # Totals
    totals = conn.execute(text(f"""
        SELECT COUNT(*), ISNULL(SUM(SHIP_QTY),0), ISNULL(SUM(HOLD_QTY),0)
        FROM [{alloc_table}]
        WHERE ISNULL(SHIP_QTY,0) > 0 OR ISNULL(HOLD_QTY,0) > 0
    """)).fetchone()
    result["alloc_rows"] = int(totals[0] or 0)
    result["ship_qty_total"] = float(totals[1] or 0)
    result["hold_qty_total"] = float(totals[2] or 0)

    _cleanup(conn)
    result["duration_sec"] = round(time.time() - t0, 1)
    logger.info(
        f"rule_engine_new DONE: listed={result['listed_opts']}, "
        f"alloc_rows={result['alloc_rows']}, ship={result['ship_qty_total']:.0f}, "
        f"hold={result['hold_qty_total']:.0f}, {result['duration_sec']}s"
    )
    return result


# ───────────────────────────────────────────────────────────────
# STAGE A — LIST OPTs
# ───────────────────────────────────────────────────────────────
def _stage_a_add_columns(conn, working_table):
    """Add LISTED_FLAG / LISTED_REASON / OPT_PRIORITY_* columns idempotently."""
    cols = {
        "LISTED_FLAG":       "INT NULL",
        "LISTED_REASON":     "NVARCHAR(500) NULL",
        "OPT_PRIORITY_RANK": "INT NULL",
        "OPT_PRIORITY_TIER": "INT NULL",
        "ALLOC_QTY":         "FLOAT NULL",
        "HOLD_QTY":          "FLOAT NULL",
        "ALLOC_STATUS":      "NVARCHAR(50) NULL",
        "ALLOC_REMARKS":     "NVARCHAR(MAX) NULL",
    }
    existing = {c.upper() for c in _cols(conn, working_table)}
    for col, typedef in cols.items():
        if col.upper() in existing:
            continue
        try:
            _run(conn, f"ALTER TABLE [{working_table}] ADD [{col}] {typedef}")
        except Exception:
            pass

    # Reset status fields (idempotent rerun support)
    _run(conn, f"""
        UPDATE [{working_table}] SET
            LISTED_FLAG=0, LISTED_REASON='',
            OPT_PRIORITY_RANK=NULL, OPT_PRIORITY_TIER=NULL,
            ALLOC_QTY=0, HOLD_QTY=0,
            ALLOC_STATUS='PENDING', ALLOC_REMARKS=''
    """)


def _stage_a_apply_rules(conn, working_table, size_threshold, min_size_count,
                          tbl_trivial_factor,
                          pri_ct_check_rl: bool = True,
                          pri_ct_check_tbc: bool = True):
    """
    Chain every rule into a reason string. LISTED_FLAG=1 iff the chain is empty.
    Rules are guarded by feature flags so the user can turn any off.

    pri_ct_check_rl / pri_ct_check_tbc:
        Scope the PRI_CT%>=100 gate (R06). TBL always enforces. RL and TBC
        honour the flag — when False, they pass R06 even with PRI_CT% < 100.
    """
    pieces = []
    if RULE_R01_LISTING:
        pieces.append("CASE WHEN ISNULL(TRY_CAST([LISTING] AS INT),1) <> 1 THEN 'R01_LISTING;' ELSE '' END")
    if RULE_R02_NOT_MIX:
        pieces.append("CASE WHEN ISNULL([OPT_TYPE],'') = 'MIX' THEN 'R02_NOT_MIX;' ELSE '' END")
    if RULE_R03_NOT_NL:
        pieces.append("CASE WHEN ISNULL([OPT_TYPE],'') = 'NL'  THEN 'R03_NOT_NL;'  ELSE '' END")
    if RULE_R04_MSA_POS:
        pieces.append("CASE WHEN ISNULL(TRY_CAST([MSA_FNL_Q] AS FLOAT),0) <= 0 THEN 'R04_MSA_POS;' ELSE '' END")
    if RULE_R05_REQ_POS:
        pieces.append("CASE WHEN ISNULL(TRY_CAST([OPT_REQ_WH] AS FLOAT),0) < 1 THEN 'R05_REQ_POS;' ELSE '' END")
    if RULE_R06_PRI_100:
        # Build the list of opt_types that enforce the PRI_CT gate.
        enforced = ["'TBL'"]  # TBL always enforces
        if pri_ct_check_rl:  enforced.append("'RL'")
        if pri_ct_check_tbc: enforced.append("'TBC'")
        opt_in = ", ".join(enforced)
        pieces.append(
            "CASE WHEN ISNULL(TRY_CAST([PRI_CT%] AS FLOAT),0) < 100 "
            "      AND ISNULL(TRY_CAST([ALLOC_FLAG] AS INT),0) <> 1 "
            f"     AND ISNULL([OPT_TYPE],'') IN ({opt_in}) "
            "      THEN 'R06_PRI_100;' ELSE '' END"
        )
    if RULE_R07_VAR_RATIO_TBL:
        pieces.append(
            f"CASE WHEN ISNULL([OPT_TYPE],'') = 'TBL' "
            f"      AND ISNULL([VAR_COUNT],0) > 0 "
            f"      AND (CAST(ISNULL([VAR_FNL_COUNT],0) AS FLOAT) / NULLIF([VAR_COUNT],0)) < {size_threshold} "
            f"      AND ISNULL([VAR_FNL_COUNT],0) < {min_size_count} "
            f"      THEN 'R07_VAR_RATIO_TBL;' ELSE '' END"
        )
    if RULE_R09_TBL_TRIVIAL:
        pieces.append(
            f"CASE WHEN ISNULL([OPT_TYPE],'') = 'TBL' "
            f"      AND ISNULL([MJ_REQ],0) < {tbl_trivial_factor} * ISNULL([MAX_DAILY_SALE],0) "
            f"      THEN 'R09_TBL_TRIVIAL;' ELSE '' END"
        )

    if not pieces:
        reason_expr = "''"
    else:
        reason_expr = " + ".join(pieces)

    _run(conn, f"""
        UPDATE [{working_table}] SET
            LISTED_REASON = {reason_expr},
            LISTED_FLAG = CASE WHEN LEN({reason_expr}) = 0 THEN 1 ELSE 0 END
    """)

    r = conn.execute(text(f"""
        SELECT SUM(CASE WHEN LISTED_FLAG=1 THEN 1 ELSE 0 END),
               SUM(CASE WHEN LISTED_FLAG=0 THEN 1 ELSE 0 END),
               COUNT(*)
        FROM [{working_table}]
    """)).fetchone()
    logger.info(f"[A] rules applied: listed={r[0]} dropped={r[1]} total={r[2]}")


def _stage_a_assign_tier(conn, working_table):
    if not ENABLE_FOCUS_TIERING:
        _run(conn, f"UPDATE [{working_table}] SET OPT_PRIORITY_TIER = 3 WHERE LISTED_FLAG = 1")
        return
    _run(conn, f"""
        UPDATE [{working_table}] SET OPT_PRIORITY_TIER =
            CASE WHEN ISNULL(TRY_CAST([FOCUS_WO_CAP] AS INT),0) = 1 THEN 1
                 WHEN ISNULL(TRY_CAST([FOCUS_W_CAP]  AS INT),0) = 1 THEN 2
                 ELSE 3 END
        WHERE LISTED_FLAG = 1
    """)


def _stage_a_assign_rank(conn, working_table):
    """
    Global rank (no PARTITION). TIER sits on top inside each opt_type;
    store is then picked by ST_RANK (per-MAJ_CAT rank) before other tie-breakers.

        OPT_TYPE (RL→1, TBC→2, TBL→3) ASC,
        OPT_PRIORITY_TIER (1=focus-uncapped, 2=focus-capped, 3=regular) ASC,
        ST_RANK ASC,                              ← store select (per MAJ_CAT)
        SEC_CT% DESC, MAX_DAILY_SALE DESC, OPT_REQ_WH DESC
    """
    _run(conn, f"""
        ;WITH R AS (
            SELECT WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR,
                   ROW_NUMBER() OVER (
                       ORDER BY
                         CASE ISNULL([OPT_TYPE],'')
                             WHEN 'RL'  THEN 1
                             WHEN 'TBC' THEN 2
                             WHEN 'TBL' THEN 3
                             ELSE 4 END,
                         ISNULL([OPT_PRIORITY_TIER], 3) ASC,
                         ISNULL([ST_RANK], 999999) ASC,
                         ISNULL(TRY_CAST([SEC_CT%] AS FLOAT), 0) DESC,
                         ISNULL([MAX_DAILY_SALE], 0) DESC,
                         ISNULL([OPT_REQ_WH], 0) DESC
                   ) AS rk
            FROM [{working_table}]
            WHERE LISTED_FLAG = 1
        )
        UPDATE W SET W.OPT_PRIORITY_RANK = R.rk
        FROM [{working_table}] W
        INNER JOIN R
            ON W.WERKS=R.WERKS AND W.MAJ_CAT=R.MAJ_CAT
           AND W.GEN_ART_NUMBER=R.GEN_ART_NUMBER
           AND ISNULL(W.CLR,'') = ISNULL(R.CLR,'')
    """)


def _stage_a_materialize_listed(conn, working_table, listed_table) -> int:
    _run(conn, f"IF OBJECT_ID('{listed_table}','U') IS NOT NULL DROP TABLE [{listed_table}]")
    _run(conn, f"""
        SELECT
            WERKS, RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, GEN_ART_DESC,
            OPT_TYPE, ISNULL(IS_NEW,0) AS IS_NEW, ISNULL(I_ROD,1) AS I_ROD,
            ISNULL(OPT_MBQ,0) AS OPT_MBQ, ISNULL(OPT_REQ,0) AS OPT_REQ,
            ISNULL(OPT_MBQ_WH, OPT_MBQ) AS OPT_MBQ_WH,
            ISNULL(OPT_REQ_WH, OPT_REQ) AS OPT_REQ_WH,
            ISNULL(MSA_FNL_Q,0) AS MSA_FNL_Q,
            ISNULL(VAR_COUNT,0) AS VAR_COUNT,
            ISNULL(VAR_FNL_COUNT,0) AS VAR_FNL_COUNT,
            ISNULL(STK_TTL,0) AS STK_TTL,
            ISNULL(ACS_D,0) AS ACS_D, ISNULL(AGE,0) AS AGE,
            ISNULL(MAX_DAILY_SALE,0) AS MAX_DAILY_SALE,
            LISTING, [PRI_CT%], [SEC_CT%], ALLOC_FLAG,
            FOCUS_W_CAP, FOCUS_WO_CAP,
            ST_RANK, OPT_PRIORITY_RANK, OPT_PRIORITY_TIER,
            LISTED_FLAG, LISTED_REASON
        INTO [{listed_table}]
        FROM [{working_table}]
        WHERE LISTED_FLAG = 1
    """)
    cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{listed_table}]")).scalar()
    return int(cnt or 0)


# ───────────────────────────────────────────────────────────────
# STAGE B — EXPLODE TO VAR_ART × SZ
# ───────────────────────────────────────────────────────────────
def _stage_b_explode(conn, listed_table, alloc_table, msa_var_table) -> int:
    _run(conn, f"IF OBJECT_ID('{alloc_table}','U') IS NOT NULL DROP TABLE [{alloc_table}]")
    _run(conn, f"""
        SELECT
            L.WERKS, L.RDC, L.MAJ_CAT, L.GEN_ART_NUMBER, L.CLR, L.GEN_ART_DESC,
            V.[ARTICLE_NUMBER] AS VAR_ART,
            V.[ARTICLE_DESC]   AS VAR_DESC,
            V.[SZ], V.[MRP], V.[PAK_SZ],

            L.OPT_TYPE, L.IS_NEW, L.I_ROD,
            L.OPT_PRIORITY_RANK, L.OPT_PRIORITY_TIER, L.ST_RANK,
            L.OPT_MBQ, L.OPT_MBQ_WH, L.OPT_REQ, L.OPT_REQ_WH,
            L.MAX_DAILY_SALE, L.ALLOC_FLAG,
            L.[PRI_CT%], L.[SEC_CT%],

            TRY_CAST(V.[FNL_Q] AS FLOAT) AS FNL_Q,
            TRY_CAST(V.[FNL_Q] AS FLOAT) AS FNL_Q_REM,
            CAST(NULL AS FLOAT) AS CONT,
            CAST(NULL AS FLOAT) AS SZ_MBQ,
            CAST(NULL AS FLOAT) AS SZ_MBQ_WH,
            CAST(0 AS FLOAT)    AS SZ_STK,
            CAST(NULL AS FLOAT) AS SZ_REQ,
            CAST(NULL AS FLOAT) AS SZ_REQ_WH,

            CAST(0 AS FLOAT) AS POOL_CONSUMED,
            CAST(0 AS FLOAT) AS SHIP_QTY,
            CAST(0 AS FLOAT) AS HOLD_QTY,
            CAST(0 AS FLOAT) AS ALLOC_QTY,
            CAST(0 AS FLOAT) AS ROUND_SHIP,
            CAST(0 AS FLOAT) AS ROUND_HOLD,
            CAST(NULL AS NVARCHAR(20))  AS ALLOC_WAVE,
            CAST(0 AS INT)              AS ALLOC_ROUND,
            CAST('PENDING' AS NVARCHAR(50)) AS ALLOC_STATUS,
            CAST(NULL AS NVARCHAR(500)) AS SKIP_REASON
        INTO [{alloc_table}]
        FROM [{listed_table}] L
        INNER JOIN [{msa_var_table}] V WITH (NOLOCK)
            ON  LTRIM(RTRIM(CAST(L.MAJ_CAT AS NVARCHAR(200))))
               = LTRIM(RTRIM(CAST(V.[MAJ_CAT] AS NVARCHAR(200))))
            AND TRY_CAST(L.GEN_ART_NUMBER AS BIGINT)
               = TRY_CAST(TRY_CAST(V.[GEN_ART_NUMBER] AS FLOAT) AS BIGINT)
            AND LTRIM(RTRIM(CAST(L.CLR AS NVARCHAR(200))))
               = LTRIM(RTRIM(CAST(V.[CLR] AS NVARCHAR(200))))
            AND LTRIM(RTRIM(CAST(L.RDC AS NVARCHAR(50))))
               = LTRIM(RTRIM(CAST(V.[RDC] AS NVARCHAR(50))))
        WHERE TRY_CAST(V.[FNL_Q] AS FLOAT) > 0
    """)
    cnt = conn.execute(text(f"SELECT COUNT(*) FROM [{alloc_table}]")).scalar()
    return int(cnt or 0)


def _stage_b_fill_cont(conn, alloc_table, cont_table):
    if _exists(conn, cont_table):
        _run(conn, f"""
            UPDATE A SET A.CONT = TRY_CAST(M.CONT AS FLOAT)
            FROM [{alloc_table}] A
            INNER JOIN [{cont_table}] M WITH (NOLOCK)
                ON LTRIM(RTRIM(CAST(M.ST_CD   AS NVARCHAR(50))))  = LTRIM(RTRIM(CAST(A.WERKS AS NVARCHAR(50))))
               AND LTRIM(RTRIM(CAST(M.MAJ_CAT AS NVARCHAR(200)))) = A.MAJ_CAT
               AND LTRIM(RTRIM(CAST(M.SZ      AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.SZ AS NVARCHAR(200))))
        """)
        _run(conn, f"""
            UPDATE A SET A.CONT = TRY_CAST(M.CONT AS FLOAT)
            FROM [{alloc_table}] A
            INNER JOIN [{cont_table}] M WITH (NOLOCK)
                ON LTRIM(RTRIM(CAST(M.ST_CD AS NVARCHAR(50)))) = 'CO'
               AND LTRIM(RTRIM(CAST(M.MAJ_CAT AS NVARCHAR(200)))) = A.MAJ_CAT
               AND LTRIM(RTRIM(CAST(M.SZ AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.SZ AS NVARCHAR(200))))
            WHERE A.CONT IS NULL
        """)
    # Uniform fallback
    _run(conn, f"""
        ;WITH SzCount AS (
            SELECT WERKS, MAJ_CAT, COUNT(DISTINCT SZ) AS sz_cnt
            FROM [{alloc_table}] GROUP BY WERKS, MAJ_CAT
        )
        UPDATE A SET A.CONT = ROUND(1.0 / NULLIF(C.sz_cnt, 0), 4)
        FROM [{alloc_table}] A
        INNER JOIN SzCount C ON A.WERKS = C.WERKS AND A.MAJ_CAT = C.MAJ_CAT
        WHERE ISNULL(A.CONT, 0) = 0
    """)


def _stage_b_fill_targets(conn, alloc_table, var_grid_table):
    # Optional: pull per-variant-size stock from variant grid, if present.
    if _exists(conn, var_grid_table):
        gcols = {c.upper() for c in _cols(conn, var_grid_table)}
        if {"STK_TTL", "WERKS", "MAJ_CAT"}.issubset(gcols):
            # Best-effort join — silently skip if variant key column isn't obvious.
            var_col = next((c for c in ("VAR_ART", "ARTICLE_NUMBER", "GEN_ART") if c in gcols), None)
            if var_col:
                _run(conn, f"""
                    UPDATE A SET A.SZ_STK = TRY_CAST(G.STK_TTL AS FLOAT)
                    FROM [{alloc_table}] A
                    INNER JOIN [{var_grid_table}] G WITH (NOLOCK)
                        ON G.WERKS = A.WERKS
                       AND G.MAJ_CAT = A.MAJ_CAT
                       AND TRY_CAST(G.[{var_col}] AS BIGINT) = TRY_CAST(A.VAR_ART AS BIGINT)
                """)

    _run(conn, f"""
        UPDATE [{alloc_table}] SET
            SZ_MBQ    = ROUND(ISNULL(OPT_MBQ,    0) * ISNULL(CONT, 0), 0),
            SZ_MBQ_WH = ROUND(ISNULL(OPT_MBQ_WH, 0) * ISNULL(CONT, 0), 0),
            SZ_STK    = ISNULL(SZ_STK, 0)
    """)
    _run(conn, f"""
        UPDATE [{alloc_table}] SET
            SZ_REQ    = CASE WHEN SZ_MBQ    - ISNULL(SZ_STK,0) > 0 THEN SZ_MBQ    - ISNULL(SZ_STK,0) ELSE 0 END,
            SZ_REQ_WH = CASE WHEN SZ_MBQ_WH - ISNULL(SZ_STK,0) > 0 THEN SZ_MBQ_WH - ISNULL(SZ_STK,0) ELSE 0 END
    """)


def _stage_b_indexes(conn, alloc_table):
    try:
        _run(conn, f"""
            CREATE CLUSTERED INDEX CIX_{alloc_table}_walk ON [{alloc_table}]
              (OPT_TYPE, OPT_PRIORITY_RANK, WERKS, MAJ_CAT,
               GEN_ART_NUMBER, CLR, VAR_ART, SZ)
        """)
    except Exception:
        pass
    try:
        _run(conn, f"""
            CREATE NONCLUSTERED INDEX IX_{alloc_table}_pool ON [{alloc_table}]
              (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
              INCLUDE (WERKS, SHIP_QTY, HOLD_QTY, FNL_Q_REM)
        """)
    except Exception:
        pass


# ───────────────────────────────────────────────────────────────
# PRIMARY-GRID DISCOVERY + REM SHADOW COLUMNS
# ───────────────────────────────────────────────────────────────
def _discover_primary_grids(conn) -> Dict[str, Dict]:
    """
    Returns: {REQ_COL: {hier, gh_col, h_col, h_rem, req_rem, extras}}
      extras = hier columns excluding WERKS and MAJ_CAT (the grid's inner grain)

    Always includes MJ_REQ (primary grid at WERKS×MAJ_CAT grain).
    Reads ARS_GRID_BUILDER for additional ACTIVE Primary grids.
    """
    out: Dict[str, Dict] = {
        "MJ_REQ": {
            "hier": ["MAJ_CAT"],
            "gh_col": "GH_MJ",
            "h_col":  "H_MJ",
            "h_rem":  "H_MJ_REM",
            "req_rem": "MJ_REQ_REM",
            "extras": [],
        }
    }
    if not _exists(conn, "ARS_GRID_BUILDER"):
        return out
    try:
        rows = conn.execute(text(
            "SELECT grid_name, hierarchy_columns, ISNULL(grid_group,'Primary') "
            "FROM [ARS_GRID_BUILDER] WHERE UPPER(status) = 'ACTIVE'"
        )).fetchall()
    except Exception as e:
        logger.warning(f"_discover_primary_grids: {e}")
        return out
    for grid_name, hier_json, grid_group in rows:
        if str(grid_group).strip().lower() != "primary":
            continue
        try:
            hier = json.loads(hier_json) if isinstance(hier_json, str) else hier_json
        except Exception:
            continue
        if not hier or any(str(x).upper() in _SKIP_ART for x in hier):
            continue
        hier_u = [str(h).upper() for h in hier]
        last = hier_u[-1]
        if last in ("WERKS", "MAJ_CAT"):
            continue  # covered by MJ
        extras = [h for h in hier_u if h not in ("WERKS", "MAJ_CAT")]
        out[f"{last}_REQ"] = {
            "hier": hier_u,
            "gh_col": f"GH_{last}",
            "h_col":  f"H_{last}",
            "h_rem":  f"H_{last}_REM",
            "req_rem": f"{last}_REQ_REM",
            "extras": extras,
        }
    return out


def _init_rem_columns(conn, working_table, grids: Dict[str, Dict]):
    """
    Create / seed _REM shadow columns on working_table:
      MSA_FNL_Q_REM, PRI_CT_REM, <grid>_REQ_REM, H_<grid>_REM
    Seeds from the originals so each run starts fresh.
    """
    cols = {c.upper() for c in _cols(conn, working_table)}

    def _ensure(col, typedef):
        if col.upper() not in cols:
            try:
                _run(conn, f"ALTER TABLE [{working_table}] ADD [{col}] {typedef}")
            except Exception:
                pass

    _ensure("MSA_FNL_Q_REM", "FLOAT NULL")
    _ensure("PRI_CT_REM",    "FLOAT NULL")

    _run(conn, f"""
        UPDATE [{working_table}] SET
            MSA_FNL_Q_REM = TRY_CAST(MSA_FNL_Q AS FLOAT),
            PRI_CT_REM    = TRY_CAST([PRI_CT%] AS FLOAT)
    """)

    # Re-read cols after potential adds
    cols = {c.upper() for c in _cols(conn, working_table)}

    for req_col, meta in grids.items():
        req_rem = meta["req_rem"]
        h_col   = meta["h_col"]
        h_rem   = meta["h_rem"]

        if req_col.upper() in cols:
            if req_rem.upper() not in cols:
                try:
                    _run(conn, f"ALTER TABLE [{working_table}] ADD [{req_rem}] FLOAT NULL")
                except Exception:
                    pass
            _run(conn, f"UPDATE [{working_table}] SET [{req_rem}] = TRY_CAST([{req_col}] AS FLOAT)")

        if h_col.upper() in cols:
            if h_rem.upper() not in cols:
                try:
                    _run(conn, f"ALTER TABLE [{working_table}] ADD [{h_rem}] INT NULL")
                except Exception:
                    pass
            _run(conn, f"UPDATE [{working_table}] SET [{h_rem}] = TRY_CAST([{h_col}] AS INT)")


def _revalidate_after_band(conn, working_table, alloc_table, opt_type,
                            band_start, band_end, grids: Dict[str, Dict],
                            pri_ct_check_rl: bool = True,
                            pri_ct_check_tbc: bool = True):
    """
    After one band allocates (BAND_SIZE=1 → one rank/OPT):
      1) Reduce MSA_FNL_Q_REM per OPT by ROUND_SHIP + ROUND_HOLD.
      2) Reduce each <grid>_REQ_REM at the grid's grain by ROUND_SHIP
         (joined via working_table to pick up extras like RNG_SEG/MACRO_MVGR).
      3) Recompute H_<grid>_REM = 1 iff REQ_REM > ACS_SKIP_FACTOR*ACS_D AND GH = 1.
      4) Recompute PRI_CT_REM = Σ(H_REM) / Σ(GH) * 100.
      5) Skip rules (apply to rows with OPT_PRIORITY_RANK > band_end):
         - MSA_FNL_Q_REM <= 0                  → SKIPPED (SKIP_MSA_EXHAUSTED)
         - PRI_CT_REM   < 100                  → SKIPPED (SKIP_PRI_BROKEN)
         - MJ_REQ_REM   < factor*ACS_D  (store skip within opt_type, if enabled)
      6) Push skip status to alloc_table so future bands exclude them.
    """
    params = {"ot": opt_type, "bs": band_start, "be": band_end}

    # Early-exit: if this band allocated nothing, no _REM values changed
    # (so no skip rules can newly trigger) — skip the 9 revalidation UPDATEs.
    # This cuts the common case where pool is already exhausted for the OPT.
    band_take = conn.execute(text(f"""
        SELECT ISNULL(SUM(ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0)), 0)
        FROM [{alloc_table}]
        WHERE OPT_TYPE = :ot
          AND OPT_PRIORITY_RANK BETWEEN :bs AND :be
    """), params).scalar()
    if float(band_take or 0) <= 0:
        return

    # Scope: only the MAJ_CATs touched by this band need H_REM / PRI_CT_REM /
    # skip-rule recomputation. For BAND_SIZE=1 this is usually one MAJ_CAT.
    # Scoping turns full-table UPDATEs (10k+ rows) into narrow ones (<1k).
    mc_rows = conn.execute(text(f"""
        SELECT DISTINCT MAJ_CAT FROM [{alloc_table}]
        WHERE OPT_TYPE = :ot
          AND OPT_PRIORITY_RANK BETWEEN :bs AND :be
          AND ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0) > 0
    """), params).fetchall()
    touched = [r[0] for r in mc_rows if r[0] is not None]
    if not touched:
        return
    mc_keys = {f"mc_{i}": mc for i, mc in enumerate(touched)}
    mc_in = ", ".join(f":mc_{i}" for i in range(len(touched)))
    params_mc = {**params, **mc_keys}

    work_cols = {c.upper() for c in _cols(conn, working_table)}
    alloc_cols = {c.upper() for c in _cols(conn, alloc_table)}

    # (1) Reduce MSA_FNL_Q_REM per OPT — this band's SHIP+HOLD
    _run(conn, f"""
        ;WITH OptTake AS (
            SELECT WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR,
                   SUM(ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0)) AS take_total,
                   SUM(ISNULL(ROUND_SHIP,0)) AS take_ship
            FROM [{alloc_table}]
            WHERE OPT_TYPE = :ot
              AND OPT_PRIORITY_RANK BETWEEN :bs AND :be
            GROUP BY WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR
            HAVING SUM(ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0)) > 0
        )
        UPDATE W SET
            W.MSA_FNL_Q_REM = CASE
                WHEN ISNULL(W.MSA_FNL_Q_REM, 0) - O.take_total < 0 THEN 0
                ELSE ISNULL(W.MSA_FNL_Q_REM, 0) - O.take_total END
        FROM [{working_table}] W
        INNER JOIN OptTake O
            ON W.WERKS=O.WERKS AND W.MAJ_CAT=O.MAJ_CAT
           AND W.GEN_ART_NUMBER=O.GEN_ART_NUMBER
           AND ISNULL(W.CLR,'') = ISNULL(O.CLR,'')
    """, params)

    # (2) Reduce each primary grid's _REQ_REM at its grain
    for req_col, meta in grids.items():
        req_rem = meta["req_rem"]
        extras  = meta["extras"]
        if req_rem.upper() not in work_cols:
            continue
        # Grid grain = (WERKS, MAJ_CAT, *extras) — all must exist on working_table
        if not all(e.upper() in work_cols for e in extras):
            continue
        grid_keys = ["WERKS", "MAJ_CAT"] + extras
        key_sql  = ", ".join(f"W2.[{k}]" for k in grid_keys)
        group_by = ", ".join(f"W2.[{k}]" for k in grid_keys)
        join_cond = " AND ".join(
            f"ISNULL(CAST(W.[{k}] AS NVARCHAR(200)),'') = ISNULL(CAST(G.[{k}] AS NVARCHAR(200)),'')"
            for k in grid_keys
        )
        _run(conn, f"""
            ;WITH GridTake AS (
                SELECT {key_sql},
                       SUM(ISNULL(A.ROUND_SHIP,0)) AS grid_take
                FROM [{alloc_table}] A
                INNER JOIN [{working_table}] W2
                    ON A.WERKS=W2.WERKS AND A.MAJ_CAT=W2.MAJ_CAT
                   AND A.GEN_ART_NUMBER=W2.GEN_ART_NUMBER
                   AND ISNULL(A.CLR,'') = ISNULL(W2.CLR,'')
                WHERE A.OPT_TYPE = :ot
                  AND A.OPT_PRIORITY_RANK BETWEEN :bs AND :be
                GROUP BY {group_by}
                HAVING SUM(ISNULL(A.ROUND_SHIP,0)) > 0
            )
            UPDATE W SET
                W.[{req_rem}] = CASE
                    WHEN ISNULL(W.[{req_rem}], 0) - G.grid_take < 0 THEN 0
                    ELSE ISNULL(W.[{req_rem}], 0) - G.grid_take END
            FROM [{working_table}] W
            INNER JOIN GridTake G ON {join_cond}
        """, params)

    # (3) Recompute H_<grid>_REM = (REQ_REM > ACS_SKIP_FACTOR*ACS_D) AND (GH=1)
    h_rem_sets = []
    for req_col, meta in grids.items():
        req_rem = meta["req_rem"]
        gh_col  = meta["gh_col"]
        h_rem   = meta["h_rem"]
        if h_rem.upper() not in work_cols or req_rem.upper() not in work_cols:
            continue
        if gh_col.upper() not in work_cols:
            continue
        h_rem_sets.append(
            f"[{h_rem}] = CASE "
            f"WHEN ISNULL([{req_rem}],0) > {ACS_SKIP_FACTOR} * ISNULL(ACS_D,0) "
            f"AND ISNULL([{gh_col}],0) = 1 THEN 1 ELSE 0 END"
        )
    if h_rem_sets:
        # Scope to touched MAJ_CATs (a grid-grain REQ_REM never crosses
        # MAJ_CAT, so H_REM only needs recompute for those rows).
        _run(conn, f"""
            UPDATE [{working_table}] SET {', '.join(h_rem_sets)}
            WHERE MAJ_CAT IN ({mc_in})
        """, params_mc)

    # (4) Recompute PRI_CT_REM = Σ(H_REM) / Σ(GH) × 100 — scoped too.
    pri_h = [meta["h_rem"] for meta in grids.values() if meta["h_rem"].upper() in work_cols]
    pri_gh = [meta["gh_col"] for meta in grids.values() if meta["gh_col"].upper() in work_cols]
    if pri_h and pri_gh:
        h_sum  = " + ".join(f"ISNULL([{c}],0)" for c in pri_h)
        gh_sum = " + ".join(f"ISNULL([{c}],0)" for c in pri_gh)
        _run(conn, f"""
            UPDATE [{working_table}] SET
                PRI_CT_REM = CASE
                    WHEN ({gh_sum}) = 0 THEN 0
                    ELSE ROUND(CAST(({h_sum}) AS FLOAT) / ({gh_sum}) * 100, 1) END
            WHERE MAJ_CAT IN ({mc_in})
        """, params_mc)

    # (5) Skip-rules on remaining PENDING/PARTIAL OPTs
    #   MSA_EXHAUSTED applies to all opt_types.
    #   PRI_BROKEN scope mirrors the Stage A gate — TBL always enforces;
    #   RL / TBC enforce only when their flag is True.
    enforced = ["'TBL'"]
    if pri_ct_check_rl:  enforced.append("'RL'")
    if pri_ct_check_tbc: enforced.append("'TBC'")
    pri_opt_in = ", ".join(enforced)
    _run(conn, f"""
        UPDATE [{working_table}] SET
            ALLOC_STATUS = CASE
                WHEN ISNULL(MSA_FNL_Q_REM, 0) <= 0 THEN 'SKIPPED'
                WHEN ISNULL(PRI_CT_REM, 0)    < 100
                     AND ISNULL(OPT_TYPE,'') IN ({pri_opt_in}) THEN 'SKIPPED'
                ELSE ALLOC_STATUS END,
            ALLOC_REMARKS = CASE
                WHEN ISNULL(MSA_FNL_Q_REM, 0) <= 0
                    THEN ISNULL(ALLOC_REMARKS,'') + ' SKIP_MSA_EXHAUSTED;'
                WHEN ISNULL(PRI_CT_REM, 0)    < 100
                     AND ISNULL(OPT_TYPE,'') IN ({pri_opt_in})
                    THEN ISNULL(ALLOC_REMARKS,'') + ' SKIP_PRI_BROKEN;'
                ELSE ALLOC_REMARKS END
        WHERE LISTED_FLAG = 1
          AND MAJ_CAT IN ({mc_in})
          AND ISNULL(ALLOC_STATUS,'PENDING') NOT IN ('SKIPPED','ALLOCATED')
          AND OPT_PRIORITY_RANK > :be
    """, params_mc)

    # Store-broken: MJ_REQ_REM < factor × ACS_D → skip rest of store for this opt_type
    if ENABLE_STORE_BROKEN and "MJ_REQ_REM" in work_cols:
        _run(conn, f"""
            UPDATE [{working_table}] SET
                ALLOC_STATUS = 'SKIPPED',
                ALLOC_REMARKS = ISNULL(ALLOC_REMARKS,'') + ' SKIP_STORE_BROKEN;'
            WHERE LISTED_FLAG = 1
              AND MAJ_CAT IN ({mc_in})
              AND ISNULL(ALLOC_STATUS,'PENDING') NOT IN ('SKIPPED','ALLOCATED')
              AND OPT_TYPE = :ot
              AND OPT_PRIORITY_RANK > :be
              AND ISNULL(MJ_REQ_REM, 0) < {ACS_SKIP_FACTOR} * ISNULL(ACS_D, 0)
        """, params_mc)

    # (6) Propagate SKIP to alloc_table so future bands' Target CTE excludes them
    _run(conn, f"""
        UPDATE A SET
            A.ALLOC_STATUS = 'SKIPPED',
            A.SKIP_REASON  = CASE
                WHEN A.SKIP_REASON IS NULL OR A.SKIP_REASON = ''
                    THEN 'REVALIDATION_SKIP'
                ELSE A.SKIP_REASON END
        FROM [{alloc_table}] A
        INNER JOIN [{working_table}] W
            ON A.WERKS=W.WERKS AND A.MAJ_CAT=W.MAJ_CAT
           AND A.GEN_ART_NUMBER=W.GEN_ART_NUMBER
           AND ISNULL(A.CLR,'') = ISNULL(W.CLR,'')
        WHERE W.ALLOC_STATUS = 'SKIPPED'
          AND W.MAJ_CAT IN ({mc_in})
          AND A.MAJ_CAT IN ({mc_in})
          AND ISNULL(A.ALLOC_STATUS,'PENDING') NOT IN ('SKIPPED','ALLOCATED','PARTIAL')
    """, params_mc)


# ───────────────────────────────────────────────────────────────
# STAGE C — ALLOCATE (pool waterfall)
# ───────────────────────────────────────────────────────────────
def _stage_c_build_pool(conn, alloc_table):
    _run(conn, f"IF OBJECT_ID('tempdb..{POOL_TABLE}') IS NOT NULL DROP TABLE {POOL_TABLE}")
    _run(conn, f"""
        SELECT RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
               MAX(ISNULL(FNL_Q,0)) AS FNL_Q_ORIG,
               MAX(ISNULL(FNL_Q,0)) AS FNL_Q_REM
        INTO {POOL_TABLE}
        FROM [{alloc_table}]
        GROUP BY RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ
    """)
    try:
        _run(conn, f"""
            CREATE UNIQUE CLUSTERED INDEX IX_pool_key ON {POOL_TABLE}
              (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
        """)
    except Exception:
        pass


def _stage_c_waterfall(conn, alloc_table, working_table=None, grids=None,
                        pri_ct_check_rl: bool = True,
                        pri_ct_check_tbc: bool = True):
    """
    For each (OPT_TYPE, round r, rank band) — run one batch SQL that:
      1) computes need_pool / need_ship per eligible row,
      2) orders rows inside each pool key by (rank, ST_RANK),
      3) takes a cumulative window to see what fraction of FNL_Q_REM each
         row can claim without overdraft,
      4) commits SHIP_QTY / HOLD_QTY, decrements the pool,
      5) if ENABLE_PER_OPT_REVALIDATION + working_table + grids are provided,
         revalidates grid REQs / PRI_CT_REM / MSA_FNL_Q_REM on working_table
         and propagates SKIP to alloc_table before the next band.
    """
    # per-opt_type bounds: scan only the rank range that actually belongs
    # to this opt_type (ranks are global; TBL's ranks start after RL+TBC).
    for ot in OPT_TYPE_ORDER:
        bounds = conn.execute(text(f"""
            SELECT ISNULL(MAX(I_ROD), 0),
                   ISNULL(MIN(OPT_PRIORITY_RANK), 0),
                   ISNULL(MAX(OPT_PRIORITY_RANK), 0)
            FROM [{alloc_table}] WHERE OPT_TYPE = :ot
        """), {"ot": ot}).fetchone()
        max_round = int(bounds[0] or 0)
        min_rank  = int(bounds[1] or 0)
        max_rank  = int(bounds[2] or 0)
        if max_round == 0 or max_rank == 0 or min_rank == 0:
            continue
        logger.info(f"[C] opt_type={ot} rounds={max_round} ranks={min_rank}..{max_rank}")
        ot_start = time.time()

        for r in range(1, max_round + 1):
            # Tighten rank window to rows whose I_ROD allows this round.
            rb = conn.execute(text(f"""
                SELECT ISNULL(MIN(OPT_PRIORITY_RANK), 0),
                       ISNULL(MAX(OPT_PRIORITY_RANK), 0)
                FROM [{alloc_table}]
                WHERE OPT_TYPE = :ot AND ISNULL(I_ROD, 1) >= :r
            """), {"ot": ot, "r": r}).fetchone()
            r_min = int(rb[0] or 0)
            r_max = int(rb[1] or 0)
            if r_min == 0 or r_max == 0:
                continue
            # Reset per-round deltas for the whole opt_type once.
            _run(conn, f"""
                UPDATE [{alloc_table}] SET ROUND_SHIP = 0, ROUND_HOLD = 0
                WHERE OPT_TYPE = :ot
            """, {"ot": ot})
            band_start = r_min
            while band_start <= r_max:
                band_end = band_start + BAND_SIZE - 1
                _stage_c_run_band(conn, alloc_table, ot, r, band_start, band_end)
                if ENABLE_PER_OPT_REVALIDATION and working_table and grids:
                    _revalidate_after_band(
                        conn, working_table, alloc_table,
                        ot, band_start, band_end, grids,
                        pri_ct_check_rl=pri_ct_check_rl,
                        pri_ct_check_tbc=pri_ct_check_tbc,
                    )
                band_start = band_end + 1

        s = conn.execute(text(f"""
            SELECT COUNT(*),
                   ISNULL(SUM(SHIP_QTY),0), ISNULL(SUM(HOLD_QTY),0),
                   SUM(CASE WHEN SHIP_QTY>0 OR HOLD_QTY>0 THEN 1 ELSE 0 END)
            FROM [{alloc_table}] WHERE OPT_TYPE = :ot
        """), {"ot": ot}).fetchone()
        logger.info(
            f"[C] {ot} done in {round(time.time()-ot_start,1)}s — "
            f"rows={s[0]}, ship={float(s[1] or 0):.0f}, "
            f"hold={float(s[2] or 0):.0f}, filled_rows={s[3]}"
        )

    # Finalise: copy SHIP_QTY to ALLOC_QTY
    _run(conn, f"UPDATE [{alloc_table}] SET ALLOC_QTY = SHIP_QTY")
    # Classify final status, using the SZ_STK-aware lifetime target.
    _run(conn, f"""
        UPDATE [{alloc_table}] SET
            ALLOC_STATUS = CASE
                WHEN SHIP_QTY + HOLD_QTY > 0
                     AND SHIP_QTY + HOLD_QTY
                         >= CASE WHEN ISNULL(SZ_MBQ_WH,0) * ISNULL(I_ROD,1)
                                      - ISNULL(SZ_STK,0) > 0
                                 THEN ISNULL(SZ_MBQ_WH,0) * ISNULL(I_ROD,1)
                                      - ISNULL(SZ_STK,0)
                                 ELSE 0 END
                     THEN 'ALLOCATED'
                WHEN SHIP_QTY > 0                 THEN 'PARTIAL'
                ELSE 'SKIPPED' END,
            SKIP_REASON = CASE
                WHEN SHIP_QTY = 0 AND HOLD_QTY = 0
                     AND ISNULL(SZ_MBQ_WH,0) * ISNULL(I_ROD,1) - ISNULL(SZ_STK,0) <= 0
                     THEN 'ALREADY_STOCKED'
                WHEN SHIP_QTY = 0 AND HOLD_QTY = 0 THEN 'NO_POOL_OR_DEMAND'
                ELSE SKIP_REASON END
    """)


def _stage_c_run_band(conn, alloc_table, opt_type, r, band_start, band_end):
    """
    One rank-band × one round × one opt_type.

    Two-statement sequence (ROUND_SHIP/ROUND_HOLD are pre-zeroed per round
    by the outer waterfall loop, so no per-band reset is needed):

      Step 1 — cumulative-window UPDATE: take pool by priority × ST_RANK.
      Step 2 — decrement #nre_pool by ROUND_SHIP + ROUND_HOLD for this band.
    """
    params = {"ot": opt_type, "bs": band_start, "be": band_end, "r": r}

    # Step 1 — compute take_pool per row, write SHIP_QTY / HOLD_QTY deltas.
    _run(conn, f"""
        ;WITH Target AS (
            SELECT A.WERKS, A.RDC, A.MAJ_CAT, A.GEN_ART_NUMBER, A.CLR,
                   A.VAR_ART, A.SZ,
                   A.OPT_PRIORITY_RANK, A.ST_RANK, A.IS_NEW,
                   /* SZ_STK (store's current stock at this size) is subtracted
                      once from the round's target. A size that is already
                      over-stocked (SZ_STK >= r * SZ_MBQ_WH) therefore has
                      need_pool = 0 and takes nothing from the pool. */
                   CASE WHEN :r * ISNULL(A.SZ_MBQ_WH,0) - ISNULL(A.SZ_STK,0)
                           > ISNULL(A.POOL_CONSUMED,0)
                        THEN :r * ISNULL(A.SZ_MBQ_WH,0) - ISNULL(A.SZ_STK,0)
                           - ISNULL(A.POOL_CONSUMED,0)
                        ELSE 0 END AS need_pool,
                   CASE WHEN :r * ISNULL(A.SZ_MBQ,0) - ISNULL(A.SZ_STK,0)
                           > ISNULL(A.SHIP_QTY,0)
                        THEN :r * ISNULL(A.SZ_MBQ,0) - ISNULL(A.SZ_STK,0)
                           - ISNULL(A.SHIP_QTY,0)
                        ELSE 0 END AS need_ship
            FROM [{alloc_table}] A
            WHERE A.OPT_TYPE = :ot
              AND A.OPT_PRIORITY_RANK BETWEEN :bs AND :be
              AND ISNULL(A.ALLOC_STATUS,'PENDING') NOT IN ('SKIPPED','INELIGIBLE')
              /* Per-row I_ROD gate: a row with I_ROD=1 must not
                 compete in round 2. */
              AND ISNULL(A.I_ROD, 1) >= :r
        ),
        Ranked AS (
            SELECT T.*, P.FNL_Q_REM,
                   ROW_NUMBER() OVER (
                       PARTITION BY T.RDC, T.MAJ_CAT, T.GEN_ART_NUMBER, T.CLR, T.VAR_ART, T.SZ
                       ORDER BY T.OPT_PRIORITY_RANK ASC, ISNULL(T.ST_RANK,999999) ASC
                   ) AS ord
            FROM Target T
            INNER JOIN {POOL_TABLE} P
                ON P.RDC = T.RDC AND P.MAJ_CAT = T.MAJ_CAT
               AND P.GEN_ART_NUMBER = T.GEN_ART_NUMBER
               AND ISNULL(P.CLR,'') = ISNULL(T.CLR,'')
               AND P.VAR_ART = T.VAR_ART AND P.SZ = T.SZ
            WHERE T.need_pool > 0 AND P.FNL_Q_REM > 0
        ),
        Cum AS (
            SELECT *,
                   SUM(need_pool) OVER (
                       PARTITION BY RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ
                       ORDER BY ord ROWS UNBOUNDED PRECEDING
                   ) AS cum_demand
            FROM Ranked
        ),
        Take AS (
            SELECT *,
                   CASE
                       WHEN FNL_Q_REM - (cum_demand - need_pool) <= 0 THEN 0
                       WHEN FNL_Q_REM - (cum_demand - need_pool) >= need_pool THEN need_pool
                       ELSE FNL_Q_REM - (cum_demand - need_pool)
                   END AS take_pool
            FROM Cum
        )
        UPDATE A SET
            A.POOL_CONSUMED = ISNULL(A.POOL_CONSUMED,0) + X.take_pool,
            A.ROUND_SHIP    = CASE WHEN A.IS_NEW = 1
                                   THEN CASE WHEN X.take_pool < X.need_ship
                                             THEN X.take_pool ELSE X.need_ship END
                                   ELSE X.take_pool END,
            A.ROUND_HOLD    = CASE WHEN A.IS_NEW = 1
                                   THEN X.take_pool - CASE WHEN X.take_pool < X.need_ship
                                                           THEN X.take_pool ELSE X.need_ship END
                                   ELSE 0 END,
            A.SHIP_QTY      = ISNULL(A.SHIP_QTY,0) +
                              CASE WHEN A.IS_NEW = 1
                                   THEN CASE WHEN X.take_pool < X.need_ship
                                             THEN X.take_pool ELSE X.need_ship END
                                   ELSE X.take_pool END,
            A.HOLD_QTY      = ISNULL(A.HOLD_QTY,0) +
                              CASE WHEN A.IS_NEW = 1
                                   THEN X.take_pool - CASE WHEN X.take_pool < X.need_ship
                                                           THEN X.take_pool ELSE X.need_ship END
                                   ELSE 0 END,
            A.ALLOC_WAVE    = CONCAT(:ot, '_R', :r),
            A.ALLOC_ROUND   = :r,
            A.ALLOC_STATUS  = CASE
                /* Row is fully ALLOCATED when POOL_CONSUMED reaches the
                   lifetime net target: I_ROD * SZ_MBQ_WH - SZ_STK
                   (clamped at 0 for already over-stocked sizes). Earlier
                   the row stays PARTIAL so the next round can top it up. */
                WHEN ISNULL(A.POOL_CONSUMED,0) + X.take_pool
                     >= CASE WHEN ISNULL(A.I_ROD,1) * ISNULL(A.SZ_MBQ_WH,0)
                                  - ISNULL(A.SZ_STK,0) > 0
                             THEN ISNULL(A.I_ROD,1) * ISNULL(A.SZ_MBQ_WH,0)
                                  - ISNULL(A.SZ_STK,0)
                             ELSE 0 END
                     THEN 'ALLOCATED'
                ELSE 'PARTIAL' END
        FROM [{alloc_table}] A
        INNER JOIN Take X
            ON A.WERKS = X.WERKS AND A.RDC = X.RDC
           AND A.MAJ_CAT = X.MAJ_CAT AND A.GEN_ART_NUMBER = X.GEN_ART_NUMBER
           AND ISNULL(A.CLR,'') = ISNULL(X.CLR,'')
           AND A.VAR_ART = X.VAR_ART AND A.SZ = X.SZ
        WHERE X.take_pool > 0
    """, params)

    # Step 2 — decrement pool by this band's total take (ROUND_SHIP+ROUND_HOLD).
    _run(conn, f"""
        ;WITH S AS (
            SELECT RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ,
                   SUM(ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0)) AS taken
            FROM [{alloc_table}]
            WHERE OPT_TYPE = :ot
              AND OPT_PRIORITY_RANK BETWEEN :bs AND :be
              AND ALLOC_ROUND = :r
            GROUP BY RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ
            HAVING SUM(ISNULL(ROUND_SHIP,0) + ISNULL(ROUND_HOLD,0)) > 0
        )
        UPDATE P SET P.FNL_Q_REM = P.FNL_Q_REM - S.taken
        FROM {POOL_TABLE} P
        INNER JOIN S
            ON P.RDC = S.RDC AND P.MAJ_CAT = S.MAJ_CAT
           AND P.GEN_ART_NUMBER = S.GEN_ART_NUMBER
           AND ISNULL(P.CLR,'') = ISNULL(S.CLR,'')
           AND P.VAR_ART = S.VAR_ART AND P.SZ = S.SZ
    """, params)


# ───────────────────────────────────────────────────────────────
# STAGE D — REFLECT & AUDIT
# ───────────────────────────────────────────────────────────────
def _stage_d_reflect(conn, working_table, alloc_table):
    _run(conn, f"""
        ;WITH Agg AS (
            SELECT WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR,
                   SUM(ISNULL(SHIP_QTY,0)) AS ship_q,
                   SUM(ISNULL(HOLD_QTY,0)) AS hold_q,
                   COUNT(*) AS sz_rows,
                   SUM(CASE WHEN SHIP_QTY > 0 THEN 1 ELSE 0 END) AS filled_rows
            FROM [{alloc_table}]
            GROUP BY WERKS, MAJ_CAT, GEN_ART_NUMBER, CLR
        )
        UPDATE W SET
            W.ALLOC_QTY    = A.ship_q,
            W.HOLD_QTY     = A.hold_q,
            W.ALLOC_STATUS = CASE
                WHEN A.ship_q = 0                 THEN 'NOT_ALLOCATED'
                WHEN A.filled_rows < A.sz_rows    THEN 'PARTIAL'
                ELSE 'ALLOCATED' END,
            W.ALLOC_REMARKS = CONCAT(
                'ship=', CAST(A.ship_q AS NVARCHAR(20)),
                '; hold=', CAST(A.hold_q AS NVARCHAR(20)),
                '; sizes=', CAST(A.filled_rows AS NVARCHAR(10)),
                '/', CAST(A.sz_rows AS NVARCHAR(10)))
        FROM [{working_table}] W
        INNER JOIN Agg A
            ON W.WERKS=A.WERKS AND W.MAJ_CAT=A.MAJ_CAT
           AND W.GEN_ART_NUMBER=A.GEN_ART_NUMBER
           AND ISNULL(W.CLR,'') = ISNULL(A.CLR,'')
    """)
    _run(conn, f"""
        UPDATE [{working_table}] SET ALLOC_STATUS = 'INELIGIBLE'
        WHERE LISTED_FLAG = 0
    """)
    _run(conn, f"""
        UPDATE [{working_table}] SET ALLOC_STATUS = 'NOT_ALLOCATED',
               ALLOC_REMARKS = ISNULL(ALLOC_REMARKS,'') + ' no pool'
        WHERE LISTED_FLAG = 1 AND (ALLOC_QTY IS NULL OR ALLOC_QTY = 0)
    """)


def _cleanup(conn):
    _run(conn, f"IF OBJECT_ID('tempdb..{POOL_TABLE}') IS NOT NULL DROP TABLE {POOL_TABLE}")
