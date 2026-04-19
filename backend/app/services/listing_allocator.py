"""
Listing Allocator v2 — Multi-Level Waterfall with Live Eligibility + Status Tracking
=====================================================================================
Replaces single-pass waterfall with:
  - OPT_TYPE priority:  RL → TBC → TBL
  - I_ROD rounds:       round N scales demand (OPT_MBQ × N)
  - Per-OPT eligibility: E1–E7 checks against ARS_LISTING_WORKING before each OPT
  - Post-alloc sync:    deduct MSA_FNL_Q, recalc OPT_REQ_WH in listing_working
  - Status tracking:    ALLOC_STATUS, SKIP_REASON on alloc; ALLOC_REMARKS on working
  - SKIP_FLAG scoped per OPT_TYPE (TBC/TBL still get their turn after RL skip)
  - Pool deduction uses ROUND_ALLOC delta (not cumulative ALLOC_QTY)

OPT = MAJ_CAT × GEN_ART_NUMBER × CLR
Pool = shared FNL_Q per (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)

Eligibility Checks (validated against ARS_LISTING_WORKING before each OPT):
  E1: LISTING = 1              (option is listed)
  E2: ALLOC_FLAG = 1           (primary grid coverage ≥ 100%)
  E3: OPT_TYPE ≠ 'MIX'        (MIX options excluded)
  E4: MSA_FNL_Q > 0            (warehouse has MSA stock)
  E5: OPT_REQ_WH ≥ 1           (store has warehouse demand)
  E6: Pool FNL_Q_REM > 0       (pool not exhausted)
  E7: Size availability ≥ thr  (enough sizes in stock)

Post-Allocation Validation:
  B1: Deduct MSA_FNL_Q in listing_working
  B2: Recalc OPT_REQ_WH in listing_working
  B3: Size availability → SKIP + break store if < threshold
"""
from typing import Dict, Tuple, Optional
from sqlalchemy import text
from loguru import logger
import time

from app.utils.db_helpers import run_sql, table_exists, get_columns, ensure_column

_run = run_sql
_exists = table_exists
_get_cols = get_columns
_ensure = ensure_column

POOL_TABLE = "#alloc_pool"
BREAK_TABLE = "ARS_ALLOC_BREAK_RANKS"
OPT_TYPE_ORDER = ["RL", "TBC", "TBL"]


# ═══════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

def run_multilevel_allocation(
    conn,
    final_table: str,
    alloc_table: str,
    msa_var_table: str = "ARS_MSA_VAR_ART",
    var_grid_table: str = "ARS_GRID_MJ_VAR_ART",
    cont_table: str = "Master_CONT_SZ",
    threshold: float = 0.6,
    enable_fallback: bool = False,
) -> Dict:
    """
    Run multi-level allocation with live eligibility checks and status tracking.
    Returns dict with: alloc_rows, phases, skipped_opts, fallback_levels, duration_sec
    """
    t0 = time.time()
    result = {"alloc_rows": 0, "phases": [], "skipped_opts": 0,
              "fallback_levels": 0, "ineligible_opts": 0}

    if not _exists(conn, final_table) or not _exists(conn, msa_var_table):
        logger.info(f"Skipped allocation: missing {final_table} or {msa_var_table}")
        return result

    # ── Step 1: Create ARS_ALLOC_WORKING ──────────────────────────────
    alloc_rows = _create_alloc_working(conn, final_table, alloc_table, msa_var_table)
    if alloc_rows == 0:
        logger.info("No eligible rows for allocation (ALLOC_FLAG=1 with FNL_Q>0)")
        return result
    logger.info(f"Alloc base: {alloc_rows} rows")

    # ── Step 2: Enrich (STK_TTL, CONT, SZ_MBQ, SZ_REQ) ──────────────
    _enrich_variant_stock(conn, alloc_table, var_grid_table)
    _enrich_size_cont(conn, alloc_table, cont_table)
    _calc_sz_mbq_req(conn, alloc_table)

    # ── Step 3: Add tracking + status columns ─────────────────────────
    _add_tracking_columns(conn, alloc_table, final_table)

    # ── Step 4: Create pool tracker ───────────────────────────────────
    _create_pool(conn, alloc_table)

    # ── Step 5: Mark initial eligibility on listing_working ───────────
    _mark_initial_eligibility(conn, final_table)

    # ── Step 6: Primary allocation (RL → TBC → TBL, I_ROD rounds) ────
    primary_result = _run_primary(conn, alloc_table, final_table, threshold)
    result["phases"].append({"pass": "PRIMARY", **primary_result})

    # ── Step 7: Fallback (optional) ───────────────────────────────────
    if enable_fallback:
        fb_result = _run_fallback(conn, final_table, alloc_table, threshold)
        result["phases"].append({"pass": "FALLBACK", **fb_result})
        result["fallback_levels"] = fb_result.get("levels", 0)

    # ── Step 8: Reflect to working table + final status ───────────────
    _reflect_to_working(conn, final_table, alloc_table)

    # ── Step 9: Cleanup ───────────────────────────────────────────────
    try:
        _run(conn, f"IF OBJECT_ID('tempdb..{POOL_TABLE}') IS NOT NULL DROP TABLE {POOL_TABLE}")
    except Exception:
        pass
    try:
        _run(conn, f"IF OBJECT_ID('{BREAK_TABLE}','U') IS NOT NULL DROP TABLE [{BREAK_TABLE}]")
    except Exception:
        pass

    result["alloc_rows"] = conn.execute(text(
        f"SELECT COUNT(*) FROM [{alloc_table}] WHERE ISNULL([ALLOC_QTY],0) > 0"
    )).scalar() or 0
    result["skipped_opts"] = conn.execute(text(
        f"SELECT COUNT(DISTINCT CAST([MAJ_CAT] AS NVARCHAR(200))+'|'+"
        f"CAST([GEN_ART_NUMBER] AS NVARCHAR(50))+'|'+CAST([CLR] AS NVARCHAR(200))+'|'+"
        f"CAST([OPT_TYPE] AS NVARCHAR(20))) "
        f"FROM [{alloc_table}] WHERE [SKIP_FLAG]=1"
    )).scalar() or 0
    result["ineligible_opts"] = conn.execute(text(
        f"SELECT COUNT(DISTINCT CAST([MAJ_CAT] AS NVARCHAR(200))+'|'+"
        f"CAST([GEN_ART_NUMBER] AS NVARCHAR(50))+'|'+CAST([CLR] AS NVARCHAR(200))) "
        f"FROM [{final_table}] WHERE [ALLOC_STATUS]='INELIGIBLE'"
    )).scalar() or 0
    result["duration_sec"] = round(time.time() - t0, 1)

    logger.info(f"Allocation complete: {result['alloc_rows']} rows allocated, "
                f"{result['skipped_opts']} OPTs skipped, "
                f"{result['ineligible_opts']} ineligible, {result['duration_sec']}s")
    return result


# ═══════════════════════════════════════════════════════════════════════
#  STEP 1: CREATE ARS_ALLOC_WORKING
# ═══════════════════════════════════════════════════════════════════════

def _create_alloc_working(conn, final_table, alloc_table, msa_var_table) -> int:
    _run(conn, f"IF OBJECT_ID('{alloc_table}','U') IS NOT NULL DROP TABLE [{alloc_table}]")
    _run(conn, f"""
        SELECT
            W.[WERKS], W.[RDC], W.[MAJ_CAT], W.[GEN_ART_NUMBER], W.[CLR],
            W.[GEN_ART_DESC], W.[OPT_TYPE], W.[ST_RANK],
            W.[DPN], W.[SAL_D], W.[I_ROD],
            W.[OPT_MBQ], W.[OPT_REQ], W.[OPT_MBQ_WH], W.[OPT_REQ_WH],
            W.[MAX_DAILY_SALE], W.[ALLOC_FLAG],
            W.[PRI_CT%], W.[SEC_CT%],
            V.[ARTICLE_NUMBER] AS VAR_ART,
            V.[ARTICLE_DESC] AS VAR_DESC,
            V.[SZ], V.[MRP], V.[PAK_SZ],
            TRY_CAST(V.[FNL_Q] AS FLOAT) AS FNL_Q,
            TRY_CAST(V.[STK_QTY] AS FLOAT) AS STK_QTY,
            TRY_CAST(V.[PEND_QTY] AS FLOAT) AS PEND_QTY,
            V.[RDC] AS VAR_RDC, V.[FAB] AS VAR_FAB, V.[SSN] AS VAR_SSN
        INTO [{alloc_table}]
        FROM [{final_table}] W
        INNER JOIN [{msa_var_table}] V WITH (NOLOCK)
            ON  W.[MAJ_CAT] = LTRIM(RTRIM(CAST(V.[MAJ_CAT] AS NVARCHAR(200))))
            AND W.[GEN_ART_NUMBER] = TRY_CAST(TRY_CAST(V.[GEN_ART_NUMBER] AS FLOAT) AS BIGINT)
            AND W.[CLR] = LTRIM(RTRIM(CAST(V.[CLR] AS NVARCHAR(200))))
            AND LTRIM(RTRIM(CAST(W.[RDC] AS NVARCHAR(50)))) = LTRIM(RTRIM(CAST(V.[RDC] AS NVARCHAR(50))))
        WHERE W.[ALLOC_FLAG] = 1
          AND TRY_CAST(V.[FNL_Q] AS FLOAT) > 0
    """)
    return conn.execute(text(f"SELECT COUNT(*) FROM [{alloc_table}]")).scalar() or 0


# ═══════════════════════════════════════════════════════════════════════
#  STEP 2: ENRICH (STK_TTL, CONT, SZ_MBQ, SZ_REQ)
# ═══════════════════════════════════════════════════════════════════════

def _enrich_variant_stock(conn, alloc_table, var_grid_table):
    """Add variant-level STK_TTL from ARS_GRID_MJ_VAR_ART."""
    try:
        _run(conn, f"ALTER TABLE [{alloc_table}] ADD [STK_TTL] FLOAT NULL")
    except Exception:
        pass

    if _exists(conn, var_grid_table):
        gcols = {c.upper() for c in _get_cols(conn, var_grid_table)}
        var_col = next((c for c in ("VAR_ART", "ARTICLE_NUMBER", "GEN_ART") if c in gcols), None)
        if "STK_TTL" in gcols and "WERKS" in gcols and "MAJ_CAT" in gcols and var_col:
            _run(conn, f"""
                UPDATE A SET A.[STK_TTL] = TRY_CAST(G.[STK_TTL] AS FLOAT)
                FROM [{alloc_table}] A
                INNER JOIN [{var_grid_table}] G WITH (NOLOCK)
                    ON G.[WERKS] = A.[WERKS] AND G.[MAJ_CAT] = A.[MAJ_CAT]
                    AND TRY_CAST(G.[{var_col}] AS BIGINT) = TRY_CAST(A.[VAR_ART] AS BIGINT)
            """)
    _run(conn, f"UPDATE [{alloc_table}] SET [STK_TTL] = 0 WHERE [STK_TTL] IS NULL")
    logger.info(f"Alloc enrich: STK_TTL from {var_grid_table}")


def _enrich_size_cont(conn, alloc_table, cont_table):
    """Add CONT from Master_CONT_SZ with ST → CO → auto fallback."""
    for col in ("CONT", "SZ_MBQ", "SZ_REQ"):
        try:
            _run(conn, f"ALTER TABLE [{alloc_table}] ADD [{col}] FLOAT NULL")
        except Exception:
            pass

    if _exists(conn, cont_table):
        # Store-level CONT
        _run(conn, f"""
            UPDATE A SET A.[CONT] = TRY_CAST(M.[CONT] AS FLOAT)
            FROM [{alloc_table}] A
            INNER JOIN [{cont_table}] M WITH (NOLOCK)
                ON  LTRIM(RTRIM(CAST(M.[ST_CD] AS NVARCHAR(50)))) = LTRIM(RTRIM(CAST(A.[WERKS] AS NVARCHAR(50))))
                AND LTRIM(RTRIM(CAST(M.[MAJ_CAT] AS NVARCHAR(200)))) = A.[MAJ_CAT]
                AND LTRIM(RTRIM(CAST(M.[SZ] AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.[SZ] AS NVARCHAR(200))))
        """)
        # CO-level fallback
        _run(conn, f"""
            UPDATE A SET A.[CONT] = TRY_CAST(M.[CONT] AS FLOAT)
            FROM [{alloc_table}] A
            INNER JOIN [{cont_table}] M WITH (NOLOCK)
                ON  LTRIM(RTRIM(CAST(M.[ST_CD] AS NVARCHAR(50)))) = 'CO'
                AND LTRIM(RTRIM(CAST(M.[MAJ_CAT] AS NVARCHAR(200)))) = A.[MAJ_CAT]
                AND LTRIM(RTRIM(CAST(M.[SZ] AS NVARCHAR(200)))) = LTRIM(RTRIM(CAST(A.[SZ] AS NVARCHAR(200))))
            WHERE A.[CONT] IS NULL
        """)

    # Auto-generate fallback: CONT = 1/COUNT(SZ) per (WERKS, MAJ_CAT)
    _run(conn, f"""
        ;WITH SzCount AS (
            SELECT [WERKS], [MAJ_CAT], COUNT(DISTINCT [SZ]) AS sz_cnt
            FROM [{alloc_table}]
            GROUP BY [WERKS], [MAJ_CAT]
        )
        UPDATE A SET A.[CONT] = ROUND(1.0 / NULLIF(C.sz_cnt, 0), 4)
        FROM [{alloc_table}] A
        INNER JOIN SzCount C ON A.[WERKS] = C.[WERKS] AND A.[MAJ_CAT] = C.[MAJ_CAT]
        WHERE ISNULL(A.[CONT], 0) = 0
    """)
    logger.info("Alloc enrich: CONT (ST -> CO -> auto)")


def _calc_sz_mbq_req(conn, alloc_table, new_only: bool = False):
    """
    SZ_MBQ = OPT_MBQ × CONT;  SZ_REQ = MAX(0, SZ_MBQ - STK_TTL).
    When new_only=True, only update rows that have never been allocated
    (protects already-allocated rows during fallback from SZ_REQ reset).
    """
    where_extra = ""
    if new_only:
        where_extra = "WHERE ISNULL([ALLOC_QTY], 0) = 0 AND ISNULL([ALLOC_ROUND], 0) = 0"
    _run(conn, f"""
        UPDATE [{alloc_table}]
        SET [SZ_MBQ] = ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0),
            [SZ_REQ] = CASE
                WHEN (ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0)) - ISNULL([STK_TTL], 0) > 0
                    THEN ROUND((ISNULL([OPT_MBQ], 0) * ISNULL([CONT], 0)) - ISNULL([STK_TTL], 0), 0)
                ELSE 0
            END
        {where_extra}
    """)
    logger.info(f"Alloc enrich: SZ_MBQ + SZ_REQ calculated{' (new rows only)' if new_only else ''}")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 3: TRACKING + STATUS COLUMNS
# ═══════════════════════════════════════════════════════════════════════

def _add_tracking_columns(conn, alloc_table, final_table):
    """Add control + status columns to both alloc and working tables."""
    # ── ARS_ALLOC_WORKING columns ─────────────────────────────────────
    alloc_cols = {
        "ALLOC_QTY":    "FLOAT NULL DEFAULT 0",
        "ALLOC_ROUND":  "INT NULL DEFAULT 0",
        "SKIP_FLAG":    "INT NULL DEFAULT 0",
        "ROUND_ALLOC":  "FLOAT NULL DEFAULT 0",       # delta for current round
        "ALLOC_STATUS": "NVARCHAR(50) NULL DEFAULT 'PENDING'",
        "SKIP_REASON":  "NVARCHAR(500) NULL",
    }
    for col, typedef in alloc_cols.items():
        try:
            _run(conn, f"ALTER TABLE [{alloc_table}] ADD [{col}] {typedef}")
        except Exception:
            pass
    _run(conn, f"""
        UPDATE [{alloc_table}]
        SET [ALLOC_QTY]=0, [ALLOC_ROUND]=0, [SKIP_FLAG]=0,
            [ROUND_ALLOC]=0, [ALLOC_STATUS]='PENDING', [SKIP_REASON]=NULL
    """)

    # ── ARS_LISTING_WORKING columns ───────────────────────────────────
    work_cols = {
        "ALLOC_STATUS":  "NVARCHAR(50) NULL DEFAULT 'PENDING'",
        "ALLOC_REMARKS": "NVARCHAR(MAX) NULL",
    }
    for col, typedef in work_cols.items():
        try:
            _run(conn, f"ALTER TABLE [{final_table}] ADD [{col}] {typedef}")
        except Exception:
            pass
    _run(conn, f"UPDATE [{final_table}] SET [ALLOC_STATUS]='PENDING', [ALLOC_REMARKS]=''")
    logger.info("Tracking columns added to alloc + working tables")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 4: POOL TRACKER
# ═══════════════════════════════════════════════════════════════════════

def _create_pool(conn, alloc_table):
    """Create #alloc_pool tracking remaining FNL_Q per variant-size pool."""
    _run(conn, f"IF OBJECT_ID('tempdb..{POOL_TABLE}') IS NOT NULL DROP TABLE {POOL_TABLE}")
    _run(conn, f"""
        SELECT
            [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ],
            MAX(ISNULL([FNL_Q], 0)) AS FNL_Q_ORIG,
            MAX(ISNULL([FNL_Q], 0)) AS FNL_Q_REM
        INTO {POOL_TABLE}
        FROM [{alloc_table}]
        GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ]
    """)
    try:
        _run(conn, f"""
            CREATE NONCLUSTERED INDEX IX_pool
            ON {POOL_TABLE}([RDC],[MAJ_CAT],[GEN_ART_NUMBER],[CLR],[VAR_ART],[SZ])
        """)
    except Exception:
        pass
    cnt = conn.execute(text(f"SELECT COUNT(*) FROM {POOL_TABLE}")).scalar()
    logger.info(f"Pool tracker: {cnt} pools created")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 5: MARK INITIAL ELIGIBILITY ON LISTING_WORKING
#  All eligibility logic is written here against ARS_LISTING_WORKING
# ═══════════════════════════════════════════════════════════════════════

def _mark_initial_eligibility(conn, final_table):
    """
    Mark each row in ARS_LISTING_WORKING with initial allocation eligibility.
    Rows that fail any check are marked INELIGIBLE with a reason.
    Only PENDING rows proceed to allocation.

    Checks (evaluated in priority order — first failure wins):
      E3: OPT_TYPE ≠ 'MIX'
      E1: LISTING = 1
      E2: ALLOC_FLAG = 1  (PRI_CT% ≥ 100)
      E4: MSA_FNL_Q > 0
      E5: OPT_REQ_WH ≥ 1
    """
    _run(conn, f"""
        UPDATE [{final_table}] SET
            [ALLOC_STATUS] = CASE
                WHEN ISNULL([OPT_TYPE], '') = 'MIX'
                    THEN 'INELIGIBLE'
                WHEN ISNULL(TRY_CAST([LISTING] AS INT), 1) != 1
                    THEN 'INELIGIBLE'
                WHEN ISNULL([ALLOC_FLAG], 0) != 1
                    THEN 'INELIGIBLE'
                WHEN ISNULL(TRY_CAST([MSA_FNL_Q] AS FLOAT), 0) <= 0
                    THEN 'INELIGIBLE'
                WHEN ISNULL(TRY_CAST([OPT_REQ_WH] AS FLOAT), 0) < 1
                    THEN 'INELIGIBLE'
                ELSE 'PENDING'
            END,
            [ALLOC_REMARKS] = CASE
                WHEN ISNULL([OPT_TYPE], '') = 'MIX'
                    THEN 'E3:OPT_TYPE=MIX; '
                WHEN ISNULL(TRY_CAST([LISTING] AS INT), 1) != 1
                    THEN 'E1:LISTING!=1; '
                WHEN ISNULL([ALLOC_FLAG], 0) != 1
                    THEN 'E2:ALLOC_FLAG=0(PRI_CT%='
                         + CAST(ISNULL([PRI_CT%], 0) AS NVARCHAR(10)) + '); '
                WHEN ISNULL(TRY_CAST([MSA_FNL_Q] AS FLOAT), 0) <= 0
                    THEN 'E4:MSA_FNL_Q=0; '
                WHEN ISNULL(TRY_CAST([OPT_REQ_WH] AS FLOAT), 0) < 1
                    THEN 'E5:OPT_REQ_WH='
                         + CAST(ISNULL([OPT_REQ_WH], 0) AS NVARCHAR(10)) + '<1; '
                ELSE ''
            END
    """)

    # Log summary
    row = conn.execute(text(f"""
        SELECT
            SUM(CASE WHEN [ALLOC_STATUS]='PENDING' THEN 1 ELSE 0 END) AS eligible,
            SUM(CASE WHEN [ALLOC_STATUS]='INELIGIBLE' THEN 1 ELSE 0 END) AS ineligible,
            COUNT(*) AS total
        FROM [{final_table}]
    """)).fetchone()
    logger.info(f"Initial eligibility: {row[0]} eligible, {row[1]} ineligible out of {row[2]} rows")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 6: PRIMARY ALLOCATION
# ═══════════════════════════════════════════════════════════════════════

def _run_primary(conn, alloc_table, final_table, threshold,
                 only_new: bool = False) -> Dict:
    """
    RL → TBC → TBL, I_ROD rounds.
    Uses BATCH allocation — all OPTs processed in ~12 SQL calls per round
    instead of N × 10 calls per OPT.

    only_new: when True (fallback mode), only process OPTs that have NEVER
              been touched (ALLOC_ROUND=0 on all rows).
    """
    stats = {"allocated": 0, "skipped": 0, "ineligible": 0, "rounds": []}

    for opt_type in OPT_TYPE_ORDER:
        irod_filter = f"WHERE [OPT_TYPE]=:ot AND ISNULL([SKIP_FLAG],0)=0"
        if only_new:
            irod_filter += " AND ISNULL([ALLOC_ROUND], 0) = 0"

        max_irod = conn.execute(text(
            f"SELECT MAX(ISNULL(CAST([I_ROD] AS INT), 1)) FROM [{alloc_table}] "
            f"{irod_filter}"
        ), {"ot": opt_type}).scalar() or 0

        if max_irod == 0:
            continue

        for round_num in range(1, max_irod + 1):
            _scale_demand_for_round(conn, alloc_table, opt_type, round_num)
            rnd_result = _allocate_batch_round(
                conn, alloc_table, final_table, opt_type, round_num,
                threshold, only_new
            )
            stats["rounds"].append(rnd_result)
            stats["allocated"] += rnd_result.get("allocated", 0)
            stats["skipped"] += rnd_result.get("skipped", 0)
            stats["ineligible"] += rnd_result.get("ineligible", 0)
            logger.info(
                f"Primary {opt_type} R{round_num}: {rnd_result.get('opts', 0)} OPTs, "
                f"alloc={rnd_result.get('allocated', 0)}, "
                f"skip={rnd_result.get('skipped', 0)}, "
                f"ineligible={rnd_result.get('ineligible', 0)}, "
                f"{rnd_result.get('seconds', 0)}s"
            )

    return stats


# ═══════════════════════════════════════════════════════════════════════
#  ALLOCATION HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _scale_demand_for_round(conn, alloc_table, opt_type: str, round_num: int):
    """
    For round N:
      SZ_MBQ = OPT_MBQ × N × CONT     (total planned need for N rounds)
      SZ_REQ = MAX(0, SZ_MBQ - STK_TTL - prev_ALLOC_QTY)
    Previous rounds' ALLOC_QTY is subtracted so only incremental demand remains.
    """
    if round_num <= 1:
        return  # Round 1 uses initial SZ_REQ from _calc_sz_mbq_req

    _run(conn, f"""
        UPDATE [{alloc_table}]
        SET [SZ_MBQ] = ROUND(ISNULL([OPT_MBQ], 0) * :rnd * ISNULL([CONT], 0), 0),
            [SZ_REQ] = CASE
                WHEN (ISNULL([OPT_MBQ], 0) * :rnd * ISNULL([CONT], 0))
                     - ISNULL([STK_TTL], 0) - ISNULL([ALLOC_QTY], 0) > 0
                THEN ROUND(
                    (ISNULL([OPT_MBQ], 0) * :rnd * ISNULL([CONT], 0))
                    - ISNULL([STK_TTL], 0) - ISNULL([ALLOC_QTY], 0), 0)
                ELSE 0
            END
        WHERE [OPT_TYPE] = :ot
          AND ISNULL(CAST([I_ROD] AS INT), 1) >= :rnd
          AND ISNULL([SKIP_FLAG], 0) = 0
    """, {"ot": opt_type, "rnd": round_num})
    logger.debug(f"Demand scaled for {opt_type} R{round_num}: SZ_MBQ=OPT_MBQ*{round_num}*CONT")


def _allocate_batch_round(
    conn, alloc_table, final_table,
    opt_type: str, round_num: int, threshold: float,
    only_new: bool = False,
) -> Dict:
    """
    Batch-allocate ALL eligible OPTs for one OPT_TYPE + round.
    ~12 SQL calls total regardless of OPT count.

    Key insight: OPTs have independent pools (keyed by MAJ_CAT+GEN_ART+CLR),
    so the waterfall PARTITION BY correctly separates them.  No per-OPT loop
    is needed — the window function handles all OPTs simultaneously.

    Steps:
      1. Batch waterfall → ROUND_ALLOC for all OPTs (1 SQL)
      2. Batch pool deduction (1 SQL)
      3. Batch validation → [{BREAK_TABLE}] temp table (1 SQL)
      4. Batch restore pool + zero ROUND_ALLOC for failing stores (2 SQL)
      5. Batch SKIP_FLAG for failing OPTs (1 SQL)
      6. Batch remarks on listing_working (2 SQL)
      7. Batch commit ALLOC_QTY += ROUND_ALLOC (1 SQL)
      8. Batch post-sync MSA_FNL_Q, OPT_REQ_WH, VAR_FNL_COUNT (3 SQL)
    """
    t0 = time.time()
    result = {"opt_type": opt_type, "round": round_num,
              "opts": 0, "allocated": 0, "skipped": 0, "ineligible": 0, "seconds": 0}

    # ── only_new filter (for fallback: skip already-processed OPTs) ───
    only_new_sql = ""
    if only_new:
        only_new_sql = f"""
            AND NOT EXISTS (
                SELECT 1 FROM [{alloc_table}] OLD
                WHERE OLD.[MAJ_CAT] = A.[MAJ_CAT]
                  AND OLD.[GEN_ART_NUMBER] = A.[GEN_ART_NUMBER]
                  AND OLD.[CLR] = A.[CLR]
                  AND ISNULL(OLD.[ALLOC_ROUND], 0) > 0
            )"""

    # ── Count eligible OPTs ───────────────────────────────────────────
    opt_count = conn.execute(text(f"""
        SELECT COUNT(DISTINCT
            CAST([MAJ_CAT] AS NVARCHAR(200)) + '|' +
            CAST([GEN_ART_NUMBER] AS NVARCHAR(50)) + '|' +
            CAST([CLR] AS NVARCHAR(200)))
        FROM [{alloc_table}]
        WHERE [OPT_TYPE] = :ot AND ISNULL([SKIP_FLAG], 0) = 0
          AND ISNULL(CAST([I_ROD] AS INT), 1) >= :rnd
    """), {"ot": opt_type, "rnd": round_num}).scalar() or 0
    result["opts"] = opt_count
    if opt_count == 0:
        result["seconds"] = round(time.time() - t0, 1)
        return result

    # ══════════════════════════════════════════════════════════════════
    # STEP 1: Batch waterfall — ALL OPTs in one SQL
    # The PARTITION BY (RDC, MAJ_CAT, GEN_ART_NUMBER, CLR, VAR_ART, SZ)
    # correctly separates independent OPT pools.
    # ══════════════════════════════════════════════════════════════════
    _run(conn, f"""
        ;WITH PoolDemand AS (
            SELECT
                A.[WERKS], A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER], A.[CLR],
                A.[VAR_ART], A.[SZ], A.[ST_RANK],
                P.[FNL_Q_REM],
                ISNULL(A.[SZ_REQ], 0) AS SZ_REQ,
                ISNULL(SUM(ISNULL(A.[SZ_REQ], 0)) OVER (
                    PARTITION BY A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER],
                                A.[CLR], A.[VAR_ART], A.[SZ]
                    ORDER BY ISNULL(A.[ST_RANK], 999999) ASC, A.[WERKS]
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0) AS prev_demand
            FROM [{alloc_table}] A
            INNER JOIN {POOL_TABLE} P
                ON  A.[RDC] = P.[RDC] AND A.[MAJ_CAT] = P.[MAJ_CAT]
                AND A.[GEN_ART_NUMBER] = P.[GEN_ART_NUMBER] AND A.[CLR] = P.[CLR]
                AND A.[VAR_ART] = P.[VAR_ART] AND A.[SZ] = P.[SZ]
            WHERE A.[OPT_TYPE] = :ot
              AND ISNULL(A.[SKIP_FLAG], 0) = 0
              AND ISNULL(CAST(A.[I_ROD] AS INT), 1) >= :rnd
              AND ISNULL(A.[SZ_REQ], 0) > 0
              {only_new_sql}
              AND EXISTS (
                  SELECT 1 FROM [{final_table}] W
                  WHERE W.[WERKS] = A.[WERKS]
                    AND W.[MAJ_CAT] = A.[MAJ_CAT]
                    AND W.[GEN_ART_NUMBER] = A.[GEN_ART_NUMBER]
                    AND W.[CLR] = A.[CLR]
                    AND ISNULL(TRY_CAST(W.[LISTING] AS INT), 1) = 1
                    AND ISNULL(W.[ALLOC_FLAG], 0) = 1
                    AND ISNULL(TRY_CAST(W.[MSA_FNL_Q] AS FLOAT), 0) > 0
                    AND ISNULL(TRY_CAST(W.[OPT_REQ_WH] AS FLOAT), 0) >= 1
              )
        ),
        Allocated AS (
            SELECT *,
                CASE
                    WHEN FNL_Q_REM - prev_demand <= 0 THEN 0
                    WHEN SZ_REQ <= FNL_Q_REM - prev_demand THEN SZ_REQ
                    ELSE FNL_Q_REM - prev_demand
                END AS round_alloc
            FROM PoolDemand
        )
        UPDATE A SET A.[ROUND_ALLOC] = AL.round_alloc, A.[ALLOC_ROUND] = :rnd
        FROM [{alloc_table}] A
        INNER JOIN Allocated AL
            ON  A.[WERKS] = AL.[WERKS] AND A.[RDC] = AL.[RDC]
            AND A.[MAJ_CAT] = AL.[MAJ_CAT] AND A.[GEN_ART_NUMBER] = AL.[GEN_ART_NUMBER]
            AND A.[CLR] = AL.[CLR] AND A.[VAR_ART] = AL.[VAR_ART] AND A.[SZ] = AL.[SZ]
        WHERE AL.round_alloc > 0
    """, {"ot": opt_type, "rnd": round_num})

    # Check if anything was allocated
    round_total = conn.execute(text(f"""
        SELECT ISNULL(SUM([ROUND_ALLOC]), 0)
        FROM [{alloc_table}]
        WHERE [OPT_TYPE] = :ot AND ISNULL([ROUND_ALLOC], 0) > 0
    """), {"ot": opt_type}).scalar() or 0

    if round_total == 0:
        result["seconds"] = round(time.time() - t0, 1)
        return result

    # ══════════════════════════════════════════════════════════════════
    # STEP 2: Batch pool deduction
    # ══════════════════════════════════════════════════════════════════
    _run(conn, f"""
        UPDATE P SET P.[FNL_Q_REM] = P.[FNL_Q_REM] - ISNULL(D.consumed, 0)
        FROM {POOL_TABLE} P
        INNER JOIN (
            SELECT [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ],
                   SUM(ISNULL([ROUND_ALLOC], 0)) AS consumed
            FROM [{alloc_table}]
            WHERE [OPT_TYPE] = :ot AND ISNULL([ROUND_ALLOC], 0) > 0
            GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ]
        ) D ON  P.[RDC] = D.[RDC] AND P.[MAJ_CAT] = D.[MAJ_CAT]
            AND P.[GEN_ART_NUMBER] = D.[GEN_ART_NUMBER] AND P.[CLR] = D.[CLR]
            AND P.[VAR_ART] = D.[VAR_ART] AND P.[SZ] = D.[SZ]
    """, {"ot": opt_type})

    # ══════════════════════════════════════════════════════════════════
    # STEP 3: Batch validation — find ALL failing OPTs + break ranks
    # Uses: pool_after_rank = FNL_Q_REM + total_alloc - cum_alloc
    #   FNL_Q_REM = current pool (fully deducted)
    #   total_alloc = sum of ROUND_ALLOC for this pool entry
    #   cum_alloc = cumulative ROUND_ALLOC up to this ST_RANK
    # ══════════════════════════════════════════════════════════════════
    _run(conn, f"IF OBJECT_ID('{BREAK_TABLE}','U') IS NOT NULL DROP TABLE [{BREAK_TABLE}]")

    _run(conn, f"""
        ;WITH AllocCum AS (
            SELECT
                [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ],
                [ST_RANK],
                SUM(ISNULL([ROUND_ALLOC], 0)) OVER (
                    PARTITION BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ]
                    ORDER BY ISNULL([ST_RANK], 999999), [WERKS]
                ) AS cum_alloc
            FROM [{alloc_table}]
            WHERE [OPT_TYPE] = :ot AND ISNULL([ROUND_ALLOC], 0) > 0
        ),
        AllocPerRank AS (
            SELECT [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ], [ST_RANK],
                   MAX(cum_alloc) AS cum_alloc
            FROM AllocCum
            GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ], [ST_RANK]
        ),
        TotalAllocPerPool AS (
            SELECT [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ],
                   MAX(cum_alloc) AS total_alloc
            FROM AllocPerRank
            GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [VAR_ART], [SZ]
        ),
        PoolAtRank AS (
            SELECT
                C.[MAJ_CAT], C.[GEN_ART_NUMBER], C.[CLR], C.[ST_RANK], P.[SZ],
                P.[FNL_Q_REM] + ISNULL(T.total_alloc, 0) - C.cum_alloc AS pool_after
            FROM AllocPerRank C
            INNER JOIN {POOL_TABLE} P
                ON  P.[RDC]=C.[RDC] AND P.[MAJ_CAT]=C.[MAJ_CAT]
                AND P.[GEN_ART_NUMBER]=C.[GEN_ART_NUMBER] AND P.[CLR]=C.[CLR]
                AND P.[VAR_ART]=C.[VAR_ART] AND P.[SZ]=C.[SZ]
            INNER JOIN TotalAllocPerPool T
                ON  T.[RDC]=C.[RDC] AND T.[MAJ_CAT]=C.[MAJ_CAT]
                AND T.[GEN_ART_NUMBER]=C.[GEN_ART_NUMBER] AND T.[CLR]=C.[CLR]
                AND T.[VAR_ART]=C.[VAR_ART] AND T.[SZ]=C.[SZ]
        ),
        SzAvail AS (
            SELECT [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [ST_RANK],
                COUNT(DISTINCT [SZ]) AS total_sz,
                COUNT(DISTINCT CASE WHEN pool_after > 0 THEN [SZ] END) AS sz_ok
            FROM PoolAtRank
            GROUP BY [MAJ_CAT], [GEN_ART_NUMBER], [CLR], [ST_RANK]
        )
        SELECT
            [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
            MIN([ST_RANK]) AS break_rank,
            MIN(total_sz) AS total_sz,
            MIN(sz_ok) AS sz_ok_at_break
        INTO [{BREAK_TABLE}]
        FROM SzAvail
        WHERE total_sz > 0 AND CAST(sz_ok AS FLOAT) / total_sz < :thr
        GROUP BY [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
    """, {"ot": opt_type, "thr": threshold})

    break_count = conn.execute(text(
        f"SELECT COUNT(*) FROM [{BREAK_TABLE}]"
    )).scalar() or 0
    result["skipped"] = break_count

    # ══════════════════════════════════════════════════════════════════
    # STEP 4+5: Batch restore pool + zero ROUND_ALLOC + SKIP_FLAG
    # ══════════════════════════════════════════════════════════════════
    if break_count > 0:
        # 4a: Restore pool for stores at or after break rank
        _run(conn, f"""
            UPDATE P SET P.[FNL_Q_REM] = P.[FNL_Q_REM] + ISNULL(R.restore_qty, 0)
            FROM {POOL_TABLE} P
            INNER JOIN (
                SELECT A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER], A.[CLR],
                       A.[VAR_ART], A.[SZ],
                       SUM(ISNULL(A.[ROUND_ALLOC], 0)) AS restore_qty
                FROM [{alloc_table}] A
                INNER JOIN [{BREAK_TABLE}] BR
                    ON  A.[MAJ_CAT] = BR.[MAJ_CAT]
                    AND A.[GEN_ART_NUMBER] = BR.[GEN_ART_NUMBER]
                    AND A.[CLR] = BR.[CLR]
                WHERE A.[OPT_TYPE] = :ot
                  AND ISNULL(A.[ST_RANK], 999999) >= BR.break_rank
                  AND ISNULL(A.[ROUND_ALLOC], 0) > 0
                GROUP BY A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER], A.[CLR],
                         A.[VAR_ART], A.[SZ]
            ) R ON  P.[RDC]=R.[RDC] AND P.[MAJ_CAT]=R.[MAJ_CAT]
                AND P.[GEN_ART_NUMBER]=R.[GEN_ART_NUMBER] AND P.[CLR]=R.[CLR]
                AND P.[VAR_ART]=R.[VAR_ART] AND P.[SZ]=R.[SZ]
        """, {"ot": opt_type})

        # 4b: Zero ROUND_ALLOC + mark skipped for stores >= break_rank
        _run(conn, f"""
            UPDATE A SET
                A.[ROUND_ALLOC] = 0,
                A.[ALLOC_STATUS] = CASE
                    WHEN ISNULL(A.[ALLOC_QTY], 0) > 0 THEN 'PARTIAL'
                    ELSE 'SKIPPED' END,
                A.[SKIP_REASON] = 'B3:SZ_AVAIL<{int(threshold*100)}%,BREAK@RANK='
                    + CAST(BR.break_rank AS NVARCHAR(10))
            FROM [{alloc_table}] A
            INNER JOIN [{BREAK_TABLE}] BR
                ON  A.[MAJ_CAT] = BR.[MAJ_CAT]
                AND A.[GEN_ART_NUMBER] = BR.[GEN_ART_NUMBER]
                AND A.[CLR] = BR.[CLR]
            WHERE A.[OPT_TYPE] = :ot
              AND ISNULL(A.[ST_RANK], 999999) >= BR.break_rank
        """, {"ot": opt_type})

        # 5: Set SKIP_FLAG for all rows of failing OPTs (this OPT_TYPE)
        _run(conn, f"""
            UPDATE A SET A.[SKIP_FLAG] = 1
            FROM [{alloc_table}] A
            INNER JOIN [{BREAK_TABLE}] BR
                ON  A.[MAJ_CAT] = BR.[MAJ_CAT]
                AND A.[GEN_ART_NUMBER] = BR.[GEN_ART_NUMBER]
                AND A.[CLR] = BR.[CLR]
            WHERE A.[OPT_TYPE] = :ot
        """, {"ot": opt_type})

    # ══════════════════════════════════════════════════════════════════
    # STEP 6: Batch remarks on listing_working
    # ══════════════════════════════════════════════════════════════════
    remark_prefix = f"{opt_type} R{round_num}:QTY="
    _run(conn, f"""
        UPDATE W SET W.[ALLOC_REMARKS] = ISNULL(W.[ALLOC_REMARKS], '') +
            :prefix + CAST(ISNULL(RA.round_qty, 0) AS NVARCHAR(20)) + '; '
        FROM [{final_table}] W
        INNER JOIN (
            SELECT [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                   SUM(ISNULL([ROUND_ALLOC], 0)) AS round_qty
            FROM [{alloc_table}]
            WHERE [OPT_TYPE] = :ot AND ISNULL([ROUND_ALLOC], 0) > 0
            GROUP BY [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        ) RA ON W.[WERKS] = RA.[WERKS] AND W.[MAJ_CAT] = RA.[MAJ_CAT]
            AND W.[GEN_ART_NUMBER] = RA.[GEN_ART_NUMBER] AND W.[CLR] = RA.[CLR]
        WHERE W.[OPT_TYPE] = :ot
    """, {"ot": opt_type, "prefix": remark_prefix})

    if break_count > 0:
        skip_prefix = f"{opt_type} R{round_num}:SKIP BREAK@RANK="
        _run(conn, f"""
            UPDATE W SET W.[ALLOC_REMARKS] = ISNULL(W.[ALLOC_REMARKS], '') +
                :prefix + CAST(BR.break_rank AS NVARCHAR(10)) + '; '
            FROM [{final_table}] W
            INNER JOIN [{BREAK_TABLE}] BR
                ON  W.[MAJ_CAT] = BR.[MAJ_CAT]
                AND W.[GEN_ART_NUMBER] = BR.[GEN_ART_NUMBER]
                AND W.[CLR] = BR.[CLR]
            WHERE W.[OPT_TYPE] = :ot
              AND ISNULL(W.[ST_RANK], 999999) >= BR.break_rank
        """, {"ot": opt_type, "prefix": skip_prefix})

    # ══════════════════════════════════════════════════════════════════
    # STEP 7: Batch commit — ALLOC_QTY += ROUND_ALLOC, set status
    # ══════════════════════════════════════════════════════════════════
    committed = conn.execute(text(f"""
        SELECT ISNULL(SUM([ROUND_ALLOC]), 0)
        FROM [{alloc_table}]
        WHERE [OPT_TYPE] = :ot AND ISNULL([ROUND_ALLOC], 0) > 0
    """), {"ot": opt_type}).scalar() or 0
    result["allocated"] = committed

    _run(conn, f"""
        UPDATE [{alloc_table}]
        SET [ALLOC_QTY] = ISNULL([ALLOC_QTY], 0) + ISNULL([ROUND_ALLOC], 0),
            [ALLOC_ROUND] = CASE WHEN ISNULL([ROUND_ALLOC], 0) > 0 THEN :rnd
                                 ELSE [ALLOC_ROUND] END,
            [ALLOC_STATUS] = CASE
                WHEN ISNULL([ROUND_ALLOC], 0) > 0
                     AND ISNULL([ROUND_ALLOC], 0) >= ISNULL([SZ_REQ], 0)
                    THEN 'ALLOCATED'
                WHEN ISNULL([ROUND_ALLOC], 0) > 0 THEN 'PARTIAL'
                WHEN [ALLOC_STATUS] IN ('SKIPPED', 'INELIGIBLE') THEN [ALLOC_STATUS]
                ELSE [ALLOC_STATUS]
            END,
            [ROUND_ALLOC] = 0
        WHERE [OPT_TYPE] = :ot
    """, {"ot": opt_type, "rnd": round_num})

    # ══════════════════════════════════════════════════════════════════
    # STEP 8: Batch post-sync to listing_working
    # ══════════════════════════════════════════════════════════════════
    # B1: MSA_FNL_Q = current pool remaining (all OPTs at once)
    _run(conn, f"""
        UPDATE W SET W.[MSA_FNL_Q] = ISNULL(P.pool_rem, 0)
        FROM [{final_table}] W
        INNER JOIN (
            SELECT [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                   SUM([FNL_Q_REM]) AS pool_rem
            FROM {POOL_TABLE}
            GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        ) P ON LTRIM(RTRIM(CAST(W.[RDC] AS NVARCHAR(50)))) = P.[RDC]
           AND W.[MAJ_CAT] = P.[MAJ_CAT]
           AND W.[GEN_ART_NUMBER] = P.[GEN_ART_NUMBER]
           AND W.[CLR] = P.[CLR]
    """)

    # B2: OPT_REQ_WH = MAX(0, OPT_MBQ_WH - STK_TTL - alloc_so_far)
    _run(conn, f"""
        UPDATE W SET
            W.[OPT_REQ] = CASE
                WHEN ISNULL(W.[OPT_MBQ], 0) - ISNULL(W.[STK_TTL], 0)
                     - ISNULL(SA.store_alloc, 0) > 0
                THEN ISNULL(W.[OPT_MBQ], 0) - ISNULL(W.[STK_TTL], 0)
                     - ISNULL(SA.store_alloc, 0)
                ELSE 0 END,
            W.[OPT_REQ_WH] = CASE
                WHEN ISNULL(W.[OPT_MBQ_WH], 0) - ISNULL(W.[STK_TTL], 0)
                     - ISNULL(SA.store_alloc, 0) > 0
                THEN ISNULL(W.[OPT_MBQ_WH], 0) - ISNULL(W.[STK_TTL], 0)
                     - ISNULL(SA.store_alloc, 0)
                ELSE 0 END
        FROM [{final_table}] W
        INNER JOIN (
            SELECT [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                   SUM(ISNULL([ALLOC_QTY], 0)) AS store_alloc
            FROM [{alloc_table}]
            GROUP BY [WERKS], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        ) SA ON W.[WERKS] = SA.[WERKS] AND W.[MAJ_CAT] = SA.[MAJ_CAT]
           AND W.[GEN_ART_NUMBER] = SA.[GEN_ART_NUMBER] AND W.[CLR] = SA.[CLR]
    """)

    # B3: VAR_FNL_COUNT = distinct variants still in pool
    _run(conn, f"""
        UPDATE W SET W.[VAR_FNL_COUNT] = ISNULL(VC.live_vars, 0)
        FROM [{final_table}] W
        INNER JOIN (
            SELECT [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                   COUNT(DISTINCT [VAR_ART]) AS live_vars
            FROM {POOL_TABLE}
            WHERE [FNL_Q_REM] > 0
            GROUP BY [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        ) VC ON LTRIM(RTRIM(CAST(W.[RDC] AS NVARCHAR(50)))) = VC.[RDC]
            AND W.[MAJ_CAT] = VC.[MAJ_CAT]
            AND W.[GEN_ART_NUMBER] = VC.[GEN_ART_NUMBER]
            AND W.[CLR] = VC.[CLR]
    """)

    # ── Cleanup ───────────────────────────────────────────────────────
    try:
        _run(conn, f"IF OBJECT_ID('{BREAK_TABLE}','U') IS NOT NULL DROP TABLE [{BREAK_TABLE}]")
    except Exception:
        pass

    result["seconds"] = round(time.time() - t0, 1)
    return result


# ═══════════════════════════════════════════════════════════════════════
#  STEP 7: FALLBACK — GRID DEMOTION
# ═══════════════════════════════════════════════════════════════════════

def _run_fallback(conn, final_table, alloc_table, threshold) -> Dict:
    """
    Demote last primary grid → secondary, one level at a time.
    Re-check ALLOC_FLAG, run allocation for ONLY newly eligible OPTs.
    Grid seq=1 always stays primary.

    Key safeguards:
      - Tracks demoted grids to restore ONLY those (not originally Secondary)
      - Re-eligibility uses condition checks (not string matching)
      - Enrichment uses new_only=True to protect already-allocated rows
      - _run_primary(only_new=True) skips already-processed OPTs
      - Calls _enrich_variant_stock for new rows (STK_TTL)
    """
    stats = {"levels": 0, "newly_eligible": 0, "allocated": 0}

    if not _exists(conn, "ARS_GRID_BUILDER"):
        return stats

    primary_grids = conn.execute(text("""
        SELECT grid_name, hierarchy_columns, seq, ISNULL(grid_group, 'None') AS grid_group
        FROM [ARS_GRID_BUILDER]
        WHERE UPPER(status) = 'ACTIVE'
          AND ISNULL(grid_group, 'None') = 'Primary'
        ORDER BY seq DESC
    """)).fetchall()

    if len(primary_grids) <= 1:
        logger.info("Fallback: only 1 primary grid (seq=1), no demotion possible")
        return stats

    # Track which grids WE demote (so we only restore those, not originals)
    demoted_grids = []

    for grid in primary_grids:
        gname, ghier_json, seq, _ = grid

        if seq <= 1:
            break

        stats["levels"] += 1
        demoted_grids.append(gname)
        logger.info(f"Fallback level {stats['levels']}: demoting {gname} (seq={seq}) to Secondary")

        _run(conn, """
            UPDATE [ARS_GRID_BUILDER] SET grid_group = 'Secondary'
            WHERE grid_name = :gn
        """, {"gn": gname})

        _recalc_alloc_flag(conn, final_table)

        # ── Re-mark eligibility: condition-based (not string matching) ──
        # Only re-enable rows where ALLOC_FLAG just became 1 AND
        # all other eligibility checks still pass
        lvl = stats["levels"]
        _run(conn, f"""
            UPDATE [{final_table}]
            SET [ALLOC_STATUS] = 'PENDING',
                [ALLOC_REMARKS] = ISNULL([ALLOC_REMARKS], '')
                    + 'FALLBACK_LVL={lvl}:ALLOC_FLAG->1; '
            WHERE [ALLOC_FLAG] = 1
              AND [ALLOC_STATUS] = 'INELIGIBLE'
              AND ISNULL(TRY_CAST([LISTING] AS INT), 1) = 1
              AND ISNULL([OPT_TYPE], '') != 'MIX'
              AND ISNULL(TRY_CAST([MSA_FNL_Q] AS FLOAT), 0) > 0
              AND ISNULL(TRY_CAST([OPT_REQ_WH] AS FLOAT), 0) >= 1
        """)

        # ── Count newly eligible OPTs (PENDING + not yet in alloc_table) ──
        new_opts = conn.execute(text(f"""
            SELECT COUNT(*)
            FROM [{final_table}] W
            WHERE W.[ALLOC_FLAG] = 1
              AND W.[ALLOC_STATUS] = 'PENDING'
              AND NOT EXISTS (
                  SELECT 1 FROM [{alloc_table}] A
                  WHERE A.[WERKS] = W.[WERKS] AND A.[MAJ_CAT] = W.[MAJ_CAT]
                    AND A.[GEN_ART_NUMBER] = W.[GEN_ART_NUMBER] AND A.[CLR] = W.[CLR]
              )
        """)).scalar() or 0

        if new_opts == 0:
            logger.info(f"Fallback level {stats['levels']}: no newly eligible OPTs, continuing")
            continue

        stats["newly_eligible"] += new_opts
        logger.info(f"Fallback level {stats['levels']}: {new_opts} newly eligible OPTs")

        # ── Insert newly eligible into alloc_table ────────────────────
        _run(conn, f"""
            INSERT INTO [{alloc_table}]
            ([WERKS],[RDC],[MAJ_CAT],[GEN_ART_NUMBER],[CLR],
             [GEN_ART_DESC],[OPT_TYPE],[ST_RANK],[DPN],[SAL_D],[I_ROD],
             [OPT_MBQ],[OPT_REQ],[OPT_MBQ_WH],[OPT_REQ_WH],
             [MAX_DAILY_SALE],[ALLOC_FLAG],[PRI_CT%],[SEC_CT%],
             [VAR_ART],[VAR_DESC],[SZ],[MRP],[PAK_SZ],
             [FNL_Q],[STK_QTY],[PEND_QTY],[VAR_RDC],[VAR_FAB],[VAR_SSN],
             [STK_TTL],[CONT],[SZ_MBQ],[SZ_REQ],
             [ALLOC_QTY],[ALLOC_ROUND],[SKIP_FLAG],[ROUND_ALLOC],
             [ALLOC_STATUS],[SKIP_REASON])
            SELECT
                W.[WERKS], W.[RDC], W.[MAJ_CAT], W.[GEN_ART_NUMBER], W.[CLR],
                W.[GEN_ART_DESC], W.[OPT_TYPE], W.[ST_RANK], W.[DPN], W.[SAL_D], W.[I_ROD],
                W.[OPT_MBQ], W.[OPT_REQ], W.[OPT_MBQ_WH], W.[OPT_REQ_WH],
                W.[MAX_DAILY_SALE], W.[ALLOC_FLAG], W.[PRI_CT%], W.[SEC_CT%],
                V.[ARTICLE_NUMBER], V.[ARTICLE_DESC], V.[SZ], V.[MRP], V.[PAK_SZ],
                TRY_CAST(V.[FNL_Q] AS FLOAT), TRY_CAST(V.[STK_QTY] AS FLOAT),
                TRY_CAST(V.[PEND_QTY] AS FLOAT),
                V.[RDC], V.[FAB], V.[SSN],
                0, 0, 0, 0,   -- STK_TTL, CONT, SZ_MBQ, SZ_REQ (enriched below)
                0, 0, 0, 0,   -- ALLOC_QTY, ALLOC_ROUND, SKIP_FLAG, ROUND_ALLOC
                'PENDING', NULL  -- ALLOC_STATUS, SKIP_REASON
            FROM [{final_table}] W
            INNER JOIN [ARS_MSA_VAR_ART] V WITH (NOLOCK)
                ON  W.[MAJ_CAT] = LTRIM(RTRIM(CAST(V.[MAJ_CAT] AS NVARCHAR(200))))
                AND W.[GEN_ART_NUMBER] = TRY_CAST(TRY_CAST(V.[GEN_ART_NUMBER] AS FLOAT) AS BIGINT)
                AND W.[CLR] = LTRIM(RTRIM(CAST(V.[CLR] AS NVARCHAR(200))))
                AND LTRIM(RTRIM(CAST(W.[RDC] AS NVARCHAR(50)))) = LTRIM(RTRIM(CAST(V.[RDC] AS NVARCHAR(50))))
            WHERE W.[ALLOC_FLAG] = 1
              AND W.[ALLOC_STATUS] = 'PENDING'
              AND TRY_CAST(V.[FNL_Q] AS FLOAT) > 0
              AND NOT EXISTS (
                  SELECT 1 FROM [{alloc_table}] A
                  WHERE A.[WERKS] = W.[WERKS] AND A.[MAJ_CAT] = W.[MAJ_CAT]
                    AND A.[GEN_ART_NUMBER] = W.[GEN_ART_NUMBER] AND A.[CLR] = W.[CLR]
                    AND A.[VAR_ART] = V.[ARTICLE_NUMBER] AND A.[SZ] = V.[SZ]
              )
        """)

        # ── Enrich new rows (new_only=True protects existing rows) ────
        _enrich_variant_stock(conn, alloc_table, "ARS_GRID_MJ_VAR_ART")
        _enrich_size_cont(conn, alloc_table, "Master_CONT_SZ")
        _calc_sz_mbq_req(conn, alloc_table, new_only=True)

        # ── Add pools for new OPTs only ───────────────────────────────
        _run(conn, f"""
            INSERT INTO {POOL_TABLE}
                ([RDC],[MAJ_CAT],[GEN_ART_NUMBER],[CLR],[VAR_ART],[SZ],[FNL_Q_ORIG],[FNL_Q_REM])
            SELECT A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER], A.[CLR], A.[VAR_ART], A.[SZ],
                   MAX(ISNULL(A.[FNL_Q], 0)), MAX(ISNULL(A.[FNL_Q], 0))
            FROM [{alloc_table}] A
            WHERE A.[ALLOC_QTY] = 0 AND A.[SKIP_FLAG] = 0
              AND ISNULL(A.[ALLOC_ROUND], 0) = 0
              AND NOT EXISTS (
                  SELECT 1 FROM {POOL_TABLE} P
                  WHERE P.[RDC] = A.[RDC] AND P.[MAJ_CAT] = A.[MAJ_CAT]
                    AND P.[GEN_ART_NUMBER] = A.[GEN_ART_NUMBER] AND P.[CLR] = A.[CLR]
                    AND P.[VAR_ART] = A.[VAR_ART] AND P.[SZ] = A.[SZ]
              )
            GROUP BY A.[RDC], A.[MAJ_CAT], A.[GEN_ART_NUMBER], A.[CLR], A.[VAR_ART], A.[SZ]
        """)

        # ── Run allocation for ONLY newly eligible OPTs ───────────────
        fb_primary = _run_primary(
            conn, alloc_table, final_table, threshold, only_new=True
        )
        stats["allocated"] += fb_primary.get("allocated", 0)

    # ── Restore ONLY the grids WE demoted (not originally Secondary) ──
    for gname in demoted_grids:
        _run(conn, """
            UPDATE [ARS_GRID_BUILDER] SET grid_group = 'Primary'
            WHERE grid_name = :gn
        """, {"gn": gname})

    if demoted_grids:
        logger.info(f"Fallback: restored {len(demoted_grids)} demoted grids to Primary")

    return stats


def _recalc_alloc_flag(conn, final_table):
    """Recalculate PRI_CT% and ALLOC_FLAG after grid demotion."""
    import json
    if not _exists(conn, "ARS_GRID_BUILDER") or not _exists(conn, "ARS_GRID_HIERARCHY"):
        return

    work_cols_upper = {c.upper() for c in _get_cols(conn, final_table)}
    _SKIP_ART = {"GEN_ART_NUMBER", "ARTICLE_NUMBER", "GEN_ART", "VAR_ART"}

    pri_h, pri_gh = [], []
    gb_rows = conn.execute(text(
        "SELECT grid_name, hierarchy_columns, ISNULL(grid_group, 'None') "
        "FROM [ARS_GRID_BUILDER] WHERE UPPER(status)='ACTIVE' ORDER BY seq"
    )).fetchall()

    mj_group = next((gg for gn, _, gg in gb_rows if gn.upper() == "MJ"), "Primary")
    if mj_group == "Primary":
        if "GH_MJ" in work_cols_upper:
            pri_gh.append("GH_MJ")
        if "H_MJ" in work_cols_upper:
            pri_h.append("H_MJ")

    for gn, hj, gg in gb_rows:
        try:
            h = json.loads(hj) if isinstance(hj, str) else hj
        except Exception:
            continue
        if not h or len(h) < 2:
            continue
        if any(x.upper() in _SKIP_ART for x in h):
            continue
        last = h[-1].upper()
        if last in ("WERKS", "MAJ_CAT"):
            continue
        if gg == "Primary":
            gh_col = f"GH_{last}"
            h_col = f"H_{last}"
            if gh_col in work_cols_upper:
                pri_gh.append(gh_col)
            if h_col in work_cols_upper:
                pri_h.append(h_col)

    if pri_h and pri_gh:
        h_sum = " + ".join(f"ISNULL([{c}], 0)" for c in pri_h)
        gh_sum = " + ".join(f"ISNULL([{c}], 0)" for c in pri_gh)
        _run(conn, f"""
            UPDATE [{final_table}] SET
                [PRI_CT%] = CASE WHEN ({gh_sum}) = 0 THEN 0
                    ELSE ROUND(CAST(({h_sum}) AS FLOAT) / ({gh_sum}) * 100, 1) END,
                [ALLOC_FLAG] = CASE WHEN ({gh_sum}) = 0 THEN 0
                    WHEN ROUND(CAST(({h_sum}) AS FLOAT) / ({gh_sum}) * 100, 1) >= 100 THEN 1
                    ELSE 0 END
        """)
    else:
        _run(conn, f"UPDATE [{final_table}] SET [PRI_CT%] = 100, [ALLOC_FLAG] = 1")

    logger.info(f"Fallback: recalculated PRI_CT%/ALLOC_FLAG (pri_h={len(pri_h)}, pri_gh={len(pri_gh)})")


# ═══════════════════════════════════════════════════════════════════════
#  STEP 8: REFLECT TO WORKING TABLE + FINAL STATUS
# ═══════════════════════════════════════════════════════════════════════

def _reflect_to_working(conn, final_table, alloc_table):
    """
    Sum ALLOC_QTY per OPT back to ARS_LISTING_WORKING.
    Set final ALLOC_STATUS based on allocation outcome.
    """
    # ── Add ALLOC_QTY column to listing_working ───────────────────────
    try:
        _run(conn, f"ALTER TABLE [{final_table}] ADD [ALLOC_QTY] FLOAT NULL")
    except Exception:
        pass

    # ── Reflect: SUM(ALLOC_QTY) per store+OPT ────────────────────────
    _run(conn, f"""
        UPDATE W SET W.[ALLOC_QTY] = A.[TOT_ALLOC]
        FROM [{final_table}] W
        INNER JOIN (
            SELECT [WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR],
                   SUM(ISNULL([ALLOC_QTY], 0)) AS TOT_ALLOC
            FROM [{alloc_table}]
            WHERE ISNULL([ALLOC_QTY], 0) > 0
            GROUP BY [WERKS], [RDC], [MAJ_CAT], [GEN_ART_NUMBER], [CLR]
        ) A
            ON  W.[WERKS] = A.[WERKS] AND W.[RDC] = A.[RDC]
            AND W.[MAJ_CAT] = A.[MAJ_CAT] AND W.[GEN_ART_NUMBER] = A.[GEN_ART_NUMBER]
            AND W.[CLR] = A.[CLR]
    """)

    # ── Set final ALLOC_STATUS on listing_working ─────────────────────
    _run(conn, f"""
        UPDATE [{final_table}] SET
            [ALLOC_STATUS] = CASE
                WHEN [ALLOC_STATUS] = 'INELIGIBLE' THEN 'INELIGIBLE'
                WHEN ISNULL([ALLOC_QTY], 0) > 0
                     AND ISNULL([ALLOC_QTY], 0) >= ISNULL([OPT_MBQ], 0)
                    THEN 'ALLOCATED'
                WHEN ISNULL([ALLOC_QTY], 0) > 0
                    THEN 'PARTIAL'
                WHEN [ALLOC_STATUS] = 'PENDING'
                    THEN 'NOT_PROCESSED'
                ELSE [ALLOC_STATUS]
            END
    """)

    # ── Set final ALLOC_STATUS on alloc_working ───────────────────────
    _run(conn, f"""
        UPDATE [{alloc_table}] SET
            [ALLOC_STATUS] = CASE
                WHEN [ALLOC_STATUS] IN ('SKIPPED', 'INELIGIBLE') THEN [ALLOC_STATUS]
                WHEN ISNULL([ALLOC_QTY], 0) > 0 THEN 'ALLOCATED'
                WHEN [SKIP_FLAG] = 1 THEN 'SKIPPED'
                ELSE 'PENDING'
            END
    """)

    logger.info(f"ALLOC_QTY + ALLOC_STATUS reflected to {final_table}")
