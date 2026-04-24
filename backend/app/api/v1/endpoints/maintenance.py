"""
Maintenance Endpoints
=====================
Superadmin-only API for TempDB monitoring, manual cleanup, aggressive
reclaim, trend history, and session diagnostics.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from app.security.dependencies import get_current_user
from app.services.tempdb_cleanup_service import tempdb_cleaner
from app.database.session import get_data_engine

router = APIRouter(prefix="/maintenance", tags=["Maintenance"])


def _require_superadmin(current_user=Depends(get_current_user)):
    """Restrict access to superadmin accounts only."""
    role_codes = set(getattr(current_user, "role_codes", []) or [])
    if "SUPER_ADMIN" not in role_codes:
        raise HTTPException(status_code=403, detail="Superadmin access required")
    return current_user


@router.get("/tempdb/status", summary="TempDB cleanup service status")
def get_tempdb_cleanup_status(_user=Depends(_require_superadmin)):
    """Return the cleanup service configuration and stats from the last run."""
    return tempdb_cleaner.status


@router.get("/tempdb/history", summary="Recent TempDB cleanup runs (for trend chart)")
def get_tempdb_history(_user=Depends(_require_superadmin)):
    """Return the in-memory history of recent cleanup runs."""
    return {"history": tempdb_cleaner.history}


@router.post("/tempdb/cleanup", summary="Trigger TempDB cleanup now")
def trigger_tempdb_cleanup(
    dry_run: bool = Query(False, description="Preview orphaned tables without dropping them"),
    _user=Depends(_require_superadmin),
):
    """
    Trigger a TempDB cleanup immediately (blocking).
    Use dry_run=true to see what would be dropped without making any changes.
    """
    logger.info(f"Manual TempDB cleanup triggered by admin (dry_run={dry_run})")
    try:
        stats = tempdb_cleaner.run_now(dry_run=dry_run)
        return {"success": True, "stats": stats}
    except Exception as exc:
        logger.error(f"Manual TempDB cleanup failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/tempdb/aggressive-shrink", summary="Force aggressive shrink now")
def trigger_tempdb_aggressive_shrink(_user=Depends(_require_superadmin)):
    """
    Force an aggressive reclaim regardless of current size:
    flush procedure/system caches then hard SHRINKFILE every tempdb data
    file to the configured target size.
    Use when the periodic TRUNCATEONLY is not releasing enough space.
    """
    logger.warning("Manual aggressive TempDB shrink triggered by admin")
    try:
        stats = tempdb_cleaner.aggressive_shrink_now()
        return {"success": True, "stats": stats}
    except Exception as exc:
        logger.error(f"Manual aggressive TempDB shrink failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/tempdb/size", summary="Current TempDB file sizes + usage breakdown")
def get_tempdb_size(_user=Depends(_require_superadmin)):
    """
    Return per-file allocation and a tempdb-wide usage breakdown from
    sys.dm_db_file_space_usage (user objects, internal, version store, free).
    FILEPROPERTY requires tempdb context, so we USE tempdb on a dedicated
    autocommit connection and invalidate it after.
    """
    engine = get_data_engine()
    shrink_fairy = None
    try:
        shrink_fairy = engine.raw_connection()
        pyodbc_conn = shrink_fairy.driver_connection
        pyodbc_conn.autocommit = True
        cursor = pyodbc_conn.cursor()
        cursor.execute("USE tempdb")

        # Per-file allocation
        cursor.execute("""
            SELECT
                f.name                                           AS file_name,
                f.type_desc                                      AS file_type,
                f.size * 8.0 / 1024                             AS allocated_mb,
                FILEPROPERTY(f.name, 'SpaceUsed') * 8.0 / 1024  AS used_mb,
                (f.size - FILEPROPERTY(f.name, 'SpaceUsed')) * 8.0 / 1024 AS free_mb
            FROM tempdb.sys.database_files f
            WHERE f.type_desc IN ('ROWS', 'LOG');
        """)
        cols = [c[0] for c in cursor.description]
        files = [dict(zip(cols, row)) for row in cursor.fetchall()]

        # DB-wide breakdown (MB) — tells you where the space went
        cursor.execute("""
            SELECT
                SUM(user_object_reserved_page_count)     * 8.0 / 1024 AS user_objects_mb,
                SUM(internal_object_reserved_page_count) * 8.0 / 1024 AS internal_objects_mb,
                SUM(version_store_reserved_page_count)   * 8.0 / 1024 AS version_store_mb,
                SUM(mixed_extent_page_count)             * 8.0 / 1024 AS mixed_extent_mb,
                SUM(unallocated_extent_page_count)       * 8.0 / 1024 AS unallocated_mb
            FROM sys.dm_db_file_space_usage;
        """)
        row = cursor.fetchone()
        breakdown = {
            "user_objects_mb":      float(row[0] or 0),
            "internal_objects_mb":  float(row[1] or 0),
            "version_store_mb":     float(row[2] or 0),
            "mixed_extent_mb":      float(row[3] or 0),
            "unallocated_mb":       float(row[4] or 0),
        }

        return {"files": files, "breakdown": breakdown}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        # Don't leak the "USE tempdb" context back to the pool.
        if shrink_fairy:
            try:
                shrink_fairy.invalidate()
            except Exception:
                pass


@router.get("/tempdb/sessions", summary="Top tempdb-consuming sessions")
def get_tempdb_sessions(_user=Depends(_require_superadmin)):
    """Top 10 live sessions ranked by tempdb pages allocated — for diagnostics."""
    try:
        return {"sessions": tempdb_cleaner.top_sessions()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/tempdb/alert/clear", summary="Clear the current TempDB alert")
def clear_tempdb_alert(_user=Depends(_require_superadmin)):
    """Dismiss the current ALERT banner. Auto-re-raises if size stays over threshold."""
    tempdb_cleaner.clear_alert()
    return {"success": True}


@router.get("/tempdb/long-transactions", summary="Open transactions pinning tempdb space")
def get_long_transactions(_user=Depends(_require_superadmin)):
    """
    Return open transactions against tempdb (database_id = 2) or any DB,
    oldest first. A long-running transaction is the usual cause of a
    bloated version_store and stuck SHRINKFILE.

    Returns rows from sys.dm_tran_database_transactions joined with
    sys.dm_exec_sessions + sys.dm_exec_requests.
    """
    engine = get_data_engine()
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("""
            SELECT TOP 20
                s.session_id,
                ISNULL(r.status, 'sleeping')                      AS status,
                ISNULL(s.login_name, '')                          AS login_name,
                ISNULL(s.host_name, '')                           AS host_name,
                ISNULL(s.program_name, '')                        AS program_name,
                DB_NAME(dt.database_id)                           AS database_name,
                dt.database_id,
                DATEDIFF(MINUTE, dt.database_transaction_begin_time, GETDATE())
                                                                   AS mins_open,
                dt.database_transaction_begin_time                AS begin_time,
                dt.database_transaction_log_bytes_used / 1024.0 / 1024.0
                                                                   AS log_mb,
                dt.database_transaction_log_record_count          AS log_records,
                s.last_request_start_time                         AS last_request_start,
                ISNULL(r.command, '')                             AS command,
                ISNULL(r.wait_type, '')                           AS wait_type
            FROM sys.dm_tran_database_transactions dt
            INNER JOIN sys.dm_tran_session_transactions st
                   ON dt.transaction_id = st.transaction_id
            INNER JOIN sys.dm_exec_sessions s
                   ON st.session_id = s.session_id
            LEFT  JOIN sys.dm_exec_requests r
                   ON s.session_id = r.session_id
            WHERE s.session_id > 50
            ORDER BY dt.database_transaction_begin_time ASC;
        """)
        cols = [c[0] for c in cursor.description]
        rows = []
        for row in cursor.fetchall():
            d = dict(zip(cols, row))
            for k in ("begin_time", "last_request_start"):
                if d.get(k):
                    d[k] = d[k].isoformat()
            rows.append(d)
        return {"transactions": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        raw_conn.close()


@router.post("/tempdb/kill-session/{session_id}", summary="KILL a SQL session")
def kill_session(session_id: int, _user=Depends(_require_superadmin)):
    """
    Issue a KILL against the given SQL Server session_id. Use this when a
    long-running transaction is pinning tempdb space and you cannot wait
    for it to finish. Only sessions with session_id > 50 (user sessions)
    can be killed; system sessions are rejected.
    """
    if session_id <= 50:
        raise HTTPException(
            status_code=400,
            detail="Refusing to kill system session (session_id <= 50)",
        )

    engine = get_data_engine()
    shrink_fairy = None
    try:
        shrink_fairy = engine.raw_connection()
        pyodbc_conn = shrink_fairy.driver_connection
        pyodbc_conn.autocommit = True  # KILL cannot run in a user transaction
        cursor = pyodbc_conn.cursor()
        logger.warning(f"KILL {session_id} issued by admin")
        cursor.execute(f"KILL {int(session_id)}")
        return {"success": True, "session_id": session_id}
    except Exception as exc:
        logger.error(f"KILL {session_id} failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if shrink_fairy:
            try:
                shrink_fairy.invalidate()
            except Exception:
                pass
