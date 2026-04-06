"""
Maintenance Endpoints
=====================
Superadmin-only API for TempDB monitoring and manual cleanup.
Follows the same router/dependency pattern as users.py and roles.py.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger

from app.security.dependencies import get_current_user
from app.services.tempdb_cleanup_service import tempdb_cleaner
from app.database.session import get_data_engine

router = APIRouter(prefix="/maintenance", tags=["Maintenance"])


def _require_superadmin(current_user=Depends(get_current_user)):
    """Restrict access to superadmin accounts only."""
    if not getattr(current_user, "is_superadmin", False):
        raise HTTPException(status_code=403, detail="Superadmin access required")
    return current_user


@router.get("/tempdb/status", summary="TempDB cleanup service status")
def get_tempdb_cleanup_status(_user=Depends(_require_superadmin)):
    """Return the cleanup service configuration and stats from the last run."""
    return tempdb_cleaner.status


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


@router.get("/tempdb/size", summary="Current TempDB file sizes")
def get_tempdb_size(_user=Depends(_require_superadmin)):
    """Return allocated vs used size in MB for every tempdb data file."""
    engine = get_data_engine()
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        cursor.execute("""
            SELECT
                f.name                                           AS file_name,
                f.size * 8.0 / 1024                             AS allocated_mb,
                FILEPROPERTY(f.name, 'SpaceUsed') * 8.0 / 1024  AS used_mb,
                (f.size - FILEPROPERTY(f.name, 'SpaceUsed')) * 8.0 / 1024 AS free_mb
            FROM tempdb.sys.database_files f
            WHERE f.type_desc = 'ROWS';
        """)
        cols = [c[0] for c in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return {"files": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        raw_conn.close()
