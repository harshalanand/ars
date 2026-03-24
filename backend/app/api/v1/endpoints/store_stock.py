"""
Store Stock - SLOC Settings API
================================
Manages KPI labels and Active/Inactive flags for each distinct SLOC
value found in the ET_STORE_STOCK table (Data DB).

Settings are stored in ARS_SLOC_SETTINGS (System DB).

Endpoints:
  GET  /store-stock/sloc-settings        – list all SLOCs with their settings
  POST /store-stock/sync                 – sync new SLOC values from ET_STORE_STOCK
  PUT  /store-stock/sloc-settings/{sloc} – update KPI / active flag for one SLOC
  PUT  /store-stock/sloc-settings/bulk   – bulk-update many SLOCs at once
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database.session import get_db, get_data_engine, get_system_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/store-stock", tags=["Store Stock"])


# ============================================================================
# Pydantic Schemas
# ============================================================================

class SlocSetting(BaseModel):
    sloc: str
    kpi: Optional[str] = None
    is_active: bool = True


class SlocSettingResponse(BaseModel):
    id: int
    sloc: str
    kpi: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    is_new: bool = False          # True when just synced but not yet saved


class BulkUpdateItem(BaseModel):
    sloc: str
    kpi: Optional[str] = None
    is_active: bool = True


class BulkUpdateRequest(BaseModel):
    items: List[BulkUpdateItem]


# ============================================================================
# Helpers
# ============================================================================

def _ensure_table(system_engine):
    """Create ARS_SLOC_SETTINGS if it does not exist (auto-migration)."""
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = 'ARS_SLOC_SETTINGS'
    )
    BEGIN
        CREATE TABLE ARS_SLOC_SETTINGS (
            id         INT IDENTITY(1,1) PRIMARY KEY,
            sloc       NVARCHAR(50)  NOT NULL UNIQUE,
            kpi        NVARCHAR(200) NULL,
            is_active  BIT           NOT NULL DEFAULT 1,
            created_at DATETIME      NOT NULL DEFAULT GETDATE(),
            updated_at DATETIME      NOT NULL DEFAULT GETDATE()
        );
        CREATE INDEX IX_ARS_SLOC_SETTINGS_sloc ON ARS_SLOC_SETTINGS(sloc);
    END
    """
    with system_engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()


def _fetch_distinct_slocs(data_engine) -> List[str]:
    """Return sorted distinct SLOC values from ET_STORE_STOCK."""
    sql = "SELECT DISTINCT sloc AS qty FROM ET_STORE_STOCK ORDER BY sloc ASC"
    with data_engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [str(r[0]) for r in rows if r[0] is not None]


def _fetch_saved_settings(system_engine) -> dict:
    """Return saved settings keyed by sloc."""
    sql = "SELECT id, sloc, kpi, is_active, created_at, updated_at FROM ARS_SLOC_SETTINGS"
    with system_engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return {
        str(r[1]): {
            "id": r[0],
            "sloc": str(r[1]),
            "kpi": r[2],
            "is_active": bool(r[3]),
            "created_at": r[4],
            "updated_at": r[5],
        }
        for r in rows
    }


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/sloc-settings", response_model=APIResponse)
def get_sloc_settings(
    current_user: User = Depends(get_current_user),
):
    """
    Return all distinct SLOCs from ET_STORE_STOCK merged with their saved
    KPI / Active settings.  New (unsaved) SLOCs are flagged is_new=True.
    """
    system_engine = get_system_engine()
    data_engine   = get_data_engine()

    _ensure_table(system_engine)

    try:
        distinct_slocs = _fetch_distinct_slocs(data_engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    saved = _fetch_saved_settings(system_engine)

    result = []
    for sloc in distinct_slocs:
        if sloc in saved:
            entry = dict(saved[sloc])
            entry["is_new"] = False
        else:
            entry = {
                "id": None,
                "sloc": sloc,
                "kpi": None,
                "is_active": True,
                "created_at": None,
                "updated_at": None,
                "is_new": True,
            }
        result.append(entry)

    return APIResponse(
        success=True,
        message=f"Loaded {len(result)} SLOC entries ({sum(1 for r in result if r['is_new'])} new)",
        data={"items": result, "total": len(result)},
    )


@router.post("/sync", response_model=APIResponse)
def sync_slocs(
    current_user: User = Depends(get_current_user),
):
    """
    Detect new SLOC values in ET_STORE_STOCK that are not yet in
    ARS_SLOC_SETTINGS and insert them with default values (kpi=NULL, is_active=1).
    """
    system_engine = get_system_engine()
    data_engine   = get_data_engine()

    _ensure_table(system_engine)

    try:
        distinct_slocs = _fetch_distinct_slocs(data_engine)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read ET_STORE_STOCK: {e}")

    saved = _fetch_saved_settings(system_engine)
    new_slocs = [s for s in distinct_slocs if s not in saved]

    if new_slocs:
        insert_sql = text("""
            INSERT INTO ARS_SLOC_SETTINGS (sloc, kpi, is_active, created_at, updated_at)
            VALUES (:sloc, NULL, 1, GETDATE(), GETDATE())
        """)
        with system_engine.connect() as conn:
            for sloc in new_slocs:
                conn.execute(insert_sql, {"sloc": sloc})
            conn.commit()

    return APIResponse(
        success=True,
        message=f"Sync complete. {len(new_slocs)} new SLOC(s) added.",
        data={"new_count": len(new_slocs), "new_slocs": new_slocs},
    )


@router.put("/sloc-settings/{sloc}", response_model=APIResponse)
def update_sloc_setting(
    sloc: str,
    payload: SlocSetting,
    current_user: User = Depends(get_current_user),
):
    """Update KPI label and Active/Inactive flag for a single SLOC."""
    system_engine = get_system_engine()
    _ensure_table(system_engine)

    # Upsert: insert if not exists, update if exists
    upsert_sql = text("""
        IF EXISTS (SELECT 1 FROM ARS_SLOC_SETTINGS WHERE sloc = :sloc)
            UPDATE ARS_SLOC_SETTINGS
            SET kpi = :kpi, is_active = :is_active, updated_at = GETDATE()
            WHERE sloc = :sloc
        ELSE
            INSERT INTO ARS_SLOC_SETTINGS (sloc, kpi, is_active, created_at, updated_at)
            VALUES (:sloc, :kpi, :is_active, GETDATE(), GETDATE())
    """)

    with system_engine.connect() as conn:
        conn.execute(upsert_sql, {
            "sloc": sloc,
            "kpi": payload.kpi,
            "is_active": 1 if payload.is_active else 0,
        })
        conn.commit()

    return APIResponse(
        success=True,
        message=f"SLOC '{sloc}' updated successfully.",
        data={"sloc": sloc, "kpi": payload.kpi, "is_active": payload.is_active},
    )


@router.put("/sloc-settings", response_model=APIResponse)
def bulk_update_sloc_settings(
    payload: BulkUpdateRequest,
    current_user: User = Depends(get_current_user),
):
    """Bulk-update KPI and Active/Inactive for multiple SLOCs at once."""
    system_engine = get_system_engine()
    _ensure_table(system_engine)

    upsert_sql = text("""
        IF EXISTS (SELECT 1 FROM ARS_SLOC_SETTINGS WHERE sloc = :sloc)
            UPDATE ARS_SLOC_SETTINGS
            SET kpi = :kpi, is_active = :is_active, updated_at = GETDATE()
            WHERE sloc = :sloc
        ELSE
            INSERT INTO ARS_SLOC_SETTINGS (sloc, kpi, is_active, created_at, updated_at)
            VALUES (:sloc, :kpi, :is_active, GETDATE(), GETDATE())
    """)

    with system_engine.connect() as conn:
        for item in payload.items:
            conn.execute(upsert_sql, {
                "sloc": item.sloc,
                "kpi": item.kpi,
                "is_active": 1 if item.is_active else 0,
            })
        conn.commit()

    return APIResponse(
        success=True,
        message=f"{len(payload.items)} SLOC record(s) updated successfully.",
        data={"updated_count": len(payload.items)},
    )
