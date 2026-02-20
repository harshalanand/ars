"""
Audit Log Viewer API
"""
from datetime import datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.schemas.common import APIResponse
from app.security.dependencies import RequirePermissions, get_current_user
from app.models.rbac import User
from app.models.audit import AuditLog

router = APIRouter(prefix="/audit", tags=["Audit Logs"])


@router.get(
    "/",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_AUDIT_READ"]))],
)
async def list_audit_logs(
    table_name: str = Query(None),
    action_type: str = Query(None),
    changed_by: str = Query(None),
    batch_id: str = Query(None),
    date_from: datetime = Query(None),
    date_to: datetime = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    """Query audit logs with filters."""
    query = db.query(AuditLog)

    if table_name:
        query = query.filter(AuditLog.table_name == table_name)
    if action_type:
        query = query.filter(AuditLog.action_type == action_type)
    if changed_by:
        query = query.filter(AuditLog.changed_by == changed_by)
    if batch_id:
        query = query.filter(AuditLog.batch_id == batch_id)
    if date_from:
        query = query.filter(AuditLog.changed_at >= date_from)
    if date_to:
        query = query.filter(AuditLog.changed_at <= date_to)

    total = query.count()
    logs = (
        query.order_by(AuditLog.changed_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return APIResponse(data={
        "logs": [
            {
                "id": log.id,
                "table_name": log.table_name,
                "action_type": log.action_type,
                "record_primary_key": log.record_primary_key,
                "changed_columns": log.changed_columns,
                "changed_by": log.changed_by,
                "changed_at": log.changed_at.isoformat() if log.changed_at else None,
                "source": log.source,
                "ip_address": log.ip_address,
                "batch_id": log.batch_id,
                "row_count": log.row_count,
                "notes": log.notes,
            }
            for log in logs
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    })


@router.get(
    "/{log_id}",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_AUDIT_READ"]))],
)
async def get_audit_detail(log_id: int, db: Session = Depends(get_db)):
    """Get full audit log entry with old/new data."""
    log = db.query(AuditLog).filter(AuditLog.id == log_id).first()
    if not log:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Audit log not found")

    return APIResponse(data={
        "id": log.id,
        "table_name": log.table_name,
        "action_type": log.action_type,
        "record_primary_key": log.record_primary_key,
        "old_data": log.old_data,
        "new_data": log.new_data,
        "changed_columns": log.changed_columns,
        "changed_by": log.changed_by,
        "changed_at": log.changed_at.isoformat() if log.changed_at else None,
        "source": log.source,
        "ip_address": log.ip_address,
        "user_agent": log.user_agent,
        "session_id": log.session_id,
        "batch_id": log.batch_id,
        "duration_ms": log.duration_ms,
        "row_count": log.row_count,
        "notes": log.notes,
    })
