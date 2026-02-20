"""
Row-Level Security Management API
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.database.session import get_db
from app.schemas.auth import StoreAccessCreate, RegionAccessCreate, ColumnRestrictionCreate
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user, RequirePermissions
from app.models.rbac import User
from app.models.rls import UserStoreAccess, UserRegionAccess, ColumnRestriction, Store
from app.audit.service import AuditService

router = APIRouter(prefix="/rls", tags=["Row-Level Security"])


# ============================================================================
# Store Access
# ============================================================================

@router.post(
    "/store-access",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_RLS_MANAGE"]))],
)
async def assign_store_access(
    body: StoreAccessCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Assign store-level access to a user."""
    added = 0
    for code in body.store_codes:
        existing = db.query(UserStoreAccess).filter(
            UserStoreAccess.user_id == body.user_id,
            UserStoreAccess.store_code == code,
        ).first()
        if existing:
            existing.is_active = True
            existing.access_level = body.access_level
        else:
            db.add(UserStoreAccess(
                user_id=body.user_id, store_code=code,
                access_level=body.access_level, granted_by=current_user.username,
            ))
            added += 1
    db.commit()

    AuditService(db).log(
        table_name="rls_user_store_access", action_type="INSERT",
        changed_by=current_user.username,
        new_data={"user_id": body.user_id, "stores": body.store_codes},
        row_count=added,
    )
    db.commit()

    return APIResponse(message=f"Store access granted for {len(body.store_codes)} stores")


@router.get("/store-access/{user_id}", response_model=APIResponse)
async def get_user_store_access(
    user_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Get store access for a user."""
    records = db.query(UserStoreAccess).filter(
        UserStoreAccess.user_id == user_id, UserStoreAccess.is_active == True
    ).all()
    return APIResponse(data=[
        {"store_code": r.store_code, "access_level": r.access_level}
        for r in records
    ])


@router.delete(
    "/store-access/{user_id}/{store_code}",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_RLS_MANAGE"]))],
)
async def revoke_store_access(
    user_id: int,
    store_code: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Revoke store access from a user."""
    record = db.query(UserStoreAccess).filter(
        UserStoreAccess.user_id == user_id,
        UserStoreAccess.store_code == store_code,
    ).first()
    if record:
        record.is_active = False
        db.commit()
    return APIResponse(message="Store access revoked")


# ============================================================================
# Region Access
# ============================================================================

@router.post(
    "/region-access",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_RLS_MANAGE"]))],
)
async def assign_region_access(
    body: RegionAccessCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Assign region-level access to a user."""
    record = UserRegionAccess(
        user_id=body.user_id,
        region=body.region,
        hub=body.hub,
        division=body.division,
        business_unit=body.business_unit,
        access_level=body.access_level,
        granted_by=current_user.username,
    )
    db.add(record)
    db.commit()
    return APIResponse(message="Region access granted")


@router.get("/region-access/{user_id}", response_model=APIResponse)
async def get_user_region_access(
    user_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Get region access for a user."""
    records = db.query(UserRegionAccess).filter(
        UserRegionAccess.user_id == user_id, UserRegionAccess.is_active == True
    ).all()
    return APIResponse(data=[
        {
            "id": r.id, "region": r.region, "hub": r.hub,
            "division": r.division, "business_unit": r.business_unit,
            "access_level": r.access_level,
        }
        for r in records
    ])


# ============================================================================
# Column Restrictions
# ============================================================================

@router.post(
    "/column-restrictions",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_RLS_MANAGE"]))],
)
async def create_column_restriction(
    body: ColumnRestrictionCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create or update a column-level restriction."""
    existing = db.query(ColumnRestriction).filter(
        ColumnRestriction.table_name == body.table_name,
        ColumnRestriction.column_name == body.column_name,
        ColumnRestriction.role_id == body.role_id,
    ).first()

    if existing:
        existing.is_visible = body.is_visible
        existing.is_masked = body.is_masked
        existing.mask_pattern = body.mask_pattern
    else:
        db.add(ColumnRestriction(
            table_name=body.table_name, column_name=body.column_name,
            role_id=body.role_id, is_visible=body.is_visible,
            is_masked=body.is_masked, mask_pattern=body.mask_pattern,
        ))

    db.commit()
    return APIResponse(message="Column restriction saved")


@router.get("/column-restrictions/{table_name}", response_model=APIResponse)
async def get_column_restrictions(
    table_name: str,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Get column restrictions for a table."""
    records = db.query(ColumnRestriction).filter(
        ColumnRestriction.table_name == table_name
    ).all()
    return APIResponse(data=[
        {
            "column_name": r.column_name, "role_id": r.role_id,
            "is_visible": r.is_visible, "is_masked": r.is_masked,
            "mask_pattern": r.mask_pattern,
        }
        for r in records
    ])


# ============================================================================
# Stores (for RLS configuration)
# ============================================================================

@router.get("/stores", response_model=APIResponse)
async def list_stores(
    region: str = None,
    division: str = None,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """List all stores (filterable by region/division)."""
    query = db.query(Store).filter(Store.is_active == True)
    if region:
        query = query.filter(Store.region == region)
    if division:
        query = query.filter(Store.division == division)

    stores = query.order_by(Store.store_code).all()
    return APIResponse(data=[
        {
            "store_code": s.store_code, "store_name": s.store_name,
            "region": s.region, "hub": s.hub, "division": s.division,
            "store_grade": s.store_grade, "city": s.city, "state": s.state,
        }
        for s in stores
    ])
