"""
FastAPI Security Dependencies: Authentication, RBAC, RLS
"""
from typing import List, Optional, Set
from functools import wraps

from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.security.jwt_handler import verify_access_token
from app.models.rbac import User, UserRole
from app.models.rls import UserStoreAccess, UserRegionAccess, Store

bearer_scheme = HTTPBearer()


# ============================================================================
# Get Current User
# ============================================================================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db)
) -> User:
    """Extract and validate current user from JWT token."""
    token = credentials.credentials
    payload = verify_access_token(token)

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    username: str = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )

    user = db.query(User).filter(User.username == username, User.is_active == True).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    if user.is_locked:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is locked",
        )

    return user


async def get_current_active_user(
    current_user: User = Depends(get_current_user),
) -> User:
    """Ensure user is active."""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user",
        )
    return current_user


# ============================================================================
# RBAC: Role Check Dependency
# ============================================================================

class RequireRoles:
    """Dependency that checks if user has at least one of the required roles."""

    def __init__(self, allowed_roles: List[str]):
        self.allowed_roles = allowed_roles

    async def __call__(self, current_user: User = Depends(get_current_user)) -> User:
        user_roles = set(current_user.role_codes)

        # Super Admin bypasses all role checks
        if "SUPER_ADMIN" in user_roles:
            return current_user

        if not user_roles.intersection(set(self.allowed_roles)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role. Required: {self.allowed_roles}",
            )
        return current_user


class RequirePermissions:
    """Dependency that checks if user has ALL required permissions."""

    def __init__(self, required_permissions: List[str]):
        self.required_permissions = required_permissions

    async def __call__(self, current_user: User = Depends(get_current_user)) -> User:
        user_roles = set(current_user.role_codes)

        # Super Admin bypasses all permission checks
        if "SUPER_ADMIN" in user_roles:
            return current_user

        user_perms = current_user.permissions
        missing = set(self.required_permissions) - user_perms

        if missing:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing permissions: {list(missing)}",
            )
        return current_user


# ============================================================================
# RLS: Row-Level Security Filter
# ============================================================================

class RLSContext:
    """Contains the RLS filter context for the current user."""

    def __init__(self, user: User, accessible_stores: List[str], is_unrestricted: bool = False):
        self.user = user
        self.accessible_stores = accessible_stores
        self.is_unrestricted = is_unrestricted

    def filter_store_query(self, query, store_code_column):
        """Apply store-level RLS filter to a SQLAlchemy query."""
        if self.is_unrestricted:
            return query
        return query.filter(store_code_column.in_(self.accessible_stores))


async def get_rls_context(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> RLSContext:
    """Build RLS context: resolve which stores the user can access."""
    user_roles = set(current_user.role_codes)

    # Super Admin and Admin see everything
    if user_roles.intersection({"SUPER_ADMIN", "ADMIN"}):
        return RLSContext(user=current_user, accessible_stores=[], is_unrestricted=True)

    # HO Planner sees everything (configurable)
    if "PLANNER" in user_roles:
        # Check if planner has region-level restrictions
        region_access = (
            db.query(UserRegionAccess)
            .filter(UserRegionAccess.user_id == current_user.id, UserRegionAccess.is_active == True)
            .all()
        )
        if not region_access:
            # No region restriction = full access (HO Planner)
            return RLSContext(user=current_user, accessible_stores=[], is_unrestricted=True)

    # Collect stores from direct store access
    store_codes: Set[str] = set()

    direct_stores = (
        db.query(UserStoreAccess.store_code)
        .filter(UserStoreAccess.user_id == current_user.id, UserStoreAccess.is_active == True)
        .all()
    )
    store_codes.update(s[0] for s in direct_stores)

    # Collect stores from region access
    region_access_records = (
        db.query(UserRegionAccess)
        .filter(UserRegionAccess.user_id == current_user.id, UserRegionAccess.is_active == True)
        .all()
    )

    for ra in region_access_records:
        store_query = db.query(Store.store_code).filter(Store.is_active == True)
        if ra.region:
            store_query = store_query.filter(Store.region == ra.region)
        if ra.hub:
            store_query = store_query.filter(Store.hub == ra.hub)
        if ra.division:
            store_query = store_query.filter(Store.division == ra.division)
        if ra.business_unit:
            store_query = store_query.filter(Store.business_unit == ra.business_unit)

        region_stores = store_query.all()
        store_codes.update(s[0] for s in region_stores)

    if not store_codes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No store access configured. Contact admin.",
        )

    return RLSContext(
        user=current_user,
        accessible_stores=list(store_codes),
        is_unrestricted=False,
    )


# ============================================================================
# Column-Level Security Helper
# ============================================================================

def get_restricted_columns(db: Session, table_name: str, role_codes: List[str]) -> dict:
    """
    Return dict of {column_name: {visible, masked, mask_pattern}} for a table + roles.
    """
    from app.models.rls import ColumnRestriction
    from app.models.rbac import Role

    restrictions = (
        db.query(ColumnRestriction)
        .join(Role)
        .filter(
            ColumnRestriction.table_name == table_name,
            Role.role_code.in_(role_codes),
        )
        .all()
    )

    result = {}
    for r in restrictions:
        col = r.column_name
        # Most restrictive wins
        if col not in result:
            result[col] = {"visible": r.is_visible, "masked": r.is_masked, "mask_pattern": r.mask_pattern}
        else:
            if not r.is_visible:
                result[col]["visible"] = False
            if r.is_masked:
                result[col]["masked"] = True
                result[col]["mask_pattern"] = r.mask_pattern

    return result


def apply_column_security(data: dict, restrictions: dict) -> dict:
    """Apply column-level security to a data dict."""
    secured = {}
    for key, value in data.items():
        if key in restrictions:
            rule = restrictions[key]
            if not rule["visible"]:
                continue  # hide column entirely
            if rule["masked"]:
                secured[key] = rule.get("mask_pattern", "***")
                continue
        secured[key] = value
    return secured
