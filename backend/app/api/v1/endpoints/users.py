"""
User Management API Endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.schemas.auth import UserCreate, UserUpdate, UserResponse, UserListResponse
from app.schemas.common import APIResponse
from app.services.auth_service import AuthService
from app.security.dependencies import get_current_user, RequirePermissions
from app.models.rbac import User

router = APIRouter(prefix="/users", tags=["User Management"])


@router.post(
    "/",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_USERS_CREATE"]))],
)
async def create_user(
    body: UserCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new user (requires ADMIN_USERS_CREATE permission)."""
    try:
        service = AuthService(db)
        user = service.create_user(body, created_by=current_user.username)
        return APIResponse(data=user.model_dump(), message="User created successfully")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.get(
    "/",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_USERS_READ"]))],
)
async def list_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: str = Query(None),
    db: Session = Depends(get_db),
):
    """List all users with pagination and search."""
    service = AuthService(db)
    result = service.list_users(page=page, page_size=page_size, search=search)
    return APIResponse(data=result)


@router.get(
    "/{user_id}",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_USERS_READ"]))],
)
async def get_user(user_id: int, db: Session = Depends(get_db)):
    """Get user by ID."""
    service = AuthService(db)
    user = service.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return APIResponse(data=user.model_dump())


@router.put(
    "/{user_id}",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_USERS_UPDATE"]))],
)
async def update_user(
    user_id: int,
    body: UserUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update user details."""
    try:
        service = AuthService(db)
        user = service.update_user(user_id, body, updated_by=current_user.username)
        return APIResponse(data=user.model_dump(), message="User updated successfully")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post(
    "/{user_id}/unlock",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["ADMIN_USERS_UPDATE"]))],
)
async def unlock_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Unlock a locked user account."""
    try:
        service = AuthService(db)
        service.unlock_user(user_id, unlocked_by=current_user.username)
        return APIResponse(message="User unlocked successfully")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
