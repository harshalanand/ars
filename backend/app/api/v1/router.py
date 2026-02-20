"""
API v1 Router - Aggregates all endpoint routers
"""
from fastapi import APIRouter

# Phase 1: Auth, RBAC, RLS, Audit
from app.api.v1.endpoints.auth import router as auth_router
from app.api.v1.endpoints.users import router as users_router
from app.api.v1.endpoints.roles import router as roles_router
from app.api.v1.endpoints.rls import router as rls_router
from app.api.v1.endpoints.audit import router as audit_router

# Phase 2: Table Management, Upsert, Upload
from app.api.v1.endpoints.tables import router as tables_router
from app.api.v1.endpoints.data_ops import router as data_ops_router
from app.api.v1.endpoints.upload import router as upload_router

# Phase 3: Allocation Engine
from app.api.v1.endpoints.allocations import router as allocations_router

api_router = APIRouter(prefix="/api/v1")

# Phase 1
api_router.include_router(auth_router)
api_router.include_router(users_router)
api_router.include_router(roles_router)
api_router.include_router(rls_router)
api_router.include_router(audit_router)

# Phase 2
api_router.include_router(tables_router)
api_router.include_router(data_ops_router)
api_router.include_router(upload_router)

# Phase 3
api_router.include_router(allocations_router)
