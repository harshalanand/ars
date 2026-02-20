"""
Dynamic Table Management API Endpoints
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.schemas.table_mgmt import CreateTableRequest, AlterTableRequest
from app.schemas.common import APIResponse
from app.services.table_mgmt_service import TableManagementService
from app.security.dependencies import get_current_user, RequirePermissions
from app.models.rbac import User

router = APIRouter(prefix="/tables", tags=["Table Management"])


# ============================================================================
# Create Table
# ============================================================================

@router.post(
    "/",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["TABLE_CREATE"]))],
)
async def create_table(
    body: CreateTableRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new database table from UI."""
    try:
        service = TableManagementService(db)
        result = service.create_table(
            table_name=body.table_name,
            columns=[c.model_dump() for c in body.columns],
            display_name=body.display_name,
            description=body.description,
            module=body.module,
            created_by=current_user.username,
        )
        return APIResponse(data=result, message=f"Table '{body.table_name}' created successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Alter Table
# ============================================================================

@router.put(
    "/{table_name}/alter",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["TABLE_ALTER"]))],
)
async def alter_table(
    table_name: str,
    body: AlterTableRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Alter table: add/drop/rename columns."""
    try:
        service = TableManagementService(db)
        result = service.alter_table(
            table_name=table_name,
            add_columns=[c.model_dump() for c in body.add_columns] if body.add_columns else None,
            drop_columns=body.drop_columns,
            rename_columns=body.rename_columns,
            changed_by=current_user.username,
        )
        return APIResponse(data=result, message="Table altered successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Soft Delete Table
# ============================================================================

@router.delete(
    "/{table_name}",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["TABLE_DELETE"]))],
)
async def delete_table(
    table_name: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft-delete a table (marks inactive, does NOT drop from DB)."""
    try:
        service = TableManagementService(db)
        result = service.soft_delete_table(table_name, deleted_by=current_user.username)
        return APIResponse(data=result, message="Table soft-deleted")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Table Metadata & Schema
# ============================================================================

@router.get("/{table_name}/schema", response_model=APIResponse)
async def get_table_schema(
    table_name: str,
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Get table schema and metadata."""
    try:
        service = TableManagementService(db)
        result = service.get_table_metadata(table_name)
        return APIResponse(data=result)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/", response_model=APIResponse)
async def list_tables(
    module: str = Query(None),
    include_system: bool = Query(False),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """List registered tables."""
    service = TableManagementService(db)
    tables = service.list_tables(module=module, include_system=include_system)
    return APIResponse(data=tables)


@router.get("/database/all", response_model=APIResponse)
async def list_all_database_tables(
    db: Session = Depends(get_db),
    _: User = Depends(RequirePermissions(["TABLE_READ"])),
):
    """List all tables from SQL Server (not just registered)."""
    service = TableManagementService(db)
    tables = service.list_all_database_tables()
    return APIResponse(data=tables)


# ============================================================================
# Table Data Operations
# ============================================================================

@router.get("/{table_name}/data", response_model=APIResponse)
async def query_table_data(
    table_name: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=5000),
    order_by: str = Query(None),
    order_dir: str = Query("ASC"),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    """Query paginated data from any table (for data grid)."""
    try:
        service = TableManagementService(db)
        result = service.query_table_data(
            table_name=table_name,
            page=page,
            page_size=page_size,
            order_by=order_by,
            order_dir=order_dir,
        )
        return APIResponse(data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete(
    "/{table_name}/data",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["TABLE_DELETE"]))],
)
async def truncate_table_data(
    table_name: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Delete all data from a table (does NOT drop the table)."""
    try:
        service = TableManagementService(db)
        result = service.truncate_table_data(table_name, deleted_by=current_user.username)
        return APIResponse(data=result, message="Table data deleted")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
