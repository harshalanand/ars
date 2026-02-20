"""
File Upload API Endpoints
- Bulk CSV/Excel upload → Upsert
- File preview (for column mapping UI)
- Sheet names extraction
"""
import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.schemas.common import APIResponse
from app.services.file_upload_service import FileUploadService
from app.security.dependencies import get_current_user, RequirePermissions
from app.models.rbac import User
from app.audit.service import get_client_ip

router = APIRouter(prefix="/upload", tags=["File Upload"])


# ============================================================================
# Bulk File Upload → Upsert
# ============================================================================

@router.post(
    "/",
    response_model=APIResponse,
    dependencies=[Depends(RequirePermissions(["DATA_UPLOAD"]))],
)
async def upload_file(
    request: Request,
    file: UploadFile = File(..., description="CSV or Excel file to upload"),
    table_name: str = Form(..., description="Target table name"),
    primary_key_columns: str = Form(..., description="Comma-separated PK column names"),
    column_mapping: Optional[str] = Form(None, description="JSON: {file_col: table_col}"),
    skip_rows: int = Form(0, description="Number of rows to skip from top"),
    sheet_name: Optional[str] = Form(None, description="Excel sheet name"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Upload a CSV or Excel file and upsert data into the target table.

    - If record exists (based on PKs) → update only changed fields
    - If record doesn't exist → insert new record
    - Supports 1M+ rows (chunked processing)
    - Audit logs all changes with batch_id

    Example:
    ```
    curl -X POST /api/v1/upload/ \
      -H "Authorization: Bearer <token>" \
      -F "file=@data.csv" \
      -F "table_name=store_stock" \
      -F "primary_key_columns=store_code,variant_code"
    ```
    """
    try:
        # Parse inputs
        pk_cols = [c.strip() for c in primary_key_columns.split(",")]

        col_map = None
        if column_mapping:
            try:
                col_map = json.loads(column_mapping)
            except json.JSONDecodeError:
                raise ValueError("Invalid column_mapping JSON")

        # Read file content
        content = await file.read()
        if not content:
            raise ValueError("File is empty")

        # Process
        service = FileUploadService(db)
        result = await service.process_upload(
            file_content=content,
            file_name=file.filename or "unknown",
            table_name=table_name,
            primary_key_columns=pk_cols,
            changed_by=current_user.username,
            ip_address=get_client_ip(request),
            column_mapping=col_map,
            skip_rows=skip_rows,
            sheet_name=sheet_name,
        )

        return APIResponse(
            data=result,
            message=(
                f"Upload complete: {result['inserted']} inserted, "
                f"{result['updated']} updated, {result['errors']} errors"
            ),
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# ============================================================================
# File Preview (for column mapping UI)
# ============================================================================

@router.post("/preview", response_model=APIResponse)
async def preview_file(
    file: UploadFile = File(...),
    rows: int = Form(20, description="Number of rows to preview"),
    skip_rows: int = Form(0),
    sheet_name: Optional[str] = Form(None),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Preview an uploaded file before processing.
    Returns column names, data types, sample values, and first N rows.
    Useful for building a column mapping UI.
    """
    try:
        content = await file.read()
        if not content:
            raise ValueError("File is empty")

        service = FileUploadService(db)
        result = service.preview_file(
            file_content=content,
            file_name=file.filename or "unknown",
            rows=rows,
            skip_rows=skip_rows,
            sheet_name=sheet_name,
        )
        return APIResponse(data=result)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ============================================================================
# Excel Sheet Names
# ============================================================================

@router.post("/sheets", response_model=APIResponse)
async def get_sheet_names(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get sheet names from an uploaded Excel file."""
    try:
        content = await file.read()
        service = FileUploadService(db)
        sheets = service.get_sheet_names(content, file.filename or "unknown")
        return APIResponse(data={"sheets": sheets})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
