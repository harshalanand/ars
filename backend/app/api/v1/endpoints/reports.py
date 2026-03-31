"""
Reports API — Pending Allocation report (ARS_pend_alc joined with VW_MASTER_PRODUCT)
"""
import io, json
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from loguru import logger
import pandas as pd

from app.database.session import get_data_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/reports", tags=["Reports"])

_ALLOWED_COLS = {'RDC','ST_CD','MATNR','QTY','MAJ_CAT','DIV','SUB_DIV','SEG',
                 'GEN_ART_NUMBER','CLR','SZ','SSN','RNG_SEG','MACRO_MVGR','MICRO_MVGR','FAB'}

_BASE_SQL = """
    SELECT
        PA.RDC, PA.ST_CD, PA.MATNR, PA.QTY,
        MP.MAJ_CAT, MP.DIV, MP.SUB_DIV, MP.SEG,
        MP.GEN_ART_NUMBER, MP.CLR, MP.SZ, MP.SSN, MP.RNG_SEG,
        MP.MACRO_MVGR, MP.MICRO_MVGR, MP.FAB
    FROM dbo.ARS_pend_alc PA WITH (NOLOCK)
    LEFT JOIN dbo.VW_MASTER_PRODUCT MP WITH (NOLOCK)
        ON CAST(PA.MATNR AS NVARCHAR(50)) = MP.ARTICLE_NUMBER
"""

def _check_table(engine):
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='ARS_pend_alc'"
        )).scalar() > 0


def _build_where(filters: dict) -> str:
    """Build WHERE clause from filters dict. e.g. {"ST_CD": ["HD24","HC02"], "DIV": ["MENS"]}"""
    parts = []
    for col, vals in filters.items():
        if col not in _ALLOWED_COLS or not vals:
            continue
        safe_vals = "','".join(v.replace("'", "''") for v in vals)
        parts.append(f"[{col}] IN ('{safe_vals}')")
    return " AND ".join(parts) if parts else ""


@router.get("/pend-alc", response_model=APIResponse)
def get_pend_alc_report(
    request: Request,
    limit: int = Query(5000),
    current_user: User = Depends(get_current_user),
):
    """Preview report. Supports server-side filters via query params: ?f_ST_CD=HD24,HC02&f_DIV=MENS"""
    engine = get_data_engine()
    if not _check_table(engine):
        return APIResponse(success=True, data={"columns": [], "total_rows": 0, "total_qty": 0, "preview": []})

    # Parse filters from query params: f_COLUMN=val1,val2
    filters = {}
    for key, val in request.query_params.items():
        if key.startswith('f_') and val:
            col = key[2:]
            if col in _ALLOWED_COLS:
                filters[col] = [v.strip() for v in val.split(',') if v.strip()]

    where = _build_where(filters)
    where_sql = f"WHERE {where}" if where else ""
    logger.info(f"PEND_ALC filters={filters} where={where_sql}")

    sql = f"SELECT TOP {limit} * FROM ({_BASE_SQL}) t {where_sql} ORDER BY RDC, ST_CD, MATNR"
    df = pd.read_sql(sql, engine)

    # Totals (with same filters applied)
    count_sql = f"SELECT COUNT(*), ISNULL(SUM(QTY),0) FROM ({_BASE_SQL}) t {where_sql}"
    with engine.connect() as conn:
        row = conn.execute(text(count_sql)).fetchone()
        total_rows, total_qty = row[0], row[1]
    logger.info(f"PEND_ALC result: {total_rows} rows, {total_qty} qty, preview={len(df)}")

    return APIResponse(success=True,
        message=f"{total_rows} records, {total_qty} total qty",
        data={
            "columns": list(df.columns),
            "total_rows": total_rows,
            "total_qty": int(total_qty),
            "has_filters": len(filters) > 0,
            "preview": json.loads(df.to_json(orient="records", date_format="iso")),
        })


@router.get("/pend-alc/distinct/{column}", response_model=APIResponse)
def get_distinct_values(column: str, current_user: User = Depends(get_current_user)):
    """Distinct values for filter dropdown from FULL table."""
    engine = get_data_engine()
    if not _check_table(engine):
        return APIResponse(success=True, data={"values": []})
    if column not in _ALLOWED_COLS:
        raise HTTPException(400, f"Invalid column: {column}")

    df = pd.read_sql(f"SELECT DISTINCT [{column}] FROM ({_BASE_SQL}) t WHERE [{column}] IS NOT NULL ORDER BY [{column}]", engine)
    return APIResponse(success=True, data={"values": df[column].astype(str).tolist()})


@router.get("/pend-alc/download")
def download_pend_alc_report(request: Request, current_user: User = Depends(get_current_user)):
    """Download filtered report as CSV."""
    engine = get_data_engine()
    if not _check_table(engine):
        raise HTTPException(404, "No data")

    # Parse filters
    filters = {}
    for key, val in request.query_params.items():
        if key.startswith('f_') and val:
            col = key[2:]
            if col in _ALLOWED_COLS:
                filters[col] = [v.strip() for v in val.split(',') if v.strip()]

    where = _build_where(filters)
    where_sql = f"WHERE {where}" if where else ""
    sql = f"SELECT * FROM ({_BASE_SQL}) t {where_sql} ORDER BY RDC, ST_CD, MATNR"

    def csv_stream():
        first = True
        for chunk in pd.read_sql(sql, engine, chunksize=50000):
            yield chunk.to_csv(index=False, header=first)
            first = False

    fname = "pend_alc_filtered.csv" if filters else "pending_allocation_report.csv"
    return StreamingResponse(csv_stream(), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"})
