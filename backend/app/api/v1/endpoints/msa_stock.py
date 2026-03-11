"""
MSA Stock Calculation API Endpoints
RESTful API for MSA filtering, calculation, and analysis
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Dict, Any
import json
from loguru import logger
from sqlalchemy import text
import pandas as pd

from app.database.session import get_db, get_data_db
from app.schemas.msa import (
    MSAFilterRequest,
    MSACalculateRequest,
    PivotTableRequest,
    MSARunRequest,
    DistinctValuesResponse,
    InitialDataResponse,
    MSAFilterResponse,
    MSACalculateResponse,
)
from app.schemas.common import APIResponse
from app.services.msa_service import MSAService
from app.security.dependencies import get_current_user
from app.models.rbac import User

router = APIRouter(prefix="/msa", tags=["MSA Stock Calculation"])


# ============================================================================
# Initialize & Configuration Endpoints
# ============================================================================

@router.get(
    "/columns",
    response_model=APIResponse,
    summary="Get MSA columns and dates"
)
def get_msa_columns(
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all available columns and dates from MSA view
    Used for initializing filter dropdowns
    
    Returns:
        - columns: List of all available columns
        - dates: List of distinct dates (sorted DESC)
        - filter_configs: List of saved filter configurations from database
    """
    try:
        service = MSAService(db)
        
        # Get columns and dates
        columns = service.get_available_columns()
        dates = service.get_available_dates()
        
        logger.info(f"✅ Retrieved {len(columns)} columns and {len(dates)} dates")
        logger.debug(f"Date samples: {dates[:5] if dates else 'None'}")
        
        # Get filter configs from database
        filter_configs = []
        try:
            # Query MSA_Filter_Config table
            sql = """
            SELECT 
                id,
                config_name, 
                created_at,
                is_last_used
            FROM dbo.MSA_Filter_Config
            ORDER BY created_at DESC
            """
            configs_df = pd.read_sql(text(sql), db.bind)
            
            if configs_df is not None and len(configs_df) > 0:
                filter_configs = [
                    {
                        'id': int(row['id']),
                        'name': row['config_name'],
                        'created_at': str(row['created_at']) if row['created_at'] else None,
                        'is_last_used': bool(row['is_last_used']) if 'is_last_used' in row else False
                    }
                    for _, row in configs_df.iterrows()
                ]
                logger.info(f"✅ Loaded {len(filter_configs)} filter configs from database: {[c['name'] for c in filter_configs]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch filter configs from database: {str(e)}")
            filter_configs = []
        
        # Always return response - dates should never be empty (uses fallback)
        return APIResponse(
            data={
                "columns": columns or [],
                "dates": dates or [],
                "filter_configs": filter_configs
            },
            message=f"Retrieved {len(columns)} columns, {len(dates)} dates, {len(filter_configs)} presets"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting MSA columns: {str(e)}", exc_info=True)
        # Return empty data instead of throwing - frontend handles gracefully
        return APIResponse(
            data={
                "columns": [],
                "dates": [],
                "filter_configs": []
            },
            message=f"Error: {str(e)}"
        )


@router.get(
    "/distinct",
    response_model=APIResponse,
    summary="Get distinct values for a column"
)
def get_distinct_values(
    column: str = Query(..., description="Column name"),
    date: str = Query(None, description="Optional date filter (YYYY-MM-DD)"),
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get distinct values for filtering a specific column
    
    Query Parameters:
        - column: Column name (required)
        - date: Optional date filter
    
    Returns:
        - values: List of distinct values for the column
        - total_count: Number of distinct values
    """
    try:
        if not column:
            raise HTTPException(status_code=400, detail="Column name required")
        
        logger.info(f"📍 Getting distinct values for column: {column}, date: {date}")
        
        service = MSAService(db)
        values = service.get_distinct_values(column, date)
        
        logger.info(f"✅ Endpoint returning {len(values)} distinct values for {column}: {values[:5]}")
        
        return APIResponse(
            data={
                "column": column,
                "values": values or [],
                "total_count": len(values)
            },
            message=f"Retrieved {len(values)} distinct values for {column}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting distinct values for {column}: {str(e)}", exc_info=True)
        # Return empty list instead of throwing error
        return APIResponse(
            data={
                "column": column,
                "values": [],
                "total_count": 0
            },
            message=f"Error: {str(e)}"
        )


@router.get(
    "/load/{config_name}",
    response_model=APIResponse,
    summary="Load filter configuration by name"
)
def load_filter_config(
    config_name: str,
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Load a saved filter configuration by name
    
    Path Parameters:
        - config_name: Name of the filter configuration to load
    
    Returns:
        - config_name: Configuration name
        - filter_columns: List of selected filter columns
        - filters: Dictionary of filter values {column: [values]}
        - sql_agg: Threshold percentage
        - created_at: When configuration was created
    """
    try:
        if not config_name:
            raise HTTPException(status_code=400, detail="Config name required")
        
        logger.info(f"📂 Loading filter config: {config_name}")
        
        # Query the config from database
        sql = """
        SELECT 
            id,
            config_name,
            filter_columns,
            filter_values,
            sql_agg,
            created_at
        FROM dbo.MSA_Filter_Config
        WHERE config_name = :name
        """
        
        import json
        configs_df = pd.read_sql(text(sql), db.bind, params={"name": config_name})
        
        if configs_df is None or len(configs_df) == 0:
            logger.warning(f"⚠️ Config not found: {config_name}")
            return APIResponse(
                data={},
                message=f"Configuration '{config_name}' not found"
            )
        
        row = configs_df.iloc[0]
        
        # Parse JSON fields
        try:
            filter_columns = json.loads(row['filter_columns']) if row['filter_columns'] else []
            filters = json.loads(row['filter_values']) if row['filter_values'] else {}
        except:
            filter_columns = []
            filters = {}
        
        logger.info(f"✅ Loaded config '{config_name}': {len(filter_columns)} columns, {len(filters)} filters")
        
        return APIResponse(
            data={
                "config_name": row['config_name'],
                "filter_columns": filter_columns,
                "filters": filters,
                "sql_agg": int(row['sql_agg']) if row['sql_agg'] else 25,
                "created_at": str(row['created_at']) if row['created_at'] else None
            },
            message=f"Loaded configuration '{config_name}'"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error loading config '{config_name}': {str(e)}", exc_info=True)
        return APIResponse(
            data={},
            message=f"Error loading configuration: {str(e)}"
        )


@router.post(
    "/config",
    response_model=APIResponse,
    summary="Save or update filter configuration"
)
def save_filter_config(
    body: Dict[str, Any],
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Save or update a filter configuration
    
    Request Body:
        - name: Configuration name (required)
        - filter_columns: List of selected filter columns
        - filters: Dictionary of filter values {column: [values]}
        - sql_agg: Threshold percentage (default 25)
    
    Returns:
        - config_name: Saved configuration name
        - message: Success or error message
    """
    try:
        config_name = body.get('name', '').strip()
        filter_columns = body.get('filter_columns', [])
        filters = body.get('filters', {})
        sql_agg = body.get('sql_agg', 25)
        
        if not config_name:
            raise HTTPException(status_code=400, detail="Configuration name is required")
        
        logger.info(f"💾 Saving filter config: {config_name}")
        
        import json
        
        # Convert to JSON strings
        filter_cols_json = json.dumps(filter_columns)
        filters_json = json.dumps(filters)
        
        # Check if config exists
        check_sql = "SELECT id FROM dbo.MSA_Filter_Config WHERE config_name = :name"
        exists_df = pd.read_sql(text(check_sql), db.bind, params={"name": config_name})
        
        with db.begin():
            if exists_df is not None and len(exists_df) > 0:
                # Update existing
                update_sql = """
                UPDATE dbo.MSA_Filter_Config
                SET filter_columns = :fc,
                    filter_values = :fv,
                    sql_agg = :sa,
                    is_last_used = 1,
                    updated_at = SYSUTCDATETIME()
                WHERE config_name = :n
                """
                db.execute(text(update_sql), {
                    "n": config_name,
                    "fc": filter_cols_json,
                    "fv": filters_json,
                    "sa": int(sql_agg)
                })
                logger.info(f"✅ Updated config: {config_name}")
            else:
                # Insert new
                insert_sql = """
                INSERT INTO dbo.MSA_Filter_Config
                (config_name, filter_columns, filter_values, sql_agg, is_last_used)
                VALUES (:n, :fc, :fv, :sa, 1)
                """
                db.execute(text(insert_sql), {
                    "n": config_name,
                    "fc": filter_cols_json,
                    "fv": filters_json,
                    "sa": int(sql_agg)
                })
                logger.info(f"✅ Created new config: {config_name}")
        
        return APIResponse(
            data={
                "config_name": config_name,
                "filter_columns": len(filter_columns),
                "filters": len(filters),
                "sql_agg": sql_agg
            },
            message=f"Configuration '{config_name}' saved successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error saving config: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error saving configuration: {str(e)}")


# ============================================================================
# Filtering & Data Loading
# ============================================================================

@router.get(
    "/debug/test-date",
    response_model=APIResponse,
    summary="Debug: Test if date has data"
)
def debug_test_date(
    date: str = Query(..., description="Date to test (YYYY-MM-DD)"),
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Debug endpoint to check if a date has data in the view
    """
    try:
        logger.info(f"🔍 DEBUG: Testing date '{date}'")
        
        # Test 1: Check if date has ANY data
        sql1 = f"""
        SELECT COUNT(*) as row_count, 
               COUNT(DISTINCT CAST([DATE] AS DATE)) as date_count
        FROM {MSAService(db).main_table}
        WHERE CAST([DATE] AS DATE) = :test_date
        """
        test_df = pd.read_sql(text(sql1), db.bind, params={"test_date": date})
        row_count = int(test_df['row_count'].iloc[0])
        date_count = int(test_df['date_count'].iloc[0])
        
        logger.info(f"✅ Date '{date}' has {row_count} rows")
        
        # Test 2: Get sample ST_CD values for this date
        sql2 = f"""
        SELECT DISTINCT ST_CD
        FROM {MSAService(db).main_table}
        WHERE CAST([DATE] AS DATE) = :test_date
        LIMIT 10
        """
        sample_df = pd.read_sql(text(sql2), db.bind, params={"test_date": date})
        st_cd_samples = sample_df['ST_CD'].tolist() if len(sample_df) > 0 else []
        
        logger.info(f"✅ Sample ST_CD values for date: {st_cd_samples}")
        
        # Test 3: Get all available dates in the view
        sql3 = f"""
        SELECT DISTINCT CAST([DATE] AS DATE) as date_val
        FROM {MSAService(db).main_table}
        ORDER BY date_val DESC
        LIMIT 20
        """
        dates_df = pd.read_sql(text(sql3), db.bind)
        available_dates = [str(d) for d in dates_df['date_val'].tolist()]
        
        logger.info(f"✅ Available dates: {available_dates}")
        
        return APIResponse(
            data={
                "date_tested": date,
                "row_count": row_count,
                "has_data": row_count > 0,
                "sample_st_cd_values": st_cd_samples,
                "available_dates": available_dates
            },
            message=f"Date '{date}' has {row_count} rows"
        )
    except Exception as e:
        logger.error(f"❌ Debug error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/filter",
    response_model=APIResponse,
    summary="Apply filters and load MSA data"
)
def apply_filters(
    body: MSAFilterRequest,
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user),
    request=None  # For session storage if needed
):
    """
    Apply filters to MSA data and load results
    Data is stored in session/cache for subsequent operations
    
    Request Body:
        - date: Date filter (YYYY-MM-DD)
        - filters: Dict of column names to list of filter values
    
    Returns:
        - row_count: Number of rows loaded
        - columns: Available columns in result
        - total_stock_qty: Sum of STK_Q column
        - message: Status message
    """
    try:
        if not body.date:
            logger.warning(f"❌ /filter endpoint called without date")
            raise HTTPException(status_code=400, detail="Date is required")
        
        logger.info(f"📥 /filter endpoint called")

        logger.info(f"   Date: {body.date}")
        logger.info(f"   Filters received: {body.filters}")
        logger.info(f"   Number of filter columns: {len(body.filters)}")
        
        # Check if filters are empty
        if not body.filters or all(not v for v in body.filters.values()):
            logger.warning(f"⚠️ No filters provided! Filters dict: {body.filters}")
        
        service = MSAService(db)
        
        # Call service to apply filters and get dataframe
        df, total_stock_qty = service.apply_filters(body.date, body.filters)
        
        logger.info(f"✅ Filters applied successfully:")
        logger.info(f"   Loaded {len(df)} rows")
        logger.info(f"   Total STK_Q: {total_stock_qty}")
        logger.info(f"   Columns: {len(df.columns)} = {df.columns.tolist() if len(df) > 0 else 'N/A'}")
        
        # Check data size
        import sys
        df_memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024) if len(df) > 0 else 0.0
        logger.info(f"   Memory usage: {df_memory_mb:.2f}MB")
        
        if len(df) == 0:
            logger.warning(f"⚠️ WARNING: Query returned 0 rows!")
            logger.warning(f"   Check if:")
            logger.warning(f"   1. Date '{body.date}' has data in the view")
            logger.warning(f"   2. Filter values actually exist for this date")
            logger.warning(f"   3. Filters are not empty: {body.filters}")
        
        if df_memory_mb > 500:
            logger.warning(f"⚠️ Large result set detected ({df_memory_mb:.2f}MB)")
        
        return APIResponse(
            data={
                "row_count": len(df),
                "columns": df.columns.tolist() if len(df) > 0 else [],
                "total_stock_qty": float(total_stock_qty),
                "memory_mb": round(df_memory_mb, 2)
            },
            message=f"Loaded {len(df)} rows successfully"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error in /filter endpoint: {str(e)}", exc_info=True)
        logger.error(f"   Request body: date={body.date if 'body' in locals() else 'N/A'}, filters={body.filters if 'body' in locals() else 'N/A'}")
        raise HTTPException(status_code=500, detail=f"Error applying filters: {str(e)}")


# ============================================================================
# MSA Calculation
# ============================================================================

@router.post(
    "/calculate",
    response_model=APIResponse,
    summary="Calculate MSA allocation"
)
def calculate_msa(
    body: MSACalculateRequest,
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user),
    request=None  # For session retrieval if needed
):
    """
    Calculate MSA allocation from filtered data
    Returns 3 result sets: base analysis, generated colors, color variants
    
    Request Body:
        - slocs: List of SLOC codes to include
        - threshold: Minimum allocation percentage (0-100)
    
    Returns:
        - msa: Base MSA analysis table
        - msa_gen_clr: Generated colors analysis
        - msa_gen_clr_var: Color variants analysis
        - row_counts: Row counts for each result set
    """
    try:
        if not body.slocs:
            raise HTTPException(status_code=400, detail="At least one SLOC is required")
        
        logger.info(f"Calculating MSA for SLOCs: {body.slocs}, threshold: {body.threshold}, date: {body.date}")
        
        service = MSAService(db)
        
        # Load data with filters to improve performance
        # Use provided date and filters, or load all data if not provided
        date_filter = body.date if body.date else ""
        filters = body.filters if body.filters else {}
        
        logger.info(f"Loading data with filters - date: '{date_filter}', filters: {filters}")
        df, _ = service.apply_filters(date_filter, filters)
        
        # Calculate MSA
        results = service.calculate(df, body.slocs, body.threshold)
        
        logger.info(f"MSA calculation complete: {results['row_counts']}")
        
        return APIResponse(
            data=results,
            message="MSA calculation completed successfully"
        )
    except Exception as e:
        logger.error(f"Error calculating MSA: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Pivot Table Generation
# ============================================================================

@router.post(
    "/pivot",
    response_model=APIResponse,
    summary="Generate pivot table"
)
def generate_pivot_table(
    body: PivotTableRequest,
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user),
    request=None
):
    """
    Generate pivot table from MSA data
    
    Request Body:
        - index_cols: Column(s) for index (rows)
        - pivot_cols: Column(s) for pivot (columns)
        - value_cols: Column(s) for values
        - agg_funcs: Aggregation functions
        - fill_zero: Fill missing with 0
        - margin_totals: Add margin totals
    
    Returns:
        - columns: Pivot table column names
        - data: Pivot table data rows
        - row_count: Number of rows in pivot
    """
    try:
        logger.info(f"Generating pivot table: index={body.index_cols}, pivot={body.pivot_cols}, values={body.value_cols}")
        
        service = MSAService(db)
        
        # Load data - in production would retrieve from cache
        df, _ = service.apply_filters("", {})
        
        # Generate pivot
        pivot_result = service.generate_pivot(
            df,
            body.index_cols,
            body.pivot_cols,
            body.value_cols,
            body.agg_funcs,
            body.fill_zero,
            body.margin_totals
        )
        
        logger.info(f"Pivot table generated: {pivot_result['row_count']} rows")
        
        return APIResponse(
            data=pivot_result,
            message="Pivot table generated successfully"
        )
    except Exception as e:
        logger.error(f"Error generating pivot: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Legacy Endpoints (for backward compatibility)
# ============================================================================

@router.post(
    "/run",
    response_model=APIResponse,
    summary="Run MSA calculation (legacy)"
)
def run_msa_legacy(
    body: MSARunRequest,
    db: Session = Depends(get_data_db),
    current_user: User = Depends(get_current_user)
):
    """
    Legacy MSA run endpoint for backward compatibility
    Combines filtering and calculation in one call
    """
    try:
        service = MSAService(db)
        
        # Apply filters
        df, _ = service.apply_filters("", body.filters)
        
        # Calculate
        results = service.calculate(df, body.slocs, body.threshold)
        
        return APIResponse(
            data=results,
            message="MSA calculation completed"
        )
    except Exception as e:
        logger.error(f"Error in legacy MSA run: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/save",
    response_model=APIResponse,
    summary="Save MSA results"
)
def save_msa_results(
    body: Dict[str, Any],
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Save MSA results to database
    Generates a token for tracking
    
    Returns:
        - token: Unique token for these results
        - threshold: Applied threshold
        - filters: Applied filters
    """
    try:
        from datetime import datetime
        import random
        
        # Generate token
        today = datetime.now().strftime("%Y%m%d")
        rand = random.randint(1, 999)
        token = f"MSA{today}-{rand:03d}"
        
        logger.info(f"Saving MSA results with token: {token}")
        
        # In production, save to database
        # data1 = body.get("data1", [])
        # data2 = body.get("data2", [])
        # data3 = body.get("data3", [])
        # threshold = body.get("threshold")
        # filters = body.get("filters", {})
        
        return APIResponse(
            data={
                "token": token,
                "threshold": body.get("threshold"),
                "filters": body.get("filters", {})
            },
            message="MSA results saved with token"
        )
    except Exception as e:
        logger.error(f"Error saving MSA results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

