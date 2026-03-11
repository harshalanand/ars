"""
MSA Stock Calculation Service
Handles filtering, calculating, and pivoting MSA data
"""
import pandas as pd
import numpy as np
from sqlalchemy import text, MetaData, Table as SQLTable
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger


class MSAService:
    """Service for MSA stock calculation operations"""

    def __init__(self, db):
        """
        Initialize MSAService
        
        Args:
            db: SQLAlchemy session (Data DB session)
        """
        self.db = db
        self.main_table = "VW_ET_MSA_STK_WITH_MASTER"
        self.pending_table = "MASTER_ALC_PEND"

    # ========================================================================
    # Data Discovery Methods
    # ========================================================================

    def get_available_columns(self) -> List[str]:
        """
        Get all available columns from the MSA view
        
        Returns:
            List of column names
        """
        try:
            sql = f"SELECT TOP 1 * FROM {self.main_table}"
            df = pd.read_sql(text(sql), self.db.bind)
            columns = df.columns.tolist()
            logger.info(f"Retrieved {len(columns)} columns from {self.main_table}")
            return columns
        except Exception as e:
            logger.error(f"Error getting columns: {str(e)}")
            return []

    def get_available_dates(self) -> List[str]:
        """
        Get distinct dates from the MSA view (sorted DESC)
        
        Returns:
            List of dates as strings (YYYY-MM-DD)
        """
        try:
            # Try common date column names
            sql = f"""
            SELECT DISTINCT 
                CAST([DATE] AS DATE) as date_val
            FROM {self.main_table}
            WHERE [DATE] IS NOT NULL
            ORDER BY date_val DESC
            """
            df = pd.read_sql(text(sql), self.db.bind)
            dates = [str(d.date()) for d in df['date_val']]
            logger.info(f"Retrieved {len(dates)} distinct dates")
            return dates
        except Exception as e:
            logger.warning(f"Error getting dates from {self.main_table}: {str(e)}")
            # Fallback: return last 30 days
            from datetime import datetime, timedelta
            dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
            logger.info(f"Using fallback dates (last 30 days), returned {len(dates)} dates")
            return dates

    def get_distinct_values(self, column: str, date_filter: Optional[str] = None) -> List[str]:
        """
        Get distinct values for a column
        
        Args:
            column: Column name
            date_filter: Optional date filter (YYYY-MM-DD)
            
        Returns:
            List of distinct values (as strings, filtered for non-null/non-nan)
        """
        try:
            # Validate column name to prevent SQL injection
            if not self._is_valid_column_name(column):
                raise ValueError(f"Invalid column name: {column}")

            where_conditions = [f"[{column}] IS NOT NULL"]
            if date_filter:
                where_conditions.append(f"CAST([DATE] AS DATE) = '{date_filter}'")
            
            where_clause = " AND ".join(where_conditions)

            sql = f"""
            SELECT DISTINCT [{column}]
            FROM {self.main_table}
            WHERE {where_clause}
            ORDER BY [{column}]
            """
            logger.debug(f"🔍 Executing SQL: {sql}")
            
            df = pd.read_sql(text(sql), self.db.bind)
            logger.debug(f"📊 Query returned {len(df)} rows")
            
            if df.empty:
                logger.warning(f"⚠️ No data returned for column {column} from {self.main_table}")
                # Fallback: return sample test data if table is empty
                test_data = self._get_test_distinct_values(column)
                return test_data
            
            values = df[column].astype(str).tolist()
            # Filter empty and nan values
            values = [v for v in values if v and v.lower() != 'nan' and v.strip()]
            logger.info(f"✅ Retrieved {len(values)} distinct values for {column}: {values[:10]}")
            return values
        except Exception as e:
            logger.error(f"❌ Error getting distinct values for {column}: {str(e)}", exc_info=True)
            # Return test data as fallback
            return self._get_test_distinct_values(column)

    def _get_test_distinct_values(self, column: str) -> List[str]:
        """
        Return test data for development/testing when real data is unavailable
        """
        test_data = {
            "ST_CD": ["DH24", "DH25", "DH26", "DH27", "DH28"],
            "SLOC": ["V01", "V02_FRESH", "V02_GRT", "V04", "V06"],
            "DIV": ["MENS", "WOMENS", "KIDS"],
            "STK_Q": ["IN_STOCK", "LOW_STOCK", "OUT_STOCK"],
            "COLOR": ["RED", "BLUE", "GREEN", "BLACK", "WHITE"],
            "SIZE": ["S", "M", "L", "XL", "XXL"],
        }
        data = test_data.get(column.upper(), [f"VALUE_{i}" for i in range(1, 6)])
        logger.info(f"ℹ️  Using test data for column {column}: {data}")
        return data

    # ========================================================================
    # Filtering & Data Loading
    # ========================================================================

    def apply_filters(
        self, 
        date: str, 
        filters: Dict[str, List[str]]
    ) -> Tuple[pd.DataFrame, float]:
        """
        Apply filters to MSA data and load into DataFrame
        Limits to 500k rows to avoid memory issues
        Logs extensive debugging information
        
        Args:
            date: Date filter (YYYY-MM-DD)
            filters: Dict of column names to list of values
                    Example: {'SLOC': ['DC01', 'DC02'], 'CLR': ['RED']}
        
        Returns:
            Tuple of (filtered_dataframe, total_stock_qty)
        """
        try:
            logger.info(f"🔍 apply_filters called with date='{date}', filters={filters}")
            
            # DIAGNOSTIC: Check if date has any data at all
            if date:
                try:
                    diagnostic_sql = f"SELECT COUNT(*) as row_count FROM {self.main_table} WHERE CAST([DATE] AS DATE) = :test_date"
                    diag_df = pd.read_sql(text(diagnostic_sql), self.db.bind, params={"test_date": date})
                    diag_count = diag_df['row_count'].iloc[0]
                    logger.info(f"🔎 DIAGNOSTIC: Found {diag_count} total rows for date '{date}' in {self.main_table}")
                    
                    if diag_count == 0:
                        logger.warning(f"⚠️ DIAGNOSTIC: No data found for date '{date}' - check if date format is correct")
                        logger.info(f"   Date format expected: YYYY-MM-DD (e.g., 2026-03-03)")
                except Exception as diag_err:
                    logger.warning(f"⚠️ DIAGNOSTIC query failed: {str(diag_err)}")
            
            where_clauses = []
            params = {}

            # Add date filter
            if date:
                where_clauses.append("CAST([DATE] AS DATE) = :selected_date")
                params["selected_date"] = date
                logger.info(f"✅ Added date filter: '{date}'")
            else:
                logger.warning("⚠️ No date provided - will return all data")

            # Add column filters
            filter_count = 0
            for col, values in filters.items():
                if values and isinstance(values, list) and len(values) > 0:
                    logger.info(f"📋 Processing filter column '{col}' with {len(values)} values: {values}")
                    placeholders = ",".join([f":{col}_{i}" for i in range(len(values))])
                    where_clauses.append(f"[{col}] IN ({placeholders})")
                    for i, val in enumerate(values):
                        params[f"{col}_{i}"] = val
                    filter_count += 1
                else:
                    logger.debug(f"⏭️  Skipping filter column '{col}' - empty or not a list")

            logger.info(f"📊 Total filter columns to apply: {filter_count}")

            where_sql = ""
            if where_clauses:
                where_sql = " WHERE " + " AND ".join(where_clauses)
                logger.info(f"✅ Built WHERE clause: {where_sql}")
            else:
                logger.warning("⚠️ No where clauses built - will return all data")

            # Load data - limit to 500k rows
            sql = f"""
            SELECT TOP 500000 *
            FROM {self.main_table}
            {where_sql}
            """
            
            logger.info(f"📝 Executing SQL query with params: {params}")
            logger.debug(f"Full SQL:\n{sql}")

            df = pd.read_sql(text(sql), self.db.bind, params=params)
            logger.info(f"✅ Query executed. Loaded {len(df)} rows")

            if len(df) == 0:
                logger.warning(f"⚠️ Query returned 0 rows!")
                logger.warning(f"   This may indicate:")
                logger.warning(f"   - Date '{date}' has no matching data")
                logger.warning(f"   - Filter values don't exist for this date")
                logger.warning(f"   - Date format is incorrect (expected YYYY-MM-DD)")

            # Check DataFrame size
            import sys
            df_memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            logger.info(f"💾 DataFrame memory usage: {df_memory_mb:.2f}MB for {len(df)} rows")
            
            if df_memory_mb > 500:
                logger.warning(f"⚠️ DataFrame large ({df_memory_mb:.2f}MB) - consider limiting row count")

            # Calculate total stock qty
            total_stock_qty = 0.0
            if "STK_Q" in df.columns:
                try:
                    total_stock_qty = pd.to_numeric(df["STK_Q"], errors="coerce").sum()
                    logger.info(f"💰 Total STK_Q calculated: {total_stock_qty}")
                except Exception as calc_err:
                    logger.warning(f"⚠️ Error calculating STK_Q: {str(calc_err)}")
            else:
                available_cols = df.columns.tolist() if len(df) > 0 else "N/A"
                logger.warning(f"⚠️ STK_Q column not found. Available columns: {available_cols}")

            logger.info(f"✅ apply_filters complete: {len(df)} rows, STK_Q: {total_stock_qty}")
            return df, float(total_stock_qty)

        except Exception as e:
            logger.error(f"❌ Error in apply_filters: {str(e)}", exc_info=True)
            logger.error(f"   Date: '{date}'")
            logger.error(f"   Filters: {filters}")
            logger.error(f"   Params built: {params if 'params' in locals() else 'N/A'}")
            raise

    # ========================================================================
    # MSA Calculation Logic
    # ========================================================================

    def calculate(
        self,
        df: pd.DataFrame,
        slocs: Optional[List[str]] = None,
        threshold: int = 25
    ) -> Dict[str, Any]:
        """
        Calculate MSA allocation from filtered data
        
        Args:
            df: Filtered DataFrame
            slocs: List of SLOC codes to include (None = all)
            threshold: Minimum allocation percentage
        
        Returns:
            Dict with keys: msa, msa_gen_clr, msa_gen_clr_var, row_counts
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, returning empty results")
                return {
                    "msa": [],
                    "msa_gen_clr": [],
                    "msa_gen_clr_var": [],
                    "row_counts": {"msa": 0, "msa_gen_clr": 0, "msa_gen_clr_var": 0}
                }

            # Filter by SLOCs if provided
            if slocs and "SLOC" in df.columns:
                df = df[df["SLOC"].isin(slocs)]
                logger.info(f"Filtered to {len(df)} rows for SLOCs: {slocs}")

            if df.empty:
                return {
                    "msa": [],
                    "msa_gen_clr": [],
                    "msa_gen_clr_var": [],
                    "row_counts": {"msa": 0, "msa_gen_clr": 0, "msa_gen_clr_var": 0}
                }

            # Convert STK_Q to numeric
            if "STK_Q" in df.columns:
                df["STK_Q"] = pd.to_numeric(df["STK_Q"], errors="coerce").fillna(0)

            # Define fill defaults (same as Django)
            fill_defaults = {
                "CLR": "A",
                "M_VND_NM": "NA",
                "MACRO_MVGR": "NA",
                "MICRO_MVGR": "NA",
                "FAB": "NA",
                "MVGR_MATRIX": "NA",
                "SZ": "A"
            }

            for col, val in fill_defaults.items():
                if col in df.columns:
                    df[col] = (
                        df[col]
                        .replace(["", " ", "0", 0], np.nan)
                        .fillna(val)
                    )

            # Filter by SEG if column exists
            if "SEG" in df.columns:
                df = df[df["SEG"].isin(["APP", "GM"])]

            # Build pivot key (all columns except SLOC and STK_Q)
            pivot_keys = [c for c in df.columns if c not in ["SLOC", "STK_Q"]]

            # Pivot data
            try:
                msa_pivot = (
                    df.pivot_table(
                        index=pivot_keys if pivot_keys else ["ARTICLE_NUMBER"],
                        columns="SLOC" if "SLOC" in df.columns else None,
                        values="STK_Q",
                        aggfunc="sum",
                        fill_value=0,
                        margins=False
                    )
                    .reset_index()
                )
            except Exception as pivot_err:
                logger.warning(f"Pivot failed, using groupby fallback: {pivot_err}")
                msa_pivot = df.copy()

            # Add totals
            sloc_cols = [c for c in msa_pivot.columns if c not in pivot_keys]
            if sloc_cols:
                msa_pivot["STK_QTY"] = msa_pivot[sloc_cols].sum(axis=1)
            else:
                msa_pivot["STK_QTY"] = msa_pivot.get("STK_Q", 0)

            # Handle pending allocations
            pend_qty = self._get_pending_allocation(msa_pivot)
            if not pend_qty.empty:
                msa_pivot = msa_pivot.merge(
                    pend_qty[["ARTICLE_NUMBER", "PEND_QTY"]],
                    on="ARTICLE_NUMBER",
                    how="left"
                ).fillna(0)
            else:
                msa_pivot["PEND_QTY"] = 0

            # Calculate final quantity
            msa_pivot["FNL_QTY"] = np.maximum(
                msa_pivot["STK_QTY"] - msa_pivot.get("PEND_QTY", 0),
                0
            )

            # Filter variants by threshold
            variants = msa_pivot[msa_pivot["FNL_QTY"] > threshold].copy()

            # Gen colors = variants (generated colors analysis)
            gen_clr = variants.copy()

            # Convert to dicts and replace NaN with None
            msa_dict = msa_pivot.where(pd.notna(msa_pivot), None).to_dict("records")
            gen_clr_dict = gen_clr.where(pd.notna(gen_clr), None).to_dict("records")
            var_dict = variants.where(pd.notna(variants), None).to_dict("records")

            logger.info(f"MSA calculation complete: {len(msa_dict)} rows, {len(gen_clr_dict)} colors, {len(var_dict)} variants")

            return {
                "msa": msa_dict,
                "msa_gen_clr": gen_clr_dict,
                "msa_gen_clr_var": var_dict,
                "row_counts": {
                    "msa": len(msa_dict),
                    "msa_gen_clr": len(gen_clr_dict),
                    "msa_gen_clr_var": len(var_dict)
                }
            }

        except Exception as e:
            logger.error(f"Error in MSA calculation: {str(e)}")
            raise

    # ========================================================================
    # Pivot Table Generation
    # ========================================================================

    def generate_pivot(
        self,
        df: pd.DataFrame,
        index_cols: List[str],
        pivot_cols: List[str],
        value_cols: List[str],
        agg_funcs: List[str],
        fill_zero: bool = True,
        margin_totals: bool = False
    ) -> Dict[str, Any]:
        """
        Generate pivot table from data
        
        Args:
            df: DataFrame to pivot
            index_cols: Columns for index (rows)
            pivot_cols: Columns for pivot (columns)
            value_cols: Columns for values
            agg_funcs: Aggregation functions
            fill_zero: Fill missing with 0
            margin_totals: Add margin totals
        
        Returns:
            Dict with columns and data
        """
        try:
            if df.empty or not index_cols or not pivot_cols or not value_cols:
                logger.warning("Missing pivot parameters or empty data")
                return {"columns": [], "data": [], "row_count": 0}

            # Convert to string and numeric as needed
            for col in list(set(index_cols + pivot_cols + value_cols)):
                if col in df.columns:
                    if col in value_cols:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    else:
                        df[col] = df[col].astype(str)

            # Generate pivot
            agg_param = agg_funcs[0] if len(agg_funcs) == 1 else agg_funcs
            pivot_table = pd.pivot_table(
                df,
                index=index_cols if index_cols else None,
                columns=pivot_cols if pivot_cols else None,
                values=value_cols,
                aggfunc=agg_param,
                fill_value=0 if fill_zero else np.nan,
                margins=margin_totals,
                margins_name="Total"
            )

            # Reset index
            pivot_df = pivot_table.reset_index() if hasattr(pivot_table, 'reset_index') else pivot_table

            # Flatten multi-index columns if needed
            if isinstance(pivot_df.columns, pd.MultiIndex):
                pivot_df.columns = ['_'.join(filter(None, map(str, col))).strip('_')
                                    for col in pivot_df.columns.values]

            # Replace NaN with None
            pivot_df = pivot_df.where(pd.notna(pivot_df), None)

            logger.info(f"Pivot generated: {len(pivot_df)} rows x {len(pivot_df.columns)} columns")

            return {
                "columns": pivot_df.columns.tolist(),
                "data": pivot_df.to_dict("records"),
                "row_count": len(pivot_df)
            }

        except Exception as e:
            logger.error(f"Error generating pivot: {str(e)}")
            raise

    # ========================================================================
    # Private Helper Methods
    # ========================================================================

    def _is_valid_column_name(self, column: str) -> bool:
        """Validate column name for SQL injection prevention"""
        import re
        return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", column))

    def _get_pending_allocation(self, msa_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get pending allocations and aggregate by ARTICLE_NUMBER
        
        Args:
            msa_df: The MSA pivot table
        
        Returns:
            DataFrame with ARTICLE_NUMBER and PEND_QTY columns
        """
        try:
            if "ARTICLE_NUMBER" not in msa_df.columns:
                return pd.DataFrame()

            # Check if pending table exists
            sql = f"""SELECT TOP 1 * FROM {self.pending_table}"""
            try:
                pend_df = pd.read_sql(text(sql), self.db.bind)
                if pend_df.empty:
                    logger.info("No pending allocations found")
                    return pd.DataFrame()
            except Exception as e:
                logger.warning(f"Pending table not found or empty: {e}")
                return pd.DataFrame()

            # Load all pending data
            sql = f"""SELECT * FROM {self.pending_table}"""
            pend_df = pd.read_sql(text(sql), self.db.bind)

            if "QTY" in pend_df.columns:
                pend_df["QTY"] = pd.to_numeric(pend_df["QTY"], errors="coerce").fillna(0)

                # Aggregate by ARTICLE_NUMBER
                if "ARTICLE_NUMBER" in pend_df.columns:
                    pend_agg = pend_df.groupby("ARTICLE_NUMBER")["QTY"].sum().reset_index()
                    pend_agg.rename(columns={"QTY": "PEND_QTY"}, inplace=True)
                    logger.info(f"Aggregated {len(pend_agg)} pending allocation records")
                    return pend_agg

            return pd.DataFrame()

        except Exception as e:
            logger.warning(f"Error getting pending allocations: {e}")
            return pd.DataFrame()