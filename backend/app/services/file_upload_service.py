"""
File Upload & Processing Service
==================================
Handles CSV and Excel file uploads, validation, and routing to the upsert engine.
Supports 1M+ rows via chunked reading.
"""
import os
import uuid
import time
from typing import Optional, List, Dict, Any
from io import BytesIO

import pandas as pd
from loguru import logger

from app.services.upsert_engine import UpsertEngine
from app.core.config import get_settings

settings = get_settings()

# Upload directory
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


class FileUploadService:
    """
    Processes uploaded CSV/Excel files and routes data to the upsert engine.
    """

    ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}
    MAX_FILE_SIZE = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024  # bytes

    def __init__(self, db):
        self.db = db
        self.upsert_engine = UpsertEngine(db)

    async def process_upload(
        self,
        file_content: bytes,
        file_name: str,
        table_name: str,
        primary_key_columns: List[str],
        changed_by: str,
        ip_address: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        skip_rows: int = 0,
        sheet_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process an uploaded file and upsert into the target table.

        Args:
            file_content: Raw file bytes
            file_name: Original filename
            table_name: Target SQL table
            primary_key_columns: PK columns for upsert
            changed_by: Username
            ip_address: Client IP
            column_mapping: Optional {file_col: table_col} mapping
            skip_rows: Number of rows to skip from top
            sheet_name: Excel sheet name (None = first sheet)

        Returns:
            Upload result with stats
        """
        start_time = time.time()
        batch_id = f"UPL_{uuid.uuid4().hex[:10]}"

        # Validate extension
        ext = os.path.splitext(file_name)[1].lower()
        if ext not in self.ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}. Allowed: {self.ALLOWED_EXTENSIONS}")

        # Validate file size
        if len(file_content) > self.MAX_FILE_SIZE:
            raise ValueError(
                f"File too large: {len(file_content) / (1024*1024):.1f}MB. "
                f"Max: {settings.MAX_UPLOAD_SIZE_MB}MB"
            )

        logger.info(f"[{batch_id}] Processing upload: {file_name} ({len(file_content)} bytes) → {table_name}")

        # Save a copy for audit trail
        saved_path = os.path.join(UPLOAD_DIR, f"{batch_id}_{file_name}")
        with open(saved_path, "wb") as f:
            f.write(file_content)

        # Read file into DataFrame
        try:
            df = self._read_file(file_content, ext, skip_rows, sheet_name)
        except Exception as e:
            raise ValueError(f"Failed to read file: {e}")

        if df.empty:
            raise ValueError("File contains no data")

        logger.info(f"[{batch_id}] Read {len(df)} rows, {len(df.columns)} columns from {file_name}")

        # Apply column mapping
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # Clean column names
        df.columns = [str(c).strip() for c in df.columns]

        # Validate PKs exist
        missing_pks = [pk for pk in primary_key_columns if pk not in df.columns]
        if missing_pks:
            raise ValueError(
                f"Primary key columns not found in file: {missing_pks}. "
                f"Available columns: {list(df.columns)}"
            )

        # Drop rows where PK is null
        pk_null_mask = df[primary_key_columns].isna().any(axis=1)
        null_pk_count = pk_null_mask.sum()
        if null_pk_count > 0:
            logger.warning(f"[{batch_id}] Dropping {null_pk_count} rows with null PKs")
            df = df[~pk_null_mask]

        # Clean data
        df = self._clean_dataframe(df)

        # Execute upsert
        result = self.upsert_engine.upsert(
            table_name=table_name,
            df=df,
            primary_key_columns=primary_key_columns,
            changed_by=changed_by,
            source="UPLOAD",
            ip_address=ip_address,
            chunk_size=settings.UPLOAD_CHUNK_SIZE,
        )

        # Add upload-specific info
        result["file_name"] = file_name
        result["file_size_bytes"] = len(file_content)
        result["null_pk_rows_dropped"] = int(null_pk_count)
        result["saved_file"] = saved_path

        duration_ms = int((time.time() - start_time) * 1000)
        result["total_duration_ms"] = duration_ms

        logger.info(
            f"[{batch_id}] Upload complete: {file_name} → {table_name} | "
            f"{result['inserted']} inserted, {result['updated']} updated, "
            f"{result['errors']} errors | {duration_ms}ms"
        )

        return result

    def preview_file(
        self,
        file_content: bytes,
        file_name: str,
        rows: int = 20,
        skip_rows: int = 0,
        sheet_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Preview first N rows of an uploaded file (for mapping & validation UI).
        """
        ext = os.path.splitext(file_name)[1].lower()
        df = self._read_file(file_content, ext, skip_rows, sheet_name, nrows=rows)

        return {
            "file_name": file_name,
            "total_columns": len(df.columns),
            "preview_rows": len(df),
            "columns": [
                {
                    "name": str(col),
                    "dtype": str(df[col].dtype),
                    "null_count": int(df[col].isna().sum()),
                    "sample_values": [
                        str(v) if pd.notna(v) else None
                        for v in df[col].head(5).tolist()
                    ],
                }
                for col in df.columns
            ],
            "data": df.head(rows).where(pd.notna(df), None).to_dict(orient="records"),
        }

    def get_sheet_names(self, file_content: bytes, file_name: str) -> List[str]:
        """Get sheet names from an Excel file."""
        ext = os.path.splitext(file_name)[1].lower()
        if ext not in (".xlsx", ".xls"):
            return []

        try:
            xls = pd.ExcelFile(BytesIO(file_content))
            return xls.sheet_names
        except Exception:
            return []

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _read_file(
        self,
        content: bytes,
        ext: str,
        skip_rows: int = 0,
        sheet_name: Optional[str] = None,
        nrows: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read file content into a Pandas DataFrame."""
        buffer = BytesIO(content)

        read_kwargs = {}
        if skip_rows > 0:
            read_kwargs["skiprows"] = skip_rows
        if nrows:
            read_kwargs["nrows"] = nrows

        if ext == ".csv":
            # Try common encodings
            for encoding in ["utf-8", "latin-1", "cp1252"]:
                try:
                    buffer.seek(0)
                    return pd.read_csv(buffer, encoding=encoding, **read_kwargs)
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
            raise ValueError("Could not read CSV with any supported encoding")

        elif ext in (".xlsx", ".xls"):
            return pd.read_excel(
                buffer,
                sheet_name=sheet_name or 0,
                engine="openpyxl" if ext == ".xlsx" else None,
                **read_kwargs,
            )

        raise ValueError(f"Unsupported file extension: {ext}")

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame before upsert."""
        # Strip whitespace from string columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' strings back to actual NaN
            df[col] = df[col].replace({"nan": None, "None": None, "": None, "NaT": None})

        # Convert numeric-looking strings
        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    numeric = pd.to_numeric(df[col], errors="coerce")
                    # Only convert if >80% values are numeric
                    if numeric.notna().sum() > 0.8 * df[col].notna().sum():
                        df[col] = numeric
                except Exception:
                    pass

        return df
