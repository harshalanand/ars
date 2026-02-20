"""
Dynamic Table Management Service
==================================
Create, alter, and manage tables from the UI.
All operations are registered in sys_table_registry and audited.
"""
import json
from typing import List, Dict, Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session
from loguru import logger

from app.models.table_mgmt import TableRegistry, ColumnRegistry
from app.audit.service import AuditService
from app.database.session import get_engine


# Allowed SQL data types (whitelist to prevent injection)
ALLOWED_DATA_TYPES = {
    "NVARCHAR", "VARCHAR", "INT", "BIGINT", "SMALLINT", "TINYINT",
    "DECIMAL", "NUMERIC", "FLOAT", "REAL",
    "BIT", "DATE", "DATETIME2", "DATETIME", "TIME",
    "UNIQUEIDENTIFIER", "NTEXT", "TEXT",
}

# Tables that cannot be altered or deleted
PROTECTED_TABLES = {
    "rbac_roles", "rbac_permissions", "rbac_role_permissions",
    "rbac_users", "rbac_user_roles",
    "rls_stores", "rls_user_store_access", "rls_user_region_access", "rls_column_restrictions",
    "audit_log", "sys_table_registry", "sys_column_registry",
}


class TableManagementService:
    """Manages dynamic table creation, alteration, and metadata."""

    def __init__(self, db: Session):
        self.db = db
        self.engine = get_engine()
        self.audit = AuditService(db)

    # ========================================================================
    # CREATE TABLE
    # ========================================================================

    def create_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        module: Optional[str] = None,
        created_by: str = "system",
    ) -> Dict[str, Any]:
        """
        Create a new SQL Server table and register it in the metadata registry.
        """
        # Validate table name
        table_name = table_name.strip()
        if table_name.lower() in PROTECTED_TABLES:
            raise ValueError(f"Cannot create table with protected name: {table_name}")

        # Check if table already exists
        existing = self.db.query(TableRegistry).filter(
            TableRegistry.table_name == table_name
        ).first()
        if existing and existing.is_active:
            raise ValueError(f"Table '{table_name}' already exists")

        # Validate columns
        if not columns:
            raise ValueError("At least one column is required")

        pk_columns = [c for c in columns if c.get("is_primary_key")]

        # Build CREATE TABLE SQL
        col_defs = []
        for col in columns:
            col_sql = self._build_column_sql(col)
            col_defs.append(col_sql)

        # Add primary key constraint
        if pk_columns:
            pk_names = ", ".join([f"[{c['column_name']}]" for c in pk_columns])
            col_defs.append(f"CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_names})")

        create_sql = f"CREATE TABLE [{table_name}] (\n  {','.join(col_defs)}\n)"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_sql))
                conn.commit()
        except Exception as e:
            raise ValueError(f"Failed to create table: {e}")

        # Register in metadata
        if existing:
            # Reactivate soft-deleted table
            existing.is_active = True
            existing.display_name = display_name or table_name
            existing.description = description
            existing.module = module
            existing.primary_key_columns = json.dumps([c["column_name"] for c in pk_columns])
            existing.created_by = created_by
            registry = existing
        else:
            registry = TableRegistry(
                table_name=table_name,
                display_name=display_name or table_name,
                description=description,
                module=module,
                primary_key_columns=json.dumps([c["column_name"] for c in pk_columns]),
                created_by=created_by,
            )
            self.db.add(registry)

        self.db.flush()

        # Register columns
        for idx, col in enumerate(columns):
            col_reg = ColumnRegistry(
                table_id=registry.id,
                column_name=col["column_name"],
                display_name=col.get("display_name", col["column_name"]),
                data_type=col["data_type"],
                max_length=col.get("max_length"),
                is_nullable=col.get("is_nullable", True),
                is_primary_key=col.get("is_primary_key", False),
                default_value=col.get("default_value"),
                column_order=col.get("column_order", idx),
            )
            self.db.add(col_reg)

        # Audit
        self.audit.log_schema_change(
            table_name=table_name,
            changed_by=created_by,
            action="CREATE_TABLE",
            details={
                "columns": [c["column_name"] for c in columns],
                "primary_keys": [c["column_name"] for c in pk_columns],
            },
        )
        self.db.commit()

        logger.info(f"Table '{table_name}' created with {len(columns)} columns by {created_by}")

        return {
            "table_name": table_name,
            "columns_created": len(columns),
            "primary_keys": [c["column_name"] for c in pk_columns],
        }

    # ========================================================================
    # ALTER TABLE
    # ========================================================================

    def alter_table(
        self,
        table_name: str,
        add_columns: Optional[List[Dict[str, Any]]] = None,
        drop_columns: Optional[List[str]] = None,
        rename_columns: Optional[Dict[str, str]] = None,
        changed_by: str = "system",
    ) -> Dict[str, Any]:
        """
        Alter an existing table: add/drop/rename columns.
        """
        if table_name.lower() in PROTECTED_TABLES:
            raise ValueError(f"Cannot alter protected table: {table_name}")

        registry = self.db.query(TableRegistry).filter(
            TableRegistry.table_name == table_name, TableRegistry.is_active == True
        ).first()
        if not registry:
            raise ValueError(f"Table '{table_name}' not found in registry")

        changes = []

        # Add columns
        if add_columns:
            for col in add_columns:
                col_sql = self._build_column_sql(col)
                alter_sql = f"ALTER TABLE [{table_name}] ADD {col_sql}"
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(alter_sql))
                        conn.commit()
                except Exception as e:
                    raise ValueError(f"Failed to add column '{col['column_name']}': {e}")

                # Register column
                col_reg = ColumnRegistry(
                    table_id=registry.id,
                    column_name=col["column_name"],
                    display_name=col.get("display_name", col["column_name"]),
                    data_type=col["data_type"],
                    max_length=col.get("max_length"),
                    is_nullable=col.get("is_nullable", True),
                    is_primary_key=False,
                    default_value=col.get("default_value"),
                )
                self.db.add(col_reg)
                changes.append(f"ADD {col['column_name']}")

        # Drop columns
        if drop_columns:
            for col_name in drop_columns:
                # Check it's not a PK
                pk_cols = json.loads(registry.primary_key_columns or "[]")
                if col_name in pk_cols:
                    raise ValueError(f"Cannot drop primary key column: {col_name}")

                alter_sql = f"ALTER TABLE [{table_name}] DROP COLUMN [{col_name}]"
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(alter_sql))
                        conn.commit()
                except Exception as e:
                    raise ValueError(f"Failed to drop column '{col_name}': {e}")

                # Soft-delete from registry
                col_reg = self.db.query(ColumnRegistry).filter(
                    ColumnRegistry.table_id == registry.id,
                    ColumnRegistry.column_name == col_name,
                ).first()
                if col_reg:
                    col_reg.is_active = False
                changes.append(f"DROP {col_name}")

        # Rename columns
        if rename_columns:
            for old_name, new_name in rename_columns.items():
                rename_sql = f"EXEC sp_rename '[{table_name}].[{old_name}]', '{new_name}', 'COLUMN'"
                try:
                    with self.engine.connect() as conn:
                        conn.execute(text(rename_sql))
                        conn.commit()
                except Exception as e:
                    raise ValueError(f"Failed to rename '{old_name}' → '{new_name}': {e}")

                # Update registry
                col_reg = self.db.query(ColumnRegistry).filter(
                    ColumnRegistry.table_id == registry.id,
                    ColumnRegistry.column_name == old_name,
                ).first()
                if col_reg:
                    col_reg.column_name = new_name
                changes.append(f"RENAME {old_name} → {new_name}")

        # Audit
        self.audit.log_schema_change(
            table_name=table_name,
            changed_by=changed_by,
            action="ALTER_TABLE",
            details={"changes": changes},
        )
        self.db.commit()

        return {"table_name": table_name, "changes": changes}

    # ========================================================================
    # SOFT DELETE TABLE
    # ========================================================================

    def soft_delete_table(self, table_name: str, deleted_by: str) -> Dict[str, Any]:
        """Soft-delete a table (mark inactive, do NOT drop from SQL Server)."""
        if table_name.lower() in PROTECTED_TABLES:
            raise ValueError(f"Cannot delete protected table: {table_name}")

        registry = self.db.query(TableRegistry).filter(
            TableRegistry.table_name == table_name, TableRegistry.is_active == True
        ).first()
        if not registry:
            raise ValueError(f"Table '{table_name}' not found")

        if registry.is_system_table:
            raise ValueError(f"Cannot delete system table: {table_name}")

        registry.is_active = False

        self.audit.log_schema_change(
            table_name=table_name,
            changed_by=deleted_by,
            action="SOFT_DELETE_TABLE",
            details={"table_name": table_name},
        )
        self.db.commit()

        return {"table_name": table_name, "status": "soft_deleted"}

    # ========================================================================
    # TABLE METADATA & SCHEMA VIEWER
    # ========================================================================

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get full metadata for a table from both registry and INFORMATION_SCHEMA."""
        registry = self.db.query(TableRegistry).filter(
            TableRegistry.table_name == table_name, TableRegistry.is_active == True
        ).first()

        # Get live schema from SQL Server
        sql = text("""
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PK
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.TABLE_NAME = :table_name
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_NAME = :table_name
            ORDER BY c.ORDINAL_POSITION
        """)

        with self.engine.connect() as conn:
            result = conn.execute(sql, {"table_name": table_name})
            columns = []
            for row in result:
                columns.append({
                    "column_name": row[0],
                    "data_type": row[1],
                    "max_length": row[2],
                    "numeric_precision": row[3],
                    "numeric_scale": row[4],
                    "is_nullable": row[5] == "YES",
                    "default_value": row[6],
                    "is_primary_key": bool(row[7]),
                    "display_name": None,
                })

        if not columns:
            raise ValueError(f"Table '{table_name}' not found in database")

        # Enrich with registry display names
        if registry:
            reg_cols = {c.column_name: c for c in registry.columns if c.is_active}
            for col in columns:
                rc = reg_cols.get(col["column_name"])
                if rc:
                    col["display_name"] = rc.display_name

        # Get row count
        count_sql = text(f"SELECT COUNT(*) FROM [{table_name}]")
        with self.engine.connect() as conn:
            row_count = conn.execute(count_sql).scalar()

        return {
            "table_name": table_name,
            "display_name": registry.display_name if registry else table_name,
            "description": registry.description if registry else None,
            "module": registry.module if registry else None,
            "is_system_table": registry.is_system_table if registry else False,
            "is_active": registry.is_active if registry else True,
            "row_count": row_count,
            "columns": columns,
            "created_at": registry.created_at.isoformat() if registry else None,
            "created_by": registry.created_by if registry else None,
        }

    def list_tables(self, module: Optional[str] = None, include_system: bool = False) -> List[Dict]:
        """List all registered tables."""
        query = self.db.query(TableRegistry).filter(TableRegistry.is_active == True)
        if module:
            query = query.filter(TableRegistry.module == module)
        if not include_system:
            query = query.filter(TableRegistry.is_system_table == False)

        tables = query.order_by(TableRegistry.table_name).all()

        return [
            {
                "id": t.id,
                "table_name": t.table_name,
                "display_name": t.display_name,
                "description": t.description,
                "module": t.module,
                "is_system_table": t.is_system_table,
                "row_count": t.row_count,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "created_by": t.created_by,
            }
            for t in tables
        ]

    def list_all_database_tables(self) -> List[Dict]:
        """List all tables from SQL Server INFORMATION_SCHEMA (not just registered)."""
        sql = text("""
            SELECT t.TABLE_NAME, p.rows as row_count
            FROM INFORMATION_SCHEMA.TABLES t
            LEFT JOIN sys.partitions p
                ON OBJECT_ID(t.TABLE_NAME) = p.object_id AND p.index_id IN (0, 1)
            WHERE t.TABLE_TYPE = 'BASE TABLE'
            ORDER BY t.TABLE_NAME
        """)
        with self.engine.connect() as conn:
            result = conn.execute(sql)
            return [{"table_name": row[0], "row_count": row[1] or 0} for row in result]

    # ========================================================================
    # DATA QUERY (Generic)
    # ========================================================================

    def query_table_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_dir: str = "ASC",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Generic paginated data query for any table.
        Used by the editable data grid frontend.
        """
        # Validate table exists
        check_sql = text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = :table_name
        """)
        with self.engine.connect() as conn:
            exists = conn.execute(check_sql, {"table_name": table_name}).scalar()
        if not exists:
            raise ValueError(f"Table '{table_name}' not found")

        # Build SELECT
        col_list = ", ".join([f"[{c}]" for c in columns]) if columns else "*"

        # Build WHERE
        where_clause = ""
        params = {}
        if filters:
            conditions = []
            for idx, (col, val) in enumerate(filters.items()):
                param_name = f"f{idx}"
                if isinstance(val, str) and "%" in val:
                    conditions.append(f"[{col}] LIKE :{param_name}")
                elif val is None:
                    conditions.append(f"[{col}] IS NULL")
                    continue
                else:
                    conditions.append(f"[{col}] = :{param_name}")
                params[param_name] = val
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

        # Count total
        count_sql = text(f"SELECT COUNT(*) FROM [{table_name}] {where_clause}")
        with self.engine.connect() as conn:
            total = conn.execute(count_sql, params).scalar()

        # Order
        order_clause = ""
        if order_by:
            direction = "DESC" if order_dir.upper() == "DESC" else "ASC"
            order_clause = f"ORDER BY [{order_by}] {direction}"
        else:
            order_clause = "ORDER BY (SELECT NULL)"

        # Paginate with OFFSET/FETCH
        offset = (page - 1) * page_size
        data_sql = text(f"""
            SELECT {col_list} FROM [{table_name}]
            {where_clause}
            {order_clause}
            OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
        """)
        params["offset"] = offset
        params["page_size"] = page_size

        with self.engine.connect() as conn:
            result = conn.execute(data_sql, params)
            col_names = list(result.keys())
            rows = [dict(zip(col_names, row)) for row in result]

        return {
            "table_name": table_name,
            "columns": col_names,
            "data": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    # ========================================================================
    # TRUNCATE TABLE DATA (not drop)
    # ========================================================================

    def truncate_table_data(self, table_name: str, deleted_by: str) -> Dict[str, Any]:
        """Delete all data from a table (TRUNCATE) without dropping the table."""
        if table_name.lower() in PROTECTED_TABLES:
            raise ValueError(f"Cannot truncate protected table: {table_name}")

        # Get row count before truncate
        count_sql = text(f"SELECT COUNT(*) FROM [{table_name}]")
        with self.engine.connect() as conn:
            row_count = conn.execute(count_sql).scalar()

        # Use DELETE instead of TRUNCATE to avoid FK issues
        delete_sql = text(f"DELETE FROM [{table_name}]")
        with self.engine.connect() as conn:
            conn.execute(delete_sql)
            conn.commit()

        self.audit.log(
            table_name=table_name,
            action_type="DELETE",
            changed_by=deleted_by,
            notes=f"Table data truncated. {row_count} rows deleted.",
            row_count=row_count,
        )
        self.db.commit()

        return {"table_name": table_name, "rows_deleted": row_count}

    # ========================================================================
    # HELPERS
    # ========================================================================

    def _build_column_sql(self, col: Dict[str, Any]) -> str:
        """Build SQL column definition from column dict."""
        name = col["column_name"]
        data_type = col["data_type"].upper()

        if data_type not in ALLOWED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}. Allowed: {ALLOWED_DATA_TYPES}")

        # Build type with length/precision
        if data_type in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR"):
            length = col.get("max_length", 255)
            type_sql = f"{data_type}({length})"
        elif data_type in ("DECIMAL", "NUMERIC"):
            precision = col.get("max_length", 18)
            scale = 2  # default scale
            type_sql = f"{data_type}({precision},{scale})"
        else:
            type_sql = data_type

        # Nullable
        null_sql = "NULL" if col.get("is_nullable", True) else "NOT NULL"

        # Default
        default_sql = ""
        if col.get("default_value"):
            default_sql = f"DEFAULT {col['default_value']}"

        return f"[{name}] {type_sql} {null_sql} {default_sql}".strip()
