"""
Import all models so SQLAlchemy knows about them.
"""
from app.models.rbac import Role, Permission, RolePermission, User, UserRole
from app.models.rls import Store, UserStoreAccess, UserRegionAccess, ColumnRestriction
from app.models.audit import AuditLog
from app.models.retail import (
    Division, SubDivision, MajorCategory, SizeMaster, ColorMaster,
    GenArticle, VariantArticle,
    AllocationHeader, AllocationDetail,
    StoreStock, StoreSales, WarehouseStock
)
from app.models.table_mgmt import TableRegistry, ColumnRegistry

__all__ = [
    "Role", "Permission", "RolePermission", "User", "UserRole",
    "Store", "UserStoreAccess", "UserRegionAccess", "ColumnRestriction",
    "AuditLog",
    "Division", "SubDivision", "MajorCategory", "SizeMaster", "ColorMaster",
    "GenArticle", "VariantArticle",
    "AllocationHeader", "AllocationDetail",
    "StoreStock", "StoreSales", "WarehouseStock",
    "TableRegistry", "ColumnRegistry",
]
