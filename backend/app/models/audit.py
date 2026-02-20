"""
Audit Log Model
"""
from datetime import datetime
from sqlalchemy import Column, BigInteger, Integer, String, DateTime, Text
from app.database.session import Base


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_name = Column(String(200), nullable=False, index=True)
    action_type = Column(String(50), nullable=False)  # INSERT, UPDATE, DELETE, UPSERT, BULK_UPLOAD, SCHEMA_CHANGE
    record_primary_key = Column(String(500))
    old_data = Column(Text)       # JSON
    new_data = Column(Text)       # JSON
    changed_columns = Column(Text) # JSON array
    changed_by = Column(String(100), nullable=False, index=True)
    changed_at = Column(DateTime, default=datetime.utcnow, index=True)
    source = Column(String(50), default="API")  # UI, API, UPLOAD, SYSTEM
    ip_address = Column(String(50))
    user_agent = Column(String(500))
    session_id = Column(String(200))
    batch_id = Column(String(100), index=True)
    duration_ms = Column(Integer)
    row_count = Column(Integer, default=1)
    notes = Column(String(1000))
