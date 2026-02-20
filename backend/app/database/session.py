"""
Database Engine & Session Management for SQL Server (ODBC Driver 18)
"""
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from sqlalchemy.pool import QueuePool
from typing import Generator
from loguru import logger

from app.core.config import get_settings

settings = get_settings()


# ============================================================================
# SQLAlchemy Engine with Connection Pooling
# ============================================================================
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_timeout=settings.DB_POOL_TIMEOUT,
    pool_recycle=settings.DB_POOL_RECYCLE,
    pool_pre_ping=True,  # verify connections before use
    echo=settings.DEBUG,
    fast_executemany=True,  # critical for bulk inserts with pyodbc
)


# ============================================================================
# Session Factory
# ============================================================================
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


# ============================================================================
# Declarative Base
# ============================================================================
class Base(DeclarativeBase):
    pass


# ============================================================================
# Dependency: Get DB Session (for FastAPI)
# ============================================================================
def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency that provides a database session per request."""
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# ============================================================================
# Raw Connection Helper (for Pandas / bulk ops)
# ============================================================================
def get_raw_connection():
    """Get a raw DBAPI connection for Pandas read_sql / to_sql operations."""
    return engine.raw_connection()


def get_engine():
    """Get the SQLAlchemy engine instance."""
    return engine


# ============================================================================
# Health Check
# ============================================================================
def check_db_connection() -> bool:
    """Verify database connectivity."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


# ============================================================================
# Event Listeners
# ============================================================================
@event.listens_for(engine, "connect")
def set_connection_options(dbapi_connection, connection_record):
    """Set connection-level options after connect."""
    # Enable MARS for multiple active result sets
    pass


@event.listens_for(engine, "checkout")
def checkout_listener(dbapi_connection, connection_record, connection_proxy):
    """Log when connection is checked out from pool."""
    logger.debug("Connection checked out from pool")
