"""
Retail Listing & Allocation System - FastAPI Application
=========================================================
Enterprise-grade backend for multi-store retail management.
"""
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.core.config import get_settings
from app.database.session import check_db_connection, check_data_db_connection, SessionLocal
from app.api.v1.router import api_router
from app.middleware.exception_handler import global_exception_handler, request_logging_middleware

settings = get_settings()

# ============================================================================
# Logging Configuration
# ============================================================================
logger.remove()
logger.add(sys.stderr, level=settings.LOG_LEVEL, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
os.makedirs("logs", exist_ok=True)
logger.add(
    settings.LOG_FILE,
    rotation="10 MB",
    retention="30 days",
    level=settings.LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
)


# ============================================================================
# Application Lifespan
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")

    # Check database connection
    if check_db_connection():
        logger.info("✅ Database connection successful")
    else:
        logger.error("❌ Database connection failed!")

    # Create super admin if needed
    try:
        from app.services.auth_service import create_super_admin_if_needed
        db = SessionLocal()
        try:
            create_super_admin_if_needed(db)
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"Super admin bootstrap skipped: {e}")

    logger.info(f"✅ {settings.APP_NAME} started on {settings.HOST}:{settings.PORT}")
    yield
    logger.info(f"Shutting down {settings.APP_NAME}")


# ============================================================================
# Create FastAPI App
# ============================================================================
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Enterprise Retail Listing & Allocation Management System",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    redirect_slashes=False,
)

# Store debug flag for exception handler
app.state.debug = settings.DEBUG

# ============================================================================
# Middleware
# ============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.middleware("http")(request_logging_middleware)
app.add_exception_handler(Exception, global_exception_handler)

# ============================================================================
# Routes
# ============================================================================
app.include_router(api_router)


@app.get("/", tags=["Health"])
async def root():
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs",
    }


@app.get("/health", tags=["Health"])
async def health_check():
    db_ok = check_db_connection()
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "version": settings.APP_VERSION,
    }


# ============================================================================
# Entry Point
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        workers=1 if settings.DEBUG else 4,
    )
