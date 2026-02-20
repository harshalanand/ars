"""
Global Exception Handler Middleware
"""
import traceback
from fastapi import Request, status
from fastapi.responses import JSONResponse
from loguru import logger


async def global_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and return structured error."""
    logger.error(f"Unhandled exception: {exc}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "message": "Internal server error",
            "errors": [str(exc)] if request.app.state.debug else ["An unexpected error occurred"],
        },
    )


async def request_logging_middleware(request: Request, call_next):
    """Log all incoming requests."""
    logger.info(f"{request.method} {request.url.path}")
    response = await call_next(request)
    logger.info(f"{request.method} {request.url.path} → {response.status_code}")
    return response
