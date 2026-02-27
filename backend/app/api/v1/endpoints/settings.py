"""
Application Settings API Endpoints
- System configuration
- Database settings
- Email configuration
- Application preferences
- Backup management
"""
import json
import os
import subprocess
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.database.session import get_db, get_data_engine, get_system_engine
from app.schemas.common import APIResponse
from app.security.dependencies import get_current_user, RequirePermissions
from app.models.rbac import User
from app.core.config import get_settings

router = APIRouter(prefix="/settings", tags=["Settings"])

settings = get_settings()

# Settings file path
SETTINGS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "app_settings.json")
# Backup directory
BACKUP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "backups")


def load_app_settings() -> Dict[str, Any]:
    """Load application settings from file."""
    default_settings = {
        "database": {
            "data_database": settings.DATA_DB_NAME or "Rep_data",
            "system_database": settings.DB_NAME or "Claude",
            "server": settings.DB_SERVER or "",
        },
        "email": {
            "smtp_server": "",
            "smtp_port": 587,
            "smtp_username": "",
            "smtp_password": "",
            "from_address": "",
            "use_tls": True,
            "notifications_enabled": False,
        },
        "application": {
            "app_name": "ARS - Allocation & Reporting System",
            "max_upload_size_mb": settings.MAX_UPLOAD_SIZE_MB,
            "session_timeout_minutes": 60,
            "enable_audit_logging": True,
            "enable_row_level_security": True,
            "default_page_size": 50,
            "max_export_rows": 500000,
        },
        "ui": {
            "primary_color": "#4f46e5",
            "sidebar_collapsed": False,
            "show_row_numbers": True,
            "date_format": "YYYY-MM-DD",
            "number_format": "en-US",
        },
    }
    
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                saved = json.load(f)
                # Merge saved settings with defaults
                for category, values in saved.items():
                    if category in default_settings:
                        default_settings[category].update(values)
                    else:
                        default_settings[category] = values
        except:
            pass
    
    return default_settings


def save_app_settings(settings_dict: Dict[str, Any]) -> bool:
    """Save application settings to file."""
    try:
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings_dict, f, indent=2)
        return True
    except Exception as e:
        raise ValueError(f"Failed to save settings: {e}")


# ============================================================================
# Get Settings
# ============================================================================

@router.get("", response_model=APIResponse)
async def get_all_settings(
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Get all application settings."""
    settings_data = load_app_settings()
    # Mask sensitive data
    if settings_data.get("email", {}).get("smtp_password"):
        settings_data["email"]["smtp_password"] = "********"
    return APIResponse(data=settings_data)


# ============================================================================
# Update Settings
# ============================================================================

class UpdateSettingsRequest(BaseModel):
    category: str
    settings: Dict[str, Any]


@router.put("", response_model=APIResponse)
async def update_settings(
    body: UpdateSettingsRequest,
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Update settings for a category."""
    all_settings = load_app_settings()
    
    if body.category not in all_settings:
        all_settings[body.category] = {}
    
    # Don't update password if it's masked
    if body.category == "email" and body.settings.get("smtp_password") == "********":
        body.settings["smtp_password"] = all_settings.get("email", {}).get("smtp_password", "")
    
    all_settings[body.category].update(body.settings)
    save_app_settings(all_settings)
    
    return APIResponse(data=all_settings[body.category], message="Settings updated successfully")


# ============================================================================
# Database Connection Test
# ============================================================================

@router.post("/test-connection", response_model=APIResponse)
async def test_database_connection(
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Test both database connections (System DB and Data DB)."""
    results = {
        "system_db": {"status": "disconnected", "database": None, "error": None},
        "data_db": {"status": "disconnected", "database": None, "error": None},
    }
    
    # Test System Database (Claude)
    try:
        engine = get_system_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION as version, DB_NAME() as database_name"))
            row = result.fetchone()
            results["system_db"] = {
                "status": "connected",
                "database": row[1],
                "server_version": row[0][:80] if row[0] else None,
            }
    except Exception as e:
        results["system_db"]["error"] = str(e)[:200]
    
    # Test Data Database (Rep_data)
    try:
        engine = get_data_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION as version, DB_NAME() as database_name"))
            row = result.fetchone()
            results["data_db"] = {
                "status": "connected",
                "database": row[1],
                "server_version": row[0][:80] if row[0] else None,
            }
    except Exception as e:
        results["data_db"]["error"] = str(e)[:200]
    
    all_connected = results["system_db"]["status"] == "connected" and results["data_db"]["status"] == "connected"
    
    return APIResponse(
        data=results,
        message="All databases connected successfully" if all_connected else "Some database connections failed"
    )


# ============================================================================
# Email Test
# ============================================================================

class TestEmailRequest(BaseModel):
    to_address: str


@router.post("/test-email", response_model=APIResponse)
async def test_email(
    body: TestEmailRequest,
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Send a test email."""
    import smtplib
    from email.mime.text import MIMEText
    
    settings_data = load_app_settings()
    email_config = settings_data.get("email", {})
    
    if not email_config.get("smtp_server"):
        raise HTTPException(status_code=400, detail="SMTP server not configured")
    
    try:
        msg = MIMEText("This is a test email from ARS Application.")
        msg['Subject'] = "ARS Test Email"
        msg['From'] = email_config.get("from_address", "noreply@ars.local")
        msg['To'] = body.to_address
        
        server = smtplib.SMTP(email_config["smtp_server"], email_config.get("smtp_port", 587))
        if email_config.get("use_tls"):
            server.starttls()
        if email_config.get("smtp_username") and email_config.get("smtp_password"):
            server.login(email_config["smtp_username"], email_config["smtp_password"])
        server.send_message(msg)
        server.quit()
        
        return APIResponse(message=f"Test email sent to {body.to_address}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Email failed: {str(e)}")


# ============================================================================
# System Info
# ============================================================================

@router.get("/system/info", response_model=APIResponse)
async def get_system_info(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get comprehensive system information."""
    import platform
    import sys
    import psutil
    from datetime import datetime, timedelta
    
    # System metrics
    try:
        cpu_percent = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
    except:
        cpu_percent = 0
        memory = None
        disk = None
        uptime = timedelta(0)
    
    # Data database stats
    try:
        engine = get_data_engine()
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as table_count,
                    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS) as column_count
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            """))
            row = result.fetchone()
            data_table_count = row[0] if row else 0
            data_column_count = row[1] if row else 0
            
            # Get database size
            size_result = conn.execute(text("""
                SELECT 
                    SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT) * 8 / 1024) as size_mb
                FROM sys.database_files
            """))
            size_row = size_result.fetchone()
            data_db_size_mb = size_row[0] if size_row and size_row[0] else 0
    except:
        data_table_count = 0
        data_column_count = 0
        data_db_size_mb = 0
    
    # System database stats
    try:
        engine = get_system_engine()
        with engine.connect() as conn:
            # Get database size
            size_result = conn.execute(text("""
                SELECT 
                    SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT) * 8 / 1024) as size_mb
                FROM sys.database_files
            """))
            size_row = size_result.fetchone()
            system_db_size_mb = size_row[0] if size_row and size_row[0] else 0
    except:
        system_db_size_mb = 0
    
    # Active users (logged in within last 24 hours)
    try:
        active_users_result = db.execute(text("""
            SELECT COUNT(*) FROM users 
            WHERE last_login >= DATEADD(day, -1, GETDATE()) AND is_active = 1
        """))
        active_users = active_users_result.scalar() or 0
        
        total_users_result = db.execute(text("SELECT COUNT(*) FROM users WHERE is_active = 1"))
        total_users = total_users_result.scalar() or 0
    except:
        active_users = 0
        total_users = 0
    
    return APIResponse(data={
        # Server info
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "hostname": platform.node(),
        "processor": platform.processor() or "Unknown",
        
        # Resource usage
        "cpu_percent": cpu_percent,
        "memory_total_gb": round(memory.total / (1024**3), 2) if memory else 0,
        "memory_used_gb": round(memory.used / (1024**3), 2) if memory else 0,
        "memory_percent": memory.percent if memory else 0,
        "disk_total_gb": round(disk.total / (1024**3), 2) if disk else 0,
        "disk_used_gb": round(disk.used / (1024**3), 2) if disk else 0,
        "disk_percent": disk.percent if disk else 0,
        
        # Uptime
        "uptime_days": uptime.days,
        "uptime_hours": uptime.seconds // 3600,
        "uptime_formatted": f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m",
        
        # Database stats
        "data_db": {
            "tables": data_table_count,
            "columns": data_column_count,
            "size_mb": data_db_size_mb,
        },
        "system_db": {
            "size_mb": system_db_size_mb,
        },
        
        # User stats
        "active_users_24h": active_users,
        "total_users": total_users,
        
        # Current user
        "current_user": current_user.username,
    })


# ============================================================================
# Database Backup
# ============================================================================

class BackupRequest(BaseModel):
    database: str = Field(..., description="Which database to backup: 'system', 'data', or 'both'")


@router.post("/backup/create", response_model=APIResponse)
async def create_backup(
    body: BackupRequest,
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Create database backup."""
    # Ensure backup directory exists
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = []
    
    if body.database in ['system', 'both']:
        try:
            backup_file = os.path.join(BACKUP_DIR, f"system_db_{timestamp}.bak")
            engine = get_system_engine()
            db_name = settings.SQL_DATABASE or "Claude"
            with engine.connect() as conn:
                conn.execute(text(f"""
                    BACKUP DATABASE [{db_name}] 
                    TO DISK = N'{backup_file}' 
                    WITH FORMAT, INIT, COMPRESSION,
                    NAME = N'System DB Backup - {timestamp}'
                """))
                conn.commit()
            results.append({"database": "system", "status": "success", "file": backup_file})
        except Exception as e:
            results.append({"database": "system", "status": "failed", "error": str(e)[:200]})
    
    if body.database in ['data', 'both']:
        try:
            backup_file = os.path.join(BACKUP_DIR, f"data_db_{timestamp}.bak")
            engine = get_data_engine()
            db_name = settings.DATA_DATABASE or "Rep_data"
            with engine.connect() as conn:
                conn.execute(text(f"""
                    BACKUP DATABASE [{db_name}] 
                    TO DISK = N'{backup_file}' 
                    WITH FORMAT, INIT, COMPRESSION,
                    NAME = N'Data DB Backup - {timestamp}'
                """))
                conn.commit()
            results.append({"database": "data", "status": "success", "file": backup_file})
        except Exception as e:
            results.append({"database": "data", "status": "failed", "error": str(e)[:200]})
    
    success_count = sum(1 for r in results if r["status"] == "success")
    
    return APIResponse(
        data={"backups": results, "backup_dir": BACKUP_DIR},
        message=f"{success_count} backup(s) created successfully"
    )


@router.get("/backup/list", response_model=APIResponse)
async def list_backups(
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """List available backups."""
    if not os.path.exists(BACKUP_DIR):
        return APIResponse(data={"backups": [], "backup_dir": BACKUP_DIR})
    
    backups = []
    for filename in os.listdir(BACKUP_DIR):
        if filename.endswith('.bak'):
            filepath = os.path.join(BACKUP_DIR, filename)
            stat = os.stat(filepath)
            backups.append({
                "filename": filename,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "database": "system" if filename.startswith("system_") else "data",
            })
    
    backups.sort(key=lambda x: x["created"], reverse=True)
    
    return APIResponse(data={"backups": backups, "backup_dir": BACKUP_DIR})


@router.delete("/backup/{filename}", response_model=APIResponse)
async def delete_backup(
    filename: str,
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Delete a backup file."""
    if not filename.endswith('.bak'):
        raise HTTPException(status_code=400, detail="Invalid backup file")
    
    filepath = os.path.join(BACKUP_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="Backup not found")
    
    os.remove(filepath)
    return APIResponse(message=f"Backup '{filename}' deleted successfully")


# ============================================================================
# Get Settings by Category (MUST BE LAST - catches all paths)
# ============================================================================

@router.get("/{category}", response_model=APIResponse)
async def get_settings_category(
    category: str,
    current_user: User = Depends(get_current_user),
    _: User = Depends(RequirePermissions(["ADMIN_SETTINGS"])),
):
    """Get settings for a specific category."""
    settings_data = load_app_settings()
    if category not in settings_data:
        raise HTTPException(status_code=404, detail=f"Category '{category}' not found")
    
    result = settings_data[category]
    # Mask sensitive data
    if category == "email" and result.get("smtp_password"):
        result["smtp_password"] = "********"
    
    return APIResponse(data=result)
