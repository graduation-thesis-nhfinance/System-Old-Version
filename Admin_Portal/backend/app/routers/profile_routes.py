import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.routers.audit_routes import append_audit_log

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/profiles", tags=["Profiles & Access Control"])

TZ_VN = timezone(timedelta(hours=7))

PERMISSIONS_CATALOG = [
    {
        "key": "view_matrix",
        "name": "Control Matrix",
        "category": "General",
        "description": "Access 24-slot real-time pipeline status and system health matrix."
    },
    {
        "key": "trigger_pipeline",
        "name": "Trigger & Rerun Pipelines",
        "category": "Operations",
        "description": "Dispatch Airflow DAG runs, rerun failed slots, and execute backfills."
    },
    {
        "key": "manage_trading_pairs",
        "name": "Trading Pairs Registry",
        "category": "Metadata",
        "description": "Add, modify, toggle active/inactive, or delete pairs in dim_trading_pairs."
    },
    {
        "key": "manage_catalog",
        "name": "Data Catalog & Dictionary",
        "category": "Metadata",
        "description": "Edit schema technical metadata, field descriptions, and business meaning."
    },
    {
        "key": "manage_profiles",
        "name": "Security & Profile Provisioning",
        "category": "Security",
        "description": "Create operator credentials, assign roles, and grant/revoke granular permissions (Admin only)."
    },
    {
        "key": "view_lakehouse",
        "name": "Lakehouse Data Explorer",
        "category": "Data Access",
        "description": "Query ClickHouse analytical tables and inspect MinIO object storage artifacts."
    },
    {
        "key": "manage_stream",
        "name": "WSIS Ingestion Stream",
        "category": "Operations",
        "description": "Monitor and control real-time WebSocket ingestion feeds and reconnect streams."
    },
    {
        "key": "view_audit",
        "name": "Audit Trail Logs",
        "category": "Security",
        "description": "Inspect user action logs, system mutations, and administrative event history."
    },
    {
        "key": "view_system_links",
        "name": "Cross-System Portals & External Links Directory",
        "category": "Security",
        "description": "Store, access, and direct-navigate to internal/external systems across the enterprise ecosystem."
    },
    {
        "key": "view_raw_urls",
        "name": "Sensitive Network & Stream URLs",
        "category": "Security",
        "description": "Inspect and copy raw WebSocket streams, REST endpoints, and internal container networking URLs."
    },
    {
        "key": "view_db_architecture",
        "name": "Database Schema Architecture",
        "category": "Data Access",
        "description": "View internal DDL schemas, physical table partition keys, and storage connection parameters."
    },
    {
        "key": "export_lakehouse_data",
        "name": "Lakehouse Artifact Export",
        "category": "Data Access",
        "description": "Export query results, download raw MinIO Bronze/Silver JSONL/Parquet artifacts."
    },
    {
        "key": "manage_system_alerts",
        "name": "Alerts & Incident Operations",
        "category": "Operations",
        "description": "Acknowledge active alerts, silence notification webhooks, and trigger incident tests."
    },
    {
        "key": "inspect_reconciliation_details",
        "name": "Deep Reconciliation Inspector",
        "category": "Operations",
        "description": "Access granular tumbling window payload variances, tolerance math, and mismatch breakdown."
    },
    {
        "key": "view_ai_models",
        "name": "AI Models & MLOps Governance",
        "category": "AI & MLOps",
        "description": "Access SYS-04 MLOps Center, monitor model performance, drift metrics, and multi-horizon forecasts."
    },
    {
        "key": "trigger_ai_inference",
        "name": "Trigger Model Online Inference",
        "category": "AI & MLOps",
        "description": "Execute ad-hoc LSTM price forecasting, microstructure anomaly evaluation, and transaction fraud scoring."
    },
    {
        "key": "manage_ai_governance",
        "name": "AI Model Governance & Reload",
        "category": "AI & MLOps",
        "description": "Hot-reload trained model artifacts, override fraud risk threshold gates, and configure risk guard rules."
    }
]

PROFILES_FILE = Path(__file__).resolve().parent.parent / "data" / "profiles.json"
ROLES_FILE = Path(__file__).resolve().parent.parent / "data" / "roles.json"

DEFAULT_SYSTEM_ROLES = [
    {
        "id": "role-super-admin",
        "name": "Super Admin",
        "description": "Full Root Privilege & Infrastructure Control across all systems",
        "badge_class": "bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/30",
        "icon": "fa-shield-halved text-purple-500",
        "default_permissions": [c["key"] for c in PERMISSIONS_CATALOG],
        "is_system": True
    },
    {
        "id": "role-data-ai-engineer",
        "name": "Data & AI Engineer",
        "description": "ETL Pipelines, Schemas, Lakehouse Sync, AI Model Inference & MLOps Governance",
        "badge_class": "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/30",
        "icon": "fa-database text-blue-500",
        "default_permissions": [
            "view_matrix", "trigger_pipeline", "manage_trading_pairs", "manage_catalog",
            "view_lakehouse", "manage_stream", "view_audit", "view_system_links", "view_raw_urls", "view_db_architecture",
            "export_lakehouse_data", "manage_system_alerts", "inspect_reconciliation_details",
            "view_ai_models", "trigger_ai_inference", "manage_ai_governance"
        ],
        "is_system": True
    },
    {
        "id": "role-operator",
        "name": "Operator",
        "description": "Operational Pipeline Monitoring, Service Health, Stream Control & Online Inference",
        "badge_class": "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/30",
        "icon": "fa-bolt text-emerald-500",
        "default_permissions": [
            "view_matrix", "trigger_pipeline", "view_lakehouse", "manage_stream",
            "manage_system_alerts", "inspect_reconciliation_details",
            "view_ai_models", "trigger_ai_inference"
        ],
        "is_system": True
    },
    {
        "id": "role-viewer",
        "name": "Viewer",
        "description": "Read-Only Business Analytics, Pipeline Monitoring & Model Forecasts",
        "badge_class": "bg-cyan-500/10 text-cyan-600 dark:text-cyan-400 border-cyan-500/30",
        "icon": "fa-eye text-cyan-500",
        "default_permissions": [
            "view_matrix", "view_lakehouse", "view_ai_models"
        ],
        "is_system": True
    },
    {
        "id": "role-service-account",
        "name": "Service Account",
        "description": "Headless Automation Token for Automated Triggers & Cron Probes",
        "badge_class": "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/30",
        "icon": "fa-robot text-amber-500",
        "default_permissions": [
            "view_matrix", "trigger_pipeline", "view_lakehouse", "trigger_ai_inference"
        ],
        "is_system": True
    }
]

def load_roles() -> List[Dict[str, Any]]:
    if ROLES_FILE.exists():
        try:
            with open(ROLES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read roles.json: %s", e)
    return list(DEFAULT_SYSTEM_ROLES)

def save_roles(roles: List[Dict[str, Any]]):
    try:
        ROLES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(ROLES_FILE, "w", encoding="utf-8") as f:
            json.dump(roles, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save roles.json: %s", e)

def register_role_if_not_exists(role_name: str, permissions: List[str] = None):
    if not role_name:
        return
    roles = load_roles()
    existing_names = {r.get("name", "").strip().lower() for r in roles}
    if role_name.strip().lower() not in existing_names:
        new_role = {
            "id": f"role-{role_name.lower().replace(' ', '-')}",
            "name": role_name.strip(),
            "description": f"Custom Role ({role_name}) configured dynamically",
            "badge_class": "bg-indigo-500/10 text-indigo-600 dark:text-indigo-400 border-indigo-500/30",
            "icon": "fa-user-tag text-indigo-500",
            "default_permissions": permissions or ["view_matrix", "view_lakehouse"],
            "is_system": False
        }
        roles.append(new_role)
        save_roles(roles)

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def verify_password(plain_password: str, hashed_or_plain: str) -> bool:
    if plain_password == hashed_or_plain:
        return True
    return hash_password(plain_password) == hashed_or_plain

def load_profiles() -> List[Dict[str, Any]]:
    if PROFILES_FILE.exists():
        try:
            with open(PROFILES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read profiles.json: %s", e)
    return []

def save_profiles(profiles: List[Dict[str, Any]]):
    try:
        PROFILES_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(PROFILES_FILE, "w", encoding="utf-8") as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save profiles.json: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to persist profiles: {str(e)}")

def sanitize_profile(p: Dict[str, Any]) -> Dict[str, Any]:
    """Strip password_hash from client responses"""
    clean = p.copy()
    clean.pop("password_hash", None)
    return clean

class CreateProfileRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Unique username identifier")
    full_name: str = Field(..., min_length=2, max_length=100, description="Full name or department title")
    email: str = Field(..., description="Email address")
    password: str = Field(..., min_length=4, description="Initial account password")
    role: str = Field(default="Operator", description="Primary role: Super Admin, Data Engineer, Operator, Viewer, Service Account")
    permissions: List[str] = Field(default_factory=list, description="Assigned granular permissions")
    status: str = Field(default="ACTIVE", description="Account status: ACTIVE, INACTIVE, or SUSPENDED")
    notes: Optional[str] = Field(default="", description="Optional account notes or department description")

class UpdateProfileRequest(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    role: Optional[str] = None
    permissions: Optional[List[str]] = None
    status: Optional[str] = None
    notes: Optional[str] = None

def check_admin_permission(current_user: dict):
    user_role = current_user.get("role", "")
    perms = current_user.get("permissions", [])
    if user_role not in ["Super Admin", "Administrator"] and "manage_profiles" not in perms and "*" not in perms:
        raise HTTPException(status_code=403, detail="Access Denied: Only Administrator can access operator profiles")

@router.get("", response_model=Dict[str, Any])
def list_profiles(
    search: Optional[str] = Query(None, description="Search by username, full name, or email"),
    role: Optional[str] = Query(None, description="Filter by role"),
    status: Optional[str] = Query(None, description="Filter by status"),
    current_user: dict = Depends(get_current_user)
):
    """List all profiles, credential summaries, role distributions, and permissions catalog. Restricted to Admin."""
    check_admin_permission(current_user)
    profiles = load_profiles()
    
    # Filter
    filtered = profiles
    if search:
        s = search.strip().lower()
        filtered = [
            p for p in filtered
            if s in p.get("username", "").lower()
            or s in p.get("full_name", "").lower()
            or s in p.get("email", "").lower()
        ]
    if role and role != "ALL":
        filtered = [p for p in filtered if p.get("role", "").lower() == role.lower()]
    if status and status != "ALL":
        filtered = [p for p in filtered if p.get("status", "").upper() == status.upper()]
        
    # Stats
    total = len(profiles)
    active = sum(1 for p in profiles if p.get("status") == "ACTIVE")
    role_counts: Dict[str, int] = {}
    roles = load_roles()
    for r in roles:
        r["user_count"] = role_counts.get(r["name"], 0)

    return {
        "profiles": [sanitize_profile(p) for p in filtered],
        "total": total,
        "active": active,
        "role_counts": role_counts,
        "roles_catalog": roles,
        "permissions_catalog": PERMISSIONS_CATALOG
    }

class CreateRoleRequest(BaseModel):
    name: str = Field(..., min_length=2, max_length=50)
    description: Optional[str] = ""
    default_permissions: List[str] = Field(default_factory=list)

@router.get("/roles/catalog", response_model=List[Dict[str, Any]])
def get_roles_catalog(current_user: dict = Depends(get_current_user)):
    check_admin_permission(current_user)
    roles = load_roles()
    profiles = load_profiles()
    counts = {}
    for p in profiles:
        r = p.get("role", "Operator")
        counts[r] = counts.get(r, 0) + 1
    for r in roles:
        r["user_count"] = counts.get(r["name"], 0)
    return roles

@router.post("/roles", response_model=Dict[str, Any])
def create_custom_role(req: CreateRoleRequest, current_user: dict = Depends(get_current_user)):
    check_admin_permission(current_user)
    roles = load_roles()
    for r in roles:
        if r.get("name", "").strip().lower() == req.name.strip().lower():
            raise HTTPException(status_code=400, detail="Role name already exists")
    new_role = {
        "id": f"role-{req.name.lower().replace(' ', '-')}",
        "name": req.name.strip(),
        "description": req.description or f"Custom Role for {req.name}",
        "badge_class": "bg-indigo-500/10 text-indigo-600 dark:text-indigo-400 border-indigo-500/30",
        "icon": "fa-user-tag text-indigo-500",
        "default_permissions": req.default_permissions or ["view_matrix", "view_lakehouse"],
        "is_system": False
    }
    roles.append(new_role)
    save_roles(roles)
    return new_role

@router.get("/{profile_id}", response_model=Dict[str, Any])
def get_profile(
    profile_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Retrieve specific profile by ID or username. Restricted to Admin."""
    check_admin_permission(current_user)
    profiles = load_profiles()
    for p in profiles:
        if p.get("id") == profile_id or p.get("username") == profile_id:
            return sanitize_profile(p)
    raise HTTPException(status_code=404, detail=f"Profile {profile_id} not found")

@router.post("", response_model=Dict[str, Any])
def create_profile(
    req: CreateProfileRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a new credential profile with assigned permissions. Restricted to Super Admin / Admin users."""
    user_role = current_user.get("role", "")
    username_actor = current_user.get("sub", "admin")
    
    # Check permissions
    if user_role not in ["Super Admin", "Administrator"]:
        # check if token has manage_profiles permission
        perms = current_user.get("permissions", [])
        if "manage_profiles" not in perms and "*" not in perms:
            raise HTTPException(status_code=403, detail="Permission denied: Requires 'manage_profiles' or Super Admin privilege")

    profiles = load_profiles()
    req_username = req.username.strip().lower()
    
    # Check duplicate
    for p in profiles:
        if p.get("username", "").lower() == req_username:
            raise HTTPException(status_code=400, detail=f"Username '{req.username}' is already taken")
        if p.get("email", "").lower() == req.email.strip().lower():
            raise HTTPException(status_code=400, detail=f"Email '{req.email}' is already registered")

    new_id = f"prof-{len(profiles) + 1:02d}-{int(datetime.now().timestamp()) % 10000}"
    now_str = datetime.now(TZ_VN).strftime("%Y-%m-%d %H:%M:%S")
    
    # Validate permissions against catalog
    valid_keys = {c["key"] for c in PERMISSIONS_CATALOG}
    assigned_perms = [k for k in req.permissions if k in valid_keys]
    if req.role in ["Super Admin", "Administrator"] and not assigned_perms:
        assigned_perms = list(valid_keys)

    register_role_if_not_exists(req.role, assigned_perms)

    new_profile = {
        "id": new_id,
        "username": req.username.strip(),
        "full_name": req.full_name.strip(),
        "email": req.email.strip(),
        "password_hash": hash_password(req.password),
        "role": req.role,
        "permissions": assigned_perms,
        "status": req.status.upper(),
        "is_system": False,
        "created_at": now_str,
        "last_login": "Never",
        "notes": req.notes.strip() if req.notes else ""
    }

    profiles.append(new_profile)
    save_profiles(profiles)

    # Log to audit trail
    append_audit_log({
        "action": "PROFILE_CREATE",
        "target": f"User: {req.username} ({req.role})",
        "operator": username_actor,
        "status": "SUCCESS",
        "detail": f"Created profile '{req.username}' with role '{req.role}' and {len(assigned_perms)} permissions."
    })

    return sanitize_profile(new_profile)

@router.put("/{profile_id}", response_model=Dict[str, Any])
def update_profile(
    profile_id: str,
    req: UpdateProfileRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update profile details, role, password or granular permissions."""
    username_actor = current_user.get("sub", "admin")
    user_role = current_user.get("role", "")
    perms = current_user.get("permissions", [])
    
    if user_role not in ["Super Admin", "Administrator"] and "manage_profiles" not in perms and "*" not in perms:
        raise HTTPException(status_code=403, detail="Permission denied: Requires 'manage_profiles' privilege")

    profiles = load_profiles()
    target_idx = None
    for idx, p in enumerate(profiles):
        if p.get("id") == profile_id or p.get("username") == profile_id:
            target_idx = idx
            break

    if target_idx is None:
        raise HTTPException(status_code=404, detail=f"Profile {profile_id} not found")

    target = profiles[target_idx]
    is_system = target.get("is_system", False)

    # Updates
    if req.full_name is not None:
        target["full_name"] = req.full_name.strip()
    if req.email is not None:
        target["email"] = req.email.strip()
    if req.password:
        target["password_hash"] = hash_password(req.password)
    if req.notes is not None:
        target["notes"] = req.notes.strip()

    # System profile safeguards
    if is_system:
        # Cannot demote or disable root system admin
        if req.status and req.status.upper() != "ACTIVE":
            raise HTTPException(status_code=400, detail="System master administrator cannot be deactivated")
        target["status"] = "ACTIVE"
        target["role"] = "Super Admin"
        valid_keys = [c["key"] for c in PERMISSIONS_CATALOG]
        target["permissions"] = valid_keys
    else:
        if req.role is not None:
            target["role"] = req.role.strip()
            register_role_if_not_exists(req.role.strip(), target.get("permissions", []))
        if req.status is not None:
            target["status"] = req.status.upper()
            if target["status"] == "INACTIVE":
                target["is_online"] = False
        if req.permissions is not None:
            valid_keys = {c["key"] for c in PERMISSIONS_CATALOG}
            target["permissions"] = [k for k in req.permissions if k in valid_keys]

    profiles[target_idx] = target
    save_profiles(profiles)

    # Audit log
    append_audit_log({
        "action": "PROFILE_UPDATE",
        "target": f"User: {target.get('username')} ({target.get('role')})",
        "operator": username_actor,
        "status": "SUCCESS",
        "detail": f"Updated profile '{target.get('username')}' (Role: {target.get('role')}, Status: {target.get('status')}, Perms: {len(target.get('permissions', []))})."
    })

    return sanitize_profile(target)

@router.post("/{profile_id}/toggle-status", response_model=Dict[str, Any])
def toggle_profile_status(
    profile_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Quickly toggle profile status between ACTIVE and INACTIVE."""
    username_actor = current_user.get("sub", "admin")
    profiles = load_profiles()
    target_idx = None
    for idx, p in enumerate(profiles):
        if p.get("id") == profile_id or p.get("username") == profile_id:
            target_idx = idx
            break

    if target_idx is None:
        raise HTTPException(status_code=404, detail=f"Profile {profile_id} not found")

    target = profiles[target_idx]
    if target.get("is_system", False):
        raise HTTPException(status_code=400, detail="Master root system administrator cannot be deactivated")

    new_status = "INACTIVE" if target.get("status") == "ACTIVE" else "ACTIVE"
    target["status"] = new_status
    if new_status == "INACTIVE":
        target["is_online"] = False
    profiles[target_idx] = target
    save_profiles(profiles)

    append_audit_log({
        "action": "PROFILE_STATUS_TOGGLE",
        "target": f"User: {target.get('username')}",
        "operator": username_actor,
        "status": "SUCCESS",
        "detail": f"Changed status of profile '{target.get('username')}' to {new_status}."
    })

    return sanitize_profile(target)

@router.delete("/{profile_id}", response_model=Dict[str, Any])
def delete_profile(
    profile_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Delete profile credential. Protected system accounts cannot be deleted."""
    username_actor = current_user.get("sub", "admin")
    profiles = load_profiles()
    target = None
    for p in profiles:
        if p.get("id") == profile_id or p.get("username") == profile_id:
            target = p
            break

    if target is None:
        raise HTTPException(status_code=404, detail=f"Profile {profile_id} not found")

    if target.get("is_system", False):
        raise HTTPException(status_code=400, detail="Master root system administrator cannot be deleted")

    profiles = [p for p in profiles if p.get("id") != target.get("id") and p.get("username") != target.get("username")]
    save_profiles(profiles)

    append_audit_log({
        "action": "PROFILE_DELETE",
        "target": f"User: {target.get('username')}",
        "operator": username_actor,
        "status": "SUCCESS",
        "detail": f"Deleted profile credential '{target.get('username')}' ({target.get('full_name')})."
    })

    return {
        "success": True,
        "message": f"Profile '{target.get('username')}' deleted successfully"
    }
