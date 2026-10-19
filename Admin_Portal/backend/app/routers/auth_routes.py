from fastapi import APIRouter, HTTPException, Depends, Security, Query
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
from app.auth import (
    verify_profile_credentials, verify_credentials_with_airflow, 
    create_access_token, get_current_user, decode_access_token,
    set_user_offline, update_user_activity, security
)
from app.config import settings

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    token: str
    username: str
    role: str
    email: str
    permissions: List[str] = []

@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest):
    if not req.username or not req.password:
        raise HTTPException(status_code=400, detail="Username and password are required")
    
    # 1. Try local profiles.json
    profile = verify_profile_credentials(req.username, req.password)
    if profile:
        role = profile.get("role", "Operator")
        permissions = profile.get("permissions", [])
        email = profile.get("email", f"{profile.get('username')}@nhfinance.vn")
        token = create_access_token(profile.get("username"), role=role, permissions=permissions)
        return LoginResponse(
            token=token,
            username=profile.get("username"),
            role=role,
            email=email,
            permissions=permissions
        )

    # 2. Fallback to Airflow / settings.ADMIN_USERNAME
    is_valid = verify_credentials_with_airflow(req.username, req.password)
    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    is_admin = (req.username == settings.ADMIN_USERNAME)
    role = "Super Admin" if is_admin else "Operator"
    all_perms = [
        "view_matrix", "trigger_pipeline", "manage_trading_pairs",
        "manage_catalog", "manage_profiles", "view_lakehouse", "manage_stream", "view_audit", "view_system_links"
    ]
    permissions = all_perms if is_admin else ["view_matrix", "trigger_pipeline", "view_lakehouse", "view_audit"]
    token = create_access_token(req.username, role=role, permissions=permissions)
    return LoginResponse(
        token=token,
        username=req.username,
        role=role,
        email=settings.ADMIN_EMAIL if is_admin else f"{req.username}@nhfinance.local",
        permissions=permissions
    )

@router.get("/me")
def get_me(current_user: dict = Depends(get_current_user)):
    username = current_user.get("sub", "")
    role = current_user.get("role", "Operator")
    permissions = current_user.get("permissions", [])
    is_admin = role in ["Super Admin", "Administrator"] or "manage_profiles" in permissions
    return {
        "username": username,
        "role": role,
        "permissions": permissions,
        "is_admin": is_admin,
        "email": settings.ADMIN_EMAIL if is_admin else f"{username}@nhfinance.vn",
        "authenticated_via": "Profiles & Ecosystem Auth"
    }

@router.post("/heartbeat")
def heartbeat(current_user: dict = Depends(get_current_user)):
    """Heartbeat endpoint: keeps session alive with sliding token renewal and enforces instant session termination on deactivation."""
    username = current_user.get("sub", "")
    role = current_user.get("role", "Operator")
    permissions = current_user.get("permissions", [])
    update_user_activity(username)
    # Issue a refreshed sliding session token so monitoring cockpits never expire while open
    refreshed_token = create_access_token(username=username, role=role, permissions=permissions)
    return {
        "status": "ACTIVE",
        "username": username,
        "token": refreshed_token
    }

@router.post("/logout")
@router.get("/logout")
def logout(token: Optional[str] = Query(None), credentials: Optional[HTTPAuthorizationCredentials] = Security(security)):
    """Log out user: marks account as offline and terminates active session."""
    tok = None
    if credentials:
        tok = credentials.credentials
    elif token:
        tok = token
    if tok:
        data = decode_access_token(tok)
        if data:
            username = data.get("sub", "")
            set_user_offline(username)
    return {"message": "Logged out successfully"}
