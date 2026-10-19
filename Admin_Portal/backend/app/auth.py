import time
import base64
import json
import hmac
import hashlib
import requests
from datetime import datetime, timezone, timedelta
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import settings

security = HTTPBearer(auto_error=False)

from pathlib import Path

TZ_VN = timezone(timedelta(hours=7))
PROFILES_FILE = Path(__file__).resolve().parent / "data" / "profiles.json"

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def verify_profile_credentials(username: str, password: str) -> dict:
    """
    Check credentials against local profiles.json registry.
    Returns profile dict if valid, or None if not found/invalid.
    """
    if not PROFILES_FILE.exists():
        return None
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            profiles = json.load(f)
        
        target_idx = None
        for idx, p in enumerate(profiles):
            if p.get("username", "").lower() == username.lower() or p.get("email", "").lower() == username.lower():
                target_idx = idx
                break
        
        if target_idx is not None:
            p = profiles[target_idx]
            stored = p.get("password_hash", "")
            # check plain match or sha256 match
            is_match = (password == stored) or (hash_password(password) == stored)
            if is_match:
                if p.get("status", "").upper() != "ACTIVE":
                    raise HTTPException(status_code=403, detail="Account is deactivated. Contact administrator.")
                # update last_login and online status with Vietnam Time (GMT+7)
                now_str = datetime.now(TZ_VN).strftime("%Y-%m-%d %H:%M:%S")
                p["last_login"] = now_str
                p["last_activity"] = now_str
                p["is_online"] = True
                profiles[target_idx] = p
                try:
                    with open(PROFILES_FILE, "w", encoding="utf-8") as f:
                        json.dump(profiles, f, indent=2, ensure_ascii=False)
                except Exception:
                    pass
                return p
    except HTTPException:
        raise
    except Exception:
        pass
    return None

def verify_credentials_with_airflow(username: str, password: str) -> bool:
    """
    Directly verify credentials against Airflow Webserver REST API.
    If Airflow is currently offline or unreachable, fall back to environment settings.
    """
    try:
        url = f"{settings.AIRFLOW_API_URL}/dags"
        # Test basic auth against Airflow API
        resp = requests.get(url, auth=(username, password), timeout=3)
        if resp.status_code == 200:
            return True
        elif resp.status_code == 401:
            return False
    except Exception:
        pass

    # Fallback to configured admin credentials
    if username == settings.ADMIN_USERNAME and password == settings.ADMIN_PASSWORD:
        return True
    return False

def create_access_token(username: str, role: str = "Administrator", permissions: list = None) -> str:
    payload = {
        "sub": username,
        "username": username,
        "role": role,
        "permissions": permissions or [],
        "exp": int(time.time()) + (settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60)
    }
    raw_payload = json.dumps(payload).encode()
    b64_payload = base64.urlsafe_b64encode(raw_payload).decode().rstrip("=")
    signature = hmac.new(
        settings.SECRET_KEY.encode(),
        b64_payload.encode(),
        hashlib.sha256
    ).hexdigest()
    return f"{b64_payload}.{signature}"

def decode_access_token(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None
        b64_payload, signature = parts
        expected_sig = hmac.new(
            settings.SECRET_KEY.encode(),
            b64_payload.encode(),
            hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            return None
        
        # Add padding back if necessary
        padded = b64_payload + "=" * (-len(b64_payload) % 4)
        raw_payload = base64.urlsafe_b64decode(padded.encode())
        data = json.loads(raw_payload.decode())
        if data.get("exp", 0) < time.time():
            return None
        return data
    except Exception:
        return None

def set_user_offline(username: str):
    """Mark a user as offline in profiles registry."""
    if not PROFILES_FILE.exists() or not username:
        return
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            profiles = json.load(f)
        changed = False
        for p in profiles:
            if p.get("username", "").lower() == username.lower() or p.get("email", "").lower() == username.lower():
                p["is_online"] = False
                changed = True
                break
        if changed:
            with open(PROFILES_FILE, "w", encoding="utf-8") as f:
                json.dump(profiles, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

def update_user_activity(username: str):
    """Update last activity and ensure is_online is True if active."""
    if not PROFILES_FILE.exists() or not username:
        return
    try:
        with open(PROFILES_FILE, "r", encoding="utf-8") as f:
            profiles = json.load(f)
        changed = False
        now_str = datetime.now(TZ_VN).strftime("%Y-%m-%d %H:%M:%S")
        for p in profiles:
            if p.get("username", "").lower() == username.lower() or p.get("email", "").lower() == username.lower():
                if p.get("status", "").upper() == "ACTIVE":
                    p["is_online"] = True
                    p["last_activity"] = now_str
                    changed = True
                else:
                    p["is_online"] = False
                    changed = True
                break
        if changed:
            with open(PROFILES_FILE, "w", encoding="utf-8") as f:
                json.dump(profiles, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Missing authorization token")
    token = credentials.credentials
    user_data = decode_access_token(token)
    if not user_data:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # Immediate Security Check: Validate whether account has been suspended or deactivated
    username = user_data.get("sub", "")
    if PROFILES_FILE.exists() and username:
        try:
            with open(PROFILES_FILE, "r", encoding="utf-8") as f:
                profiles = json.load(f)
            for p in profiles:
                if p.get("username", "").lower() == username.lower():
                    if p.get("status", "").upper() == "INACTIVE":
                        p["is_online"] = False
                        try:
                            with open(PROFILES_FILE, "w", encoding="utf-8") as f_out:
                                json.dump(profiles, f_out, indent=2, ensure_ascii=False)
                        except Exception:
                            pass
                        raise HTTPException(
                            status_code=403,
                            detail="ACCOUNT_DEACTIVATED: Your account has been suspended or deactivated by an Administrator. Session terminated immediately."
                        )
                    # Mark online while active
                    p["is_online"] = True
                    break
        except HTTPException:
            raise
        except Exception:
            pass

    return user_data
