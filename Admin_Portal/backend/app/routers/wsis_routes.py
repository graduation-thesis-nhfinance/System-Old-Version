from fastapi import APIRouter, Depends
from app.connectors.wsis import wsis_connector
from app.auth import get_current_user

router = APIRouter(prefix="/api/v1/wsis", tags=["WSIS Monitor"])

@router.get("/status")
def get_wsis_status(current_user: dict = Depends(get_current_user)):
    return wsis_connector.get_status()

@router.get("/history")
def get_wsis_history(current_user: dict = Depends(get_current_user)):
    status = wsis_connector.get_status()
    return {
        "history": status.get("historical_match_rates", []),
        "max_records": 10,
        "current_window": status.get("full_display_timestamp")
    }

