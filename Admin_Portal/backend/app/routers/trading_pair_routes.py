import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.connectors.clickhouse import clickhouse_connector
from app.auth import get_current_user
from app.routers.audit_routes import append_audit_log

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/trading-pairs", tags=["Trading Pairs Registry"])

TZ_VN = timezone(timedelta(hours=7))
CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "dim_trading_pairs.json"

DEFAULT_PAIRS = [
    {"pair_id": 1, "base_coin": "BTC", "quote_coin": "USDT", "trading_pair": "BTC/USDT", "symbol": "BTCUSDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_active": 1, "updated_at": "2026-09-18 16:38:52"},
    {"pair_id": 2, "base_coin": "ETH", "quote_coin": "USDT", "trading_pair": "ETH/USDT", "symbol": "ETHUSDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_active": 1, "updated_at": "2026-09-18 16:38:52"},
    {"pair_id": 3, "base_coin": "BNB", "quote_coin": "USDT", "trading_pair": "BNB/USDT", "symbol": "BNBUSDT", "market_type": "SPOT_FUTURES", "category": "crypto_popular", "is_active": 1, "updated_at": "2026-09-18 16:38:52"},
    {"pair_id": 4, "base_coin": "AAPLB", "quote_coin": "USDT", "trading_pair": "AAPLB/USDT", "symbol": "AAPLBUSDT", "market_type": "SPOT", "category": "equity_token", "is_active": 1, "updated_at": "2026-09-18 16:38:52"},
    {"pair_id": 5, "base_coin": "AMZNB", "quote_coin": "USDT", "trading_pair": "AMZNB/USDT", "symbol": "AMZNBUSDT", "market_type": "SPOT", "category": "equity_token", "is_active": 1, "updated_at": "2026-09-18 16:38:52"},
    {"pair_id": 6, "base_coin": "DELLB", "quote_coin": "USDT", "trading_pair": "DELLB/USDT", "symbol": "DELLBUSDT", "market_type": "SPOT", "category": "equity_token", "is_active": 1, "updated_at": "2026-09-18 16:38:52"}
]

def load_cached_pairs() -> List[Dict[str, Any]]:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not read local dim_trading_pairs cache: %s", e)
    return DEFAULT_PAIRS.copy()

def save_cached_pairs(pairs: List[Dict[str, Any]]):
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2)
    except Exception as e:
        logger.error("Failed to write dim_trading_pairs cache: %s", e)

class CreateTradingPairRequest(BaseModel):
    symbol: str = Field(..., description="Trading pair symbol, e.g. SOLUSDT")
    base_coin: str = Field(..., description="Base asset, e.g. SOL")
    quote_coin: str = Field(default="USDT", description="Quote asset, e.g. USDT")
    trading_pair: Optional[str] = Field(default=None, description="Formatted pair, e.g. SOL/USDT")
    market_type: str = Field(default="SPOT_FUTURES", description="Market type: SPOT_FUTURES, SPOT, FUTURES, or TOKENIZED_STOCK")
    category: str = Field(default="crypto_popular", description="Category classification, e.g. crypto_popular, layer1, defi, equity_token")
    is_active: bool = Field(default=True, description="Whether the trading pair is active for collection")

class UpdateTradingPairRequest(BaseModel):
    base_coin: Optional[str] = None
    quote_coin: Optional[str] = None
    trading_pair: Optional[str] = None
    market_type: Optional[str] = None
    category: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("")
def list_trading_pairs(
    search: Optional[str] = Query(None),
    market_type: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    is_active: Optional[str] = Query(None)
):
    """
    Returns all trading pairs registered in ClickHouse default.dim_trading_pairs
    with failover to local persistent JSON registry, strictly deduplicated.
    """
    pairs = []
    # Try querying ClickHouse first with FINAL modifier to collapse duplicates
    try:
        ch_data = clickhouse_connector.query(
            "SELECT pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, toString(updated_at) as updated_at "
            "FROM default.dim_trading_pairs FINAL ORDER BY pair_id ASC"
        )
        if ch_data:
            # Guarantee deduplication by symbol & pair_id (latest wins)
            dedup_map = {}
            for p in ch_data:
                sym_key = str(p.get("symbol", "")).upper().strip()
                dedup_map[sym_key] = p
            pairs = sorted(dedup_map.values(), key=lambda x: int(x.get("pair_id", 0)))
            save_cached_pairs(pairs)
    except Exception as e:
        logger.warning("ClickHouse query for dim_trading_pairs failed: %s", e)

    if not pairs:
        raw_cached = load_cached_pairs()
        dedup_map = {}
        for p in raw_cached:
            sym_key = str(p.get("symbol", "")).upper().strip()
            dedup_map[sym_key] = p
        pairs = sorted(dedup_map.values(), key=lambda x: int(x.get("pair_id", 0)))

    # Apply filters
    filtered = []
    for p in pairs:
        if search:
            q = search.upper().strip()
            sym = str(p.get("symbol", "")).upper()
            base = str(p.get("base_coin", "")).upper()
            quote = str(p.get("quote_coin", "")).upper()
            tp = str(p.get("trading_pair", "")).upper()
            if q not in sym and q not in base and q not in quote and q not in tp:
                continue

        if market_type and market_type != "all":
            if str(p.get("market_type", "")).upper() != market_type.upper():
                continue

        if category and category != "all":
            if str(p.get("category", "")).lower() != category.lower():
                continue

        if is_active and is_active != "all":
            want_active = 1 if is_active in ("1", "true", "active") else 0
            if int(p.get("is_active", 1)) != want_active:
                continue

        filtered.append(p)

    return {
        "total": len(filtered),
        "source": "clickhouse" if pairs and clickhouse_connector.get_health().get("online") else "local_cache",
        "table": "default.dim_trading_pairs",
        "trading_pairs": filtered
    }

def check_trading_pair_permission(current_user: dict):
    role = current_user.get("role", "")
    perms = current_user.get("permissions", [])
    if role not in ["Super Admin", "Administrator"] and "manage_trading_pairs" not in perms and "*" not in perms:
        raise HTTPException(status_code=403, detail="Permission denied: Requires 'manage_trading_pairs' or Administrator privilege")

@router.post("")
def create_or_update_trading_pair(
    req: CreateTradingPairRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Register or overwrite a trading pair in default.dim_trading_pairs table
    and the local cache. If the symbol already exists, it is overwritten with the updated values.
    """
    check_trading_pair_permission(current_user)
    sym = req.symbol.strip().upper()
    base = req.base_coin.strip().upper()
    quote = req.quote_coin.strip().upper()
    tp = req.trading_pair.strip().upper() if req.trading_pair else f"{base}/{quote}"
    m_type = req.market_type.strip().upper()
    cat = req.category.strip().lower()
    act_int = 1 if req.is_active else 0

    # Load existing pairs to check if updating existing or assigning new pair_id
    existing = load_cached_pairs()
    try:
        ch_pairs = clickhouse_connector.query("SELECT pair_id, symbol FROM default.dim_trading_pairs FINAL")
        if ch_pairs:
            existing = ch_pairs
    except Exception:
        pass

    existing_pair = next((p for p in existing if str(p.get("symbol", "")).upper() == sym), None)
    is_overwrite = existing_pair is not None

    if is_overwrite:
        target_id = int(existing_pair["pair_id"])
    else:
        pair_ids = [int(p.get("pair_id", 0)) for p in existing if isinstance(p.get("pair_id"), (int, float, str)) and str(p.get("pair_id")).isdigit()]
        target_id = (max(pair_ids) + 1) if pair_ids else (len(existing) + 1)

    now_gmt7 = datetime.now(timezone.utc).astimezone(TZ_VN)
    updated_at_str = now_gmt7.strftime("%Y-%m-%d %H:%M:%S")

    record = {
        "pair_id": target_id,
        "base_coin": base,
        "quote_coin": quote,
        "trading_pair": tp,
        "symbol": sym,
        "market_type": m_type,
        "category": cat,
        "is_active": act_int,
        "updated_at": updated_at_str
    }

    # Execute INSERT into ClickHouse ReplacingMergeTree (replaces on matching primary key pair_id)
    ch_insert_sql = f"""
    INSERT INTO default.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, updated_at)
    VALUES ({target_id}, '{base}', '{quote}', '{tp}', '{sym}', '{m_type}', '{cat}', {act_int}, now())
    """
    ch_success = clickhouse_connector.execute(ch_insert_sql)
    # Immediately trigger OPTIMIZE FINAL to collapse physical rows
    try:
        clickhouse_connector.execute("OPTIMIZE TABLE default.dim_trading_pairs FINAL")
    except Exception as e:
        logger.debug("Optimize table notice: %s", e)

    # Persist in local cache (deduplicated)
    cached = load_cached_pairs()
    cached = [p for p in cached if int(p.get("pair_id", -1)) != target_id and str(p.get("symbol", "")).upper() != sym]
    cached.append(record)
    cached = sorted(cached, key=lambda x: int(x.get("pair_id", 0)))
    save_cached_pairs(cached)

    operator = current_user.get("username", "nhfinanceadmin")
    append_audit_log({
        "operator": operator,
        "action": "OVERWRITE_TRADING_PAIR" if is_overwrite else "ADD_TRADING_PAIR",
        "target_table": "default.dim_trading_pairs",
        "pair_id": target_id,
        "symbol": sym,
        "trading_pair": tp,
        "reason": f"{'Updated & Overwritten' if is_overwrite else 'Created'} trading pair {sym} ({tp}) by {operator}",
        "clickhouse_synced": ch_success
    })

    return {
        "status": "SUCCESS",
        "message": f"Trading pair {sym} successfully {'updated and overwritten' if is_overwrite else 'registered'} in dim_trading_pairs",
        "clickhouse_synced": ch_success,
        "trading_pair": record
    }

@router.put("/{pair_id}")
def update_trading_pair(
    pair_id: int,
    req: UpdateTradingPairRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Explicitly update an existing trading pair by pair_id, overwriting it in ClickHouse.
    """
    check_trading_pair_permission(current_user)
    pairs = load_cached_pairs()
    try:
        ch_pairs = clickhouse_connector.query("SELECT pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active FROM default.dim_trading_pairs FINAL")
        if ch_pairs:
            pairs = ch_pairs
    except Exception:
        pass

    target = next((p for p in pairs if int(p.get("pair_id", -1)) == pair_id), None)
    if not target:
        raise HTTPException(status_code=404, detail=f"Trading pair with ID {pair_id} not found")

    base = req.base_coin.strip().upper() if req.base_coin else str(target.get("base_coin", "BTC"))
    quote = req.quote_coin.strip().upper() if req.quote_coin else str(target.get("quote_coin", "USDT"))
    tp = req.trading_pair.strip().upper() if req.trading_pair else (target.get("trading_pair") or f"{base}/{quote}")
    m_type = req.market_type.strip().upper() if req.market_type else str(target.get("market_type", "SPOT_FUTURES"))
    cat = req.category.strip().lower() if req.category else str(target.get("category", "crypto_popular"))
    act_int = (1 if req.is_active else 0) if req.is_active is not None else int(target.get("is_active", 1))
    sym = str(target.get("symbol", f"{base}{quote}")).upper()

    now_gmt7 = datetime.now(timezone.utc).astimezone(TZ_VN)
    updated_at_str = now_gmt7.strftime("%Y-%m-%d %H:%M:%S")

    updated_record = {
        "pair_id": pair_id,
        "base_coin": base,
        "quote_coin": quote,
        "trading_pair": tp,
        "symbol": sym,
        "market_type": m_type,
        "category": cat,
        "is_active": act_int,
        "updated_at": updated_at_str
    }

    # ReplacingMergeTree INSERT with pair_id
    sql = f"""
    INSERT INTO default.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, updated_at)
    VALUES ({pair_id}, '{base}', '{quote}', '{tp}', '{sym}', '{m_type}', '{cat}', {act_int}, now())
    """
    ch_success = clickhouse_connector.execute(sql)
    try:
        clickhouse_connector.execute("OPTIMIZE TABLE default.dim_trading_pairs FINAL")
    except Exception as e:
        logger.debug("Optimize table notice: %s", e)

    cached = load_cached_pairs()
    cached = [p for p in cached if int(p.get("pair_id", -1)) != pair_id]
    cached.append(updated_record)
    cached = sorted(cached, key=lambda x: int(x.get("pair_id", 0)))
    save_cached_pairs(cached)

    operator = current_user.get("username", "nhfinanceadmin")
    append_audit_log({
        "operator": operator,
        "action": "UPDATE_TRADING_PAIR",
        "target_table": "default.dim_trading_pairs",
        "pair_id": pair_id,
        "symbol": sym,
        "reason": f"Updated trading pair ID {pair_id} ({sym}) by {operator}",
        "clickhouse_synced": ch_success
    })

    return {
        "status": "SUCCESS",
        "message": f"Trading pair {sym} (ID: {pair_id}) successfully updated and overwritten",
        "clickhouse_synced": ch_success,
        "trading_pair": updated_record
    }

@router.post("/{pair_id}/toggle-active")
def toggle_trading_pair_active(
    pair_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Toggle is_active flag for a specific trading pair.
    """
    check_trading_pair_permission(current_user)
    pairs = load_cached_pairs()
    target = next((p for p in pairs if int(p.get("pair_id", -1)) == pair_id), None)
    if not target:
        raise HTTPException(status_code=404, detail=f"Trading pair with ID {pair_id} not found")

    new_active = 0 if int(target.get("is_active", 1)) == 1 else 1
    target["is_active"] = new_active
    target["updated_at"] = datetime.now(timezone.utc).astimezone(TZ_VN).strftime("%Y-%m-%d %H:%M:%S")

    # Update ClickHouse via ReplacingMergeTree INSERT
    sql = f"""
    INSERT INTO default.dim_trading_pairs (pair_id, base_coin, quote_coin, trading_pair, symbol, market_type, category, is_active, updated_at)
    VALUES ({pair_id}, '{target["base_coin"]}', '{target["quote_coin"]}', '{target["trading_pair"]}', '{target["symbol"]}', '{target["market_type"]}', '{target["category"]}', {new_active}, now())
    """
    ch_success = clickhouse_connector.execute(sql)

    save_cached_pairs(pairs)

    operator = current_user.get("username", "nhfinanceadmin")
    append_audit_log({
        "operator": operator,
        "action": "TOGGLE_TRADING_PAIR_ACTIVE",
        "target_table": "default.dim_trading_pairs",
        "pair_id": pair_id,
        "symbol": target["symbol"],
        "is_active": new_active,
        "reason": f"Manual active toggle for {target['symbol']}: {'Active' if new_active == 1 else 'Inactive'}"
    })

    return {
        "status": "SUCCESS",
        "pair_id": pair_id,
        "symbol": target["symbol"],
        "is_active": new_active,
        "clickhouse_synced": ch_success
    }

class BulkDeleteTradingPairsRequest(BaseModel):
    pair_ids: List[int] = Field(..., description="List of pair_ids to delete")

@router.delete("/{pair_id}")
def delete_trading_pair(
    pair_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a specific trading pair from default.dim_trading_pairs in ClickHouse
    and update the local persistent cache.
    """
    check_trading_pair_permission(current_user)
    pairs = load_cached_pairs()
    try:
        ch_pairs = clickhouse_connector.query("SELECT pair_id, symbol, base_coin, quote_coin, trading_pair FROM default.dim_trading_pairs")
        if ch_pairs:
            pairs = ch_pairs
    except Exception:
        pass

    target = next((p for p in pairs if int(p.get("pair_id", -1)) == pair_id), None)
    if not target:
        raise HTTPException(status_code=404, detail=f"Trading pair with ID {pair_id} not found")

    target_symbol = target.get("symbol", f"PAIR_{pair_id}")

    # Execute DELETE in ClickHouse
    sql = f"ALTER TABLE default.dim_trading_pairs DELETE WHERE pair_id = {pair_id}"
    ch_success = clickhouse_connector.execute(sql)

    # Remove from local cache
    cached = load_cached_pairs()
    cached = [p for p in cached if int(p.get("pair_id", -1)) != pair_id]
    save_cached_pairs(cached)

    operator = current_user.get("username", "nhfinanceadmin")
    append_audit_log({
        "operator": operator,
        "action": "DELETE_TRADING_PAIR",
        "target_table": "default.dim_trading_pairs",
        "pair_id": pair_id,
        "symbol": target_symbol,
        "reason": f"Manual deletion of trading pair {target_symbol} (ID: {pair_id}) by {operator}",
        "clickhouse_synced": ch_success
    })

    return {
        "status": "SUCCESS",
        "message": f"Trading pair {target_symbol} (ID: {pair_id}) deleted successfully from dim_trading_pairs",
        "pair_id": pair_id,
        "symbol": target_symbol,
        "clickhouse_synced": ch_success
    }

@router.post("/batch-delete")
def batch_delete_trading_pairs(
    req: BulkDeleteTradingPairsRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Batch delete multiple trading pairs from ClickHouse default.dim_trading_pairs.
    """
    check_trading_pair_permission(current_user)
    if not req.pair_ids:
        raise HTTPException(status_code=400, detail="No pair_ids provided for batch deletion")

    ids_str = ", ".join(str(i) for i in req.pair_ids)
    sql = f"ALTER TABLE default.dim_trading_pairs DELETE WHERE pair_id IN ({ids_str})"
    ch_success = clickhouse_connector.execute(sql)

    cached = load_cached_pairs()
    cached = [p for p in cached if int(p.get("pair_id", -1)) not in req.pair_ids]
    save_cached_pairs(cached)

    operator = current_user.get("username", "nhfinanceadmin")
    append_audit_log({
        "operator": operator,
        "action": "BATCH_DELETE_TRADING_PAIRS",
        "target_table": "default.dim_trading_pairs",
        "pair_ids": req.pair_ids,
        "count": len(req.pair_ids),
        "reason": f"Batch deletion of {len(req.pair_ids)} trading pairs by {operator}",
        "clickhouse_synced": ch_success
    })

    return {
        "status": "SUCCESS",
        "message": f"Successfully deleted {len(req.pair_ids)} trading pairs from dim_trading_pairs",
        "deleted_count": len(req.pair_ids),
        "pair_ids": req.pair_ids,
        "clickhouse_synced": ch_success
    }

