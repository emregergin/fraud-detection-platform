import math
import redis
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Tuple, List, Optional
from worker.database import SessionLocal, TransactionRecord, FraudAlert

# Configuration
load_dotenv = lambda: None # Placeholder if needed, handled in database.py
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
logger = logging.getLogger(__name__)

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance between two points in km."""
    R = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

async def check_velocity(user_id: str) -> bool:
    """Rule: >5 transactions per minute for the same user."""
    key = f"velocity:{user_id}"
    current_count = r.incr(key)
    if current_count == 1:
        r.expire(key, 60)
    
    if current_count > 5:
        logger.warning(f"Velocity alert for user {user_id}: {current_count} transactions/min")
        return True
    return False

async def check_amount(user_id: str, amount: float, db) -> bool:
    """Rule: Transaction > 3x the user's last 24h average."""
    # This is a simplified version. In a real system, we'd cache the average.
    yesterday = datetime.utcnow() - timedelta(hours=24)
    records = db.query(TransactionRecord).filter(
        TransactionRecord.user_id == user_id,
        TransactionRecord.timestamp >= yesterday
    ).all()
    
    if not records:
        return False
        
    avg_amount = sum(r.amount for r in records) / len(records)
    if amount > (avg_amount * 3):
        logger.warning(f"Amount alert for user {user_id}: {amount} is > 3x avg {avg_amount}")
        return True
    return False

async def check_location(user_id: str, lat: float, lon: float, timestamp: datetime) -> Optional[str]:
    """Rule: Impossible physical distance between two consecutive transactions."""
    last_loc_key = f"location:{user_id}"
    last_loc_data = r.get(last_loc_key)
    
    current_loc = {"lat": lat, "lon": lon, "time": timestamp.isoformat()}
    r.set(last_loc_key, json.dumps(current_loc))

    if not last_loc_data:
        return None
        
    last_loc = json.loads(last_loc_data)
    last_time = datetime.fromisoformat(last_loc["time"])
    
    distance = haversine(last_loc["lat"], last_loc["lon"], lat, lon)
    time_diff = (timestamp - last_time).total_seconds() / 3600.0  # in hours
    
    if time_diff <= 0:
        return None
        
    speed = distance / time_diff # km/h
    # If speed > 1000 km/h (speed of a commercial jet), it's likely impossible
    if speed > 1000:
        logger.warning(f"Location alert for user {user_id}: distance {distance:.2f}km in {time_diff*60:.2f}min (Speed: {speed:.2f}km/h)")
        return f"Impossible travel: {distance:.2f}km in {time_diff*60:.2f}min"
    
    return None

async def process_transaction(data: dict):
    db = SessionLocal()
    try:
        user_id = data["user_id"]
        amount = data["amount"]
        lat = data["latitude"]
        lon = data["longitude"]
        ts = datetime.fromisoformat(data["timestamp"].replace('Z', ''))
        tx_id = data["transaction_id"]

        is_anomaly = False
        reasons = []

        # 1. Velocity Check
        if await check_velocity(user_id):
            is_anomaly = True
            reasons.append("Velocity exceeded (>5 tx/min)")

        # 2. Amount Check
        if await check_amount(user_id, amount, db):
            is_anomaly = True
            reasons.append(f"Amount suspicious (>3x 24h avg)")

        # 3. Location Check
        loc_alert = await check_location(user_id, lat, lon, ts)
        if loc_alert:
            is_anomaly = True
            reasons.append(loc_alert)

        # Save Transaction
        record = TransactionRecord(
            id=tx_id,
            user_id=user_id,
            amount=amount,
            currency=data.get("currency", "USD"),
            merchant_id=data["merchant_id"],
            latitude=lat,
            longitude=lon,
            timestamp=ts,
            is_anomaly=is_anomaly
        )
        db.add(record)

        # Save Fraud Alert if suspect
        if is_anomaly:
            alert = FraudAlert(
                id=tx_id,
                user_id=user_id,
                reason=", ".join(reasons),
                details=json.dumps(data)
            )
            db.add(alert)
        
        db.commit()
    except Exception as e:
        logger.error(f"Error processing transaction {data.get('transaction_id')}: {e}")
        db.rollback()
    finally:
        db.close()
