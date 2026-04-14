import os
import asyncio
import json
import logging
from mcp.server.fastmcp import FastMCP
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import redis
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

# Database Setup
DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
engine = sa.create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Redis Setup
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD", ""),
    decode_responses=True
)

mcp = FastMCP("fraud-detection-server")

@mcp.tool()
async def get_recent_frauds(limit: int = 10) -> str:
    """Get the latest detected fraud alerts from the system."""
    try:
        with SessionLocal() as db:
            result = db.execute(sa.text(f"SELECT id, user_id, reason, details, created_at FROM fraud_alerts ORDER BY created_at DESC LIMIT {limit}"))
            frauds = [dict(row._mapping) for row in result]
            for f in frauds:
                if isinstance(f.get("created_at"), datetime):
                    f["created_at"] = f["created_at"].isoformat()
            
            return json.dumps(frauds, indent=2)
    except Exception as e:
        logger.error(f"get_recent_frauds failed: {e}")
        return f"Error: {str(e)}"

@mcp.tool()
async def check_user_status(user_id: str) -> str:
    """Check the current status and risk level of a specific user."""
    try:
        with SessionLocal() as db:
            # Get last 5 transactions
            tx_res = db.execute(sa.text(f"SELECT * FROM transactions WHERE user_id = :user_id ORDER BY timestamp DESC LIMIT 5"), {"user_id": user_id})
            transactions = [dict(row._mapping) for row in tx_res]
            for tx in transactions:
                if isinstance(tx.get("timestamp"), datetime):
                    tx["timestamp"] = tx["timestamp"].isoformat()

            # Get alerts
            alert_res = db.execute(sa.text(f"SELECT reason, created_at FROM fraud_alerts WHERE user_id = :user_id ORDER BY created_at DESC"), {"user_id": user_id})
            alerts_data = [dict(row._mapping) for row in alert_res]
            for a in alerts_data:
                if isinstance(a.get("created_at"), datetime):
                    a["created_at"] = a["created_at"].isoformat()

            # Get current velocity from Redis
            velocity = r.get(f"velocity:{user_id}") or 0

            status = {
                "user_id": user_id,
                "current_velocity_per_min": int(velocity),
                "total_alerts": len(alerts_data),
                "recent_alerts": alerts_data[:3],
                "recent_transactions": transactions
            }
            return json.dumps(status, indent=2)
    except Exception as e:
        logger.error(f"check_user_status failed: {e}")
        return f"Error: {str(e)}"

if __name__ == "__main__":
    mcp.run()
