import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
import uvicorn
from .schemas import Transaction
from .rabbitmq_client import rabbitmq_client
from contextlib import asynccontextmanager


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up backend service...")
    await rabbitmq_client.connect()
    yield
    logger.info("Shutting down backend service...")
    await rabbitmq_client.close()

app = FastAPI(title="Fraud Detection Platform", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/transactions", status_code=201)
async def ingest_transaction(transaction: Transaction):
    try:
        # Pydantic validates the input automatically
        transaction_dict = transaction.model_dump()
        # Ensure timestamp is string for JSON serialization
        transaction_dict["timestamp"] = transaction_dict["timestamp"].isoformat()
        
        await rabbitmq_client.publish_transaction(transaction_dict)
        return {"message": "Transaction received and queued", "transaction_id": transaction.transaction_id}
    except Exception as e:
        logger.error(f"Error ingesting transaction: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while processing transaction")
