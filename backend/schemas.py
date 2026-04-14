from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class Transaction(BaseModel):
    transaction_id: str = Field(...)
    user_id: str = Field(...)
    amount: float = Field(..., gt=0)
    currency: str = Field(...)
    merchant_id: str = Field(...)
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_ABC123",
                "user_id": "user_456",
                "amount": 707.00,
                "currency": "USD",
                "merchant_id": "merch_789",
                "latitude": 41.015137,
                "longitude": 28.979530,
                "timestamp": "2024-04-14T10:00:00Z"
            }
        }
