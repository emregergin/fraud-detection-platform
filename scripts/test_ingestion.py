import requests
import time
import uuid
import random
from datetime import datetime

url = "http://localhost:8000/transactions"

def send_test_transaction():
    tx_id = f"tx_{uuid.uuid4().hex[:8]}"
    data = {
        "transaction_id": tx_id,
        "user_id": f"user_{random.randint(1, 4)}",
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "currency": "USD",
        "merchant_id": f"merch_{random.randint(1, 10)}",
        "latitude": 41.0082,
        "longitude": 28.9784,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    try:
        response = requests.post(url, json=data)
        if response.status_code == 201:
            print(f"Success | ID: {tx_id} | Response: {response.json()}")
        else:
            print(f"Failed  | ID: {tx_id} | Status: {response.status_code} | Error: {response.text}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print(f"Starting ingestion test to {url}...")
    for _ in range(3):
        send_test_transaction()
        time.sleep(1)
    print("Test completed.")
