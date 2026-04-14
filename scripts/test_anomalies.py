import requests
import time
import uuid
import random
from datetime import datetime, timedelta

url = "http://localhost:8000/transactions"

def send_tx(user_id, amount, lat, lon, tx_id=None):
    if not tx_id:
        tx_id = f"tx_{uuid.uuid4().hex[:8]}"
    
    data = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "currency": "USD",
        "merchant_id": "merch_test",
        "latitude": lat,
        "longitude": lon,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    try:
        response = requests.post(url, json=data)
        print(f"Sent tx {tx_id} for {user_id}: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("--- Test 1: Velocity Anomaly (>5 tx/min) ---")
    user_v = "user_velocity_test"
    for i in range(7):
        send_tx(user_v, 10.0, 41.0, 28.0)
        time.sleep(0.5)

    print("\n--- Test 2: Amount Anomaly (>3x 24h avg) ---")
    user_a = "user_amount_test"
    # First establish an average
    send_tx(user_a, 100.0, 41.0, 28.0)
    time.sleep(1)
    # Trigger anomaly
    send_tx(user_a, 400.0, 41.0, 28.0)

    print("\n--- Test 3: Location Anomaly (Impossible Travel) ---")
    user_l = "user_location_test"
    # Istanbul
    send_tx(user_l, 50.0, 41.0082, 28.9784)
    time.sleep(2)
    # New York (Impossible to reach in 2 seconds)
    send_tx(user_l, 50.0, 40.7128, -74.0060)

    print("\nTests triggered. Check worker logs and database for alerts.")
