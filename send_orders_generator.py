import time
import json
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os


CONNECTION_STRING = os.getenv("connection_string")
EVENT_HUB_NAME = os.getenv("event_hub_name")
# ==============================
# CREATE PRODUCER
# ==============================
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    eventhub_name=EVENT_HUB_NAME
)

regions = ["North", "South", "East", "West"]
products = ["P100", "P200", "P300", "P400"]

print("Starting real-time event stream...")

# ==============================
# STREAM LOOP (1 event/sec)
# ==============================
while True:
    order_event = {
        "order_id": random.randint(1000, 9999),
        "customer_id": f"C{random.randint(1, 100)}",
        "product_id": random.choice(products),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "region": random.choice(regions)
    }

    event_json = json.dumps(order_event)

    with producer:
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(event_json))
        producer.send_batch(event_data_batch)

    print(f"Sent: {event_json}")

    # 1 event per second
    time.sleep(1)