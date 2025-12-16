import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "transactions_json"

print("📤 JSON Producer started")

with open("../csv_lab/data/transactions.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)

    for row in reader:
        event = {
            "transaction_id": int(row[0]),
            "user_id": int(row[1]),
            "amount": float(row[2]),
            "timestamp": row[3]
        }

        producer.send(TOPIC, event)
        print(f"Sent → {event}")
        time.sleep(1)

producer.flush()
producer.close()
print("✅ JSON Producer finished")
