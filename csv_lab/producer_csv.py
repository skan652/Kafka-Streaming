import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.encode("utf-8")
)

TOPIC = "transactions"

print("📤 Producer started")

with open("data/transactions_dirty.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)

    for i, row in enumerate(reader, start=2):
        if len(row) != len(header):
            print(f"⚠ Skipping row {i} (wrong columns)")
            continue

        message = ",".join(row)
        producer.send(TOPIC, message)
        print(f"Sent → {message}")
        time.sleep(1)

producer.flush()
producer.close()
print("✅ Producer finished")
