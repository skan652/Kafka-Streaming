# Write your json consumer code here
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "transactions_json",
    bootstrap_servers="localhost:9092",
    group_id="json-consumer-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: v.decode("utf-8")
)

print("📥 JSON Consumer started")

for msg in consumer:
    print(f"Received ← {msg.value}")
print("✅ JSON Consumer finished")