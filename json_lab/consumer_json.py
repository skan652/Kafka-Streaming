import json
from kafka import KafkaConsumer


def run_consume(topic="transactions_json"):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id="json-consumer-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8")
    )

    print("📥 JSON Consumer started")

    for msg in consumer:
        raw = msg.value
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            print(f"⚠ Received invalid JSON: {raw}")
            continue

        # optional basic sanity checks
        if not isinstance(event, dict):
            print(f"⚠ Unexpected message type: {type(event)}")
            continue

        print(f"Received ← {event}")

    print("✅ JSON Consumer finished")


if __name__ == "__main__":
    run_consume()