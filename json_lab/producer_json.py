import json
import time
import os
from kafka import KafkaProducer


class JSONProducer:
    def __init__(self, json_file, topic="transactions_json", delay: float = 1.0):
        self.json_file = json_file
        self.topic = topic
        self.delay = delay
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def run(self):
        print("📤 JSON Producer started")

        with open(self.json_file, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"⚠ Skipping malformed JSON on line {i}: {e}")
                    continue

                # basic schema enforcement / type casting
                try:
                    event["transaction_id"] = int(event.get("transaction_id"))
                    event["user_id"] = int(event.get("user_id"))
                except Exception as e:
                    print(f"⚠ Skipping record with bad IDs on line {i}: {e}")
                    continue

                # amount may be missing or non-numeric
                amt = event.get("amount")
                try:
                    event["amount"] = float(amt) if amt not in (None, "") else None
                except Exception:
                    print(f"⚠ Non-numeric amount on line {i} ({amt})")
                    event["amount"] = None

                # timestamp left as-is; consumer can validate if needed

                self.producer.send(self.topic, event)
                print(f"Sent → {event}")
                time.sleep(self.delay)

        self.producer.flush()
        self.producer.close()
        print("✅ JSON Producer finished")


# allow running as a script for quick tests
if __name__ == "__main__":
    base = os.path.dirname(__file__)
    clean = os.path.join(base, "data", "transactions.jsonl")
    dirty = os.path.join(base, "data", "transactions_dirty.jsonl")

    # by default send clean data, override with environment variable for dirty
    path = dirty if os.getenv("USE_DIRTY") == "1" else clean
    JSONProducer(path).run()

