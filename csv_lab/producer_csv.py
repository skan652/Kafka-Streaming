import csv
import time
import os
from kafka import KafkaProducer


class CSVProducer:
    def __init__(self, csv_file, topic="transactions", delay: float = 1.0):
        self.csv_file = csv_file
        self.topic = topic
        self.delay = delay
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: v.encode("utf-8")
        )

    def run(self):
        print("📤 Producer started")
        with open(self.csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)

            for i, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    print(f"⚠ Skipping row {i} (wrong columns)")
                    continue

                message = ",".join(row)
                self.producer.send(self.topic, message)
                print(f"Sent → {message}")
                time.sleep(self.delay)

        self.producer.flush()
        self.producer.close()
        print("✅ Producer finished")


if __name__ == "__main__":
    base = os.path.dirname(__file__)
    dirty = os.path.join(base, "data", "transactions_dirty.csv")
    CSVProducer(dirty).run()
