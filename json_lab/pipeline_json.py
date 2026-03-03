import threading
import os
from producer_json import JSONProducer
from consumer_json import run_consume


def main():
    print("🚀 Starting JSON Streaming Pipeline...")

    # consumer thread runs indefinitely (daemon)
    consumer_thread = threading.Thread(target=run_consume, daemon=True)
    consumer_thread.start()

    base = os.path.dirname(__file__)
    dirty = os.path.join(base, "data", "transactions_dirty.jsonl")

    # start producer with dirty data for demonstration
    producer = JSONProducer(json_file=dirty, delay=1.0)
    producer_thread = threading.Thread(target=producer.run)
    producer_thread.start()

    producer_thread.join()
    print("✅ Pipeline finished. Consumer still listening in background...")


if __name__ == "__main__":
    main()
