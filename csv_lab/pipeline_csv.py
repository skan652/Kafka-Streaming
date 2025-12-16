"""
pipeline.py
----------------
This file launches BOTH producer and consumer using Python threads to simulate
a real streaming pipeline.

"""

import threading
from producer_csv import CSVProducer
from consumer_csv import run_consume

def main():
    consumer_thread = threading.Thread(target=run_consume, daemon=True)
    consumer_thread.start()

    producer_instance= CSVProducer("data/transactions_dirty.csv")
    producer_thread = threading.Thread(target=producer_instance.run)
    producer_thread.start()

    producer_thread.join()
    print("Pipeline finished. Press Ctrl+C to stop consumer if running...")

if __name__ == "__main__":
    main()
