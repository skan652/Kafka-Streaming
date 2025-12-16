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
    producer = threading.Thread(target=CSVProducer().run)

    consumer_thread.start()
    producer.run()

    print("Pipeline finished. Press Ctrl+C to stop consumer if running...")

if __name__ == "__main__":
    main()
