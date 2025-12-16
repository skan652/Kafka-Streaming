"""
pipeline.py
----------------
This file launches BOTH producer and consumer using Python threads to simulate
a real streaming pipeline.

"""

import threading
from queue import Queue
from producer_csv import main as run_producer
from consumer_csv import main as run_consumer

def main():
    producer = threading.Thread(target=run_producer)
    consumer = threading.Thread(target=run_consumer)

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()
