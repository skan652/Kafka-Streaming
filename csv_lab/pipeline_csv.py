import threading
from producer_csv import CSVProducer
from consumer_csv import run_consume

def main():
    print("🚀 Starting CSV Streaming Pipeline...")

    # Start consumer thread
    consumer_thread = threading.Thread(target=run_consume, daemon=True)
    consumer_thread.start()

    # Start producer thread
    producer_instance = CSVProducer(csv_file='data/transactions_dirty.csv', delay=1.0)
    producer_thread = threading.Thread(target=producer_instance.run)
    producer_thread.start()

    # Wait for producer to finish
    producer_thread.join()
    print("✅ Pipeline finished. Consumer still listening in background...")

if __name__ == "__main__":
    main()

