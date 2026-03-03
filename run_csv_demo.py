#!/usr/bin/env python3
"""
CSV Lab Demo - Phase 1: Clean CSV Data Streaming
Demonstrates basic Kafka streaming with CSV producer/consumer
"""

import sys
import threading
import time
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'csv_lab'))

from csv_lab.producer_csv import CSVProducer
from csv_lab.consumer_csv import run_consume


def run_csv_demo():
    """Run the CSV streaming demo with clean data"""
    print("\n" + "="*60)
    print("[CSV STREAMING - PHASE 1: CSV DATA]")
    print("="*60)
    
    print("\nDataset: transactions.csv (clean data)")
    print("   - 201 transaction records")
    print("   - Fields: transaction_id, user_id, amount, timestamp")
    
    print("\nObjectives:")
    print("   * Stream CSV data into Kafka")
    print("   * Understand topics, partitions, and offsets")
    print("   * Work with consumer groups")
    
    # Start consumer thread (daemon - runs indefinitely)
    print("\nStarting Consumer Group: 'csv-consumer-group'")
    consumer_thread = threading.Thread(target=run_consume, daemon=True)
    consumer_thread.start()
    
    # Give consumer time to subscribe
    time.sleep(2)
    
    # Start producer
    print("\nStarting Producer: Sending CSV rows to 'transactions' topic...")
    csv_path = os.path.join(os.path.dirname(__file__), 'csv_lab', 'data', 'transactions.csv')
    producer = CSVProducer(csv_file=csv_path, delay=0.5)
    producer.run()
    
    print("\n" + "="*60)
    print("[CSV DEMO COMPLETED]")
    print("   Producer finished sending all records")
    print("   Consumer is still listening (daemon thread)")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_csv_demo()
