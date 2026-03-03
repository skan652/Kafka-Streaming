#!/usr/bin/env python3
"""
JSON Lab Demo - Phase 2: Structured JSON Data Streaming  
Demonstrates Kafka streaming with structured JSON events and error handling
"""

import sys
import threading
import time
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'json_lab'))

from json_lab.producer_json import JSONProducer
from json_lab.consumer_json import run_consume


def run_json_demo():
    """Run the JSON streaming demo with dirty data to show error handling"""
    print("\n" + "="*60)
    print("[JSON STREAMING - PHASE 2: JSON DATA]")
    print("="*60)
    
    print("\nDataset: transactions_dirty.jsonl (dirty data)")
    print("   - 15 transaction records with quality issues:")
    print("     * Missing amounts")
    print("     * Invalid user_id values")
    print("     * Invalid timestamps")
    print("     * Non-numeric amounts")
    
    print("\nObjectives:")
    print("   * Stream structured JSON events")
    print("   * Validate and type-cast data")
    print("   * Handle malformed records gracefully")
    print("   * Understand schema evolution")
    
    # Start consumer thread (daemon - runs indefinitely)
    print("\nStarting Consumer Group: 'json-consumer-group'")
    consumer_thread = threading.Thread(target=run_consume, daemon=True)
    consumer_thread.start()
    
    # Give consumer time to subscribe
    time.sleep(2)
    
    # Start producer with dirty data
    print("\nStarting Producer: Sending JSON events to 'transactions_json' topic...")
    json_path = os.path.join(os.path.dirname(__file__), 'json_lab', 'data', 'transactions_dirty.jsonl')
    producer = JSONProducer(json_file=json_path, delay=0.5)
    producer.run()
    
    print("\n" + "="*60)
    print("[JSON DEMO COMPLETED]")
    print("   Producer finished sending all records")
    print("   Consumer is still listening (daemon thread)")
    print("   Notice: Malformed records were skipped with warnings")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_json_demo()
