from os import name
import time
import csv
from timeit import main
from kafka import KafkaProducer

class CSVProducer:
    def __init__(self, csv_file_path='data/transactions.csv', broker='localhost:9092', topic_name='csv_topic', delay=1.0):
        self.csv_file_path = csv_file_path
        self.broker = broker
        self.topic_name = topic_name
        self.delay = delay
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda x: x.encode('utf-8')
        )

    def run(self):
        print("📤 CSV Producer started...")
        with open(self.csv_file_path, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Skip header row
            for i, row in enumerate(reader, start=2):
                try:
                    if len(row) != len(header):
                        print(f"Warning: Row {i} has {len(row)} columns, expected {len(header)}. Skipping row.")
                        continue
                    message = ','.join(row)
                    self.producer.send(self.topic_name, value=message)
                    print(f'Sent: {message} (Row {i})')
                    time.sleep(self.delay)  # Simulate delay between messages
                except Exception as e:
                    print(f"Error sending message: {e}")