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
            for row in reader:
                message = ','.join(row)
                self.producer.send(self.topic_name, value=message)
                print(f'Sent: {message}')
                time.sleep(self.delay)  # Simulate delay between messages
        print("📤 CSV Producer finished sending messages.")