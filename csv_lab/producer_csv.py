import time
import csv
from timeit import main
from kafka import KafkaProducer

BROKER = 'localhost:9092'
TOPIC_NAME = 'csv_topic'
CSV_FILE_PATH = 'data/transactions.csv'

def produce_csv_data():
    producer = KafkaProducer(bootstrap_servers=BROKER)

    with open(CSV_FILE_PATH, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        
        header = next(csvreader)  # Skip header row if present

        for row in csvreader:
            message = ','.join(row).encode('utf-8')
            producer.send(TOPIC_NAME, message)
            print(f'Sent: {message}')
            time.sleep(0.5)  # Simulate delay between messages

    print("📤 CSV Producer started...")
    
    producer.flush()
    producer.close()
    print("📤 CSV Producer finished sending messages.")

    if main():
        produce_csv_data()