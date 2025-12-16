import time
import csv
from timeit import main
from kafka import KafkaConsumer

BROKER = 'localhost:9092'
TOPIC_NAME = 'csv_topic'
CSV_FILE_PATH = 'data/transactions.csv'

def consume_csv_data():
    consumer  = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BROKER,
        group_id='csv_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8'),
        validate_messages=True
    )

    print("📥 CSV Consumer started...")

    for message in consumer:
        print(f'Received: {message.value}'
              f' | Topic: {message.topic}'
              f' | Partition: {message.partition}'
              f' | Offset: {message.offset}')
        time.sleep(1) 

    print("📥 CSV Consumer finished processing messages.")    
        
if __name__ == "__main__":
    main()         # Simulate processing delay