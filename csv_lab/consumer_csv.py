import time
import csv
from os import name
from kafka import KafkaConsumer

def run_consume():
    consumer  = KafkaConsumer(
        "transactions",
        bootstrap_servers='localhost:9092',
        group_id='csv-consmer-group',
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

    print("📥 CSV Consumer finished processing messages.")      # Simulate processing delay