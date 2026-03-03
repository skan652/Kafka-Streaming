from kafka import KafkaConsumer


def run_consume(topic="transactions"):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id="csv-consumer-group",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v.decode("utf-8")
    )

    print("📥 Consumer started")
    for msg in consumer:
        print(f"Received ← {msg.value}")
    print("✅ Consumer finished")


if __name__ == "__main__":
    run_consume()