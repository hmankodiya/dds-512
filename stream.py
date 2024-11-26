import argparse
import random
import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
import json
from cassandra.cluster import Cluster

# Argument Parser Setup
parser = argparse.ArgumentParser()
parser.add_argument("--save_csv", help="save to pandas", type=bool, default=False)
parser.add_argument("--num_readings", help="number of readings per sensor", type=int, default=1)

# Initialize Faker and other settings
fake = Faker()

num_sensors = 10  # Number of sensors
time_interval = timedelta(minutes=1)  # Time between sensor readings

# Define sensor types and their data ranges
sensor_types = {
    "temperature": {"min": 15, "max": 35, "unit": "Celsius"},
    "humidity": {"min": 30, "max": 70, "unit": "%"},
    "air_quality": {"min": 50, "max": 200, "unit": "AQI"},
}

# Create a list of sensors with random sensor types
sensors = [
    {
        "sensor_id": f"sensor_{i+1:03}",
        "sensor_type": random.choice(list(sensor_types.keys())),
    }
    for i in range(num_sensors)
]


# Generate synthetic data
def generate_sensor_data(num_readings):
    start_time = datetime.now() - timedelta(days=1)  # Start from 24 hours ago
    for _ in range(num_readings):
        for sensor in sensors:
            current_time = start_time
            reading = np.random.uniform(
                sensor_types[sensor["sensor_type"]]["min"],
                sensor_types[sensor["sensor_type"]]["max"],
            )
            yield {
                "sensor_id": sensor["sensor_id"],
                "timestamp": current_time.isoformat(),
                "sensor_type": sensor["sensor_type"],
                "sensor_reading": round(reading, 2),
                "unit": sensor_types[sensor["sensor_type"]]["unit"],
            }
            current_time += time_interval   


# Send data to Kafka producer
def send_to_kafka(producer, data):
    producer.send("iot-sensor-data", value=data)
    print(f"Sent to Kafka: {data}")
    time.sleep(0.1)  # Simulate delay between readings


# Connect to Cassandra and insert data
def send_to_cassandra(session, data):
    query = f"""
            INSERT INTO iot_data.sensor_readings 
            (sensor_id, timestamp, sensor_type, sensor_reading, unit) 
            VALUES ('{str(data['sensor_id'])}', '{data['timestamp']}', '{data['sensor_type']}', {data['sensor_reading']}, '{str(data['unit'])}');
            """
    session.execute(query)
    print(f"Inserted into Cassandra: {data}")


# Kafka Consumer to read from Kafka and send to Cassandra
def consume_from_kafka_and_send_to_cassandra():
    consumer = KafkaConsumer(
        "iot-sensor-data",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="iot-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    cluster = Cluster(["127.0.0.1"])  # Replace with your Cassandra cluster IP
    session = cluster.connect()
    session.set_keyspace("iot_data")  # Keyspace where your data is stored

    for message in consumer:
        data = message.value
        send_to_cassandra(session, data)  # Send to Cassandra


if __name__ == "__main__":
    # Parse arguments
    args = parser.parse_args()

    # Kafka Producer setup
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],  # Replace with your Kafka broker
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),  # Serialize as JSON
    )

    # Start streaming data to Kafka
    try:
        print("Starting to stream IoT data...")
        for data in generate_sensor_data(args.num_readings):
            send_to_kafka(producer, data)  # Send to Kafka

            # Save to CSV if specified
            if args.save_csv:
                df = pd.DataFrame([data])
                df.to_csv(
                    "synthetic_iot_sensor_data.csv", mode="a", header=False, index=False
                )

    except KeyboardInterrupt:
        print("Streaming stopped.")
    finally:
        producer.close()

    # Start consuming from Kafka and sending to Cassandra
    try:
        print("Starting to consume data from Kafka and send to Cassandra...")
        consume_from_kafka_and_send_to_cassandra()
    except KeyboardInterrupt:
        print("Consumer stopped.")
