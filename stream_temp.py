import argparse
import random

import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from faker import Faker
from kafka import KafkaProducer


parser = argparse.ArgumentParser()
parser.add_argument("--save_csv", help="save to pandas", type=bool, default=False)
parser.add_argument(
    "--num_readings", help="number of readings per sensor", type=int, default=1
)


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
def generate_sensor_data():
    data = []
    start_time = datetime.now() - timedelta(days=1)  # Start from 24 hours ago

    for sensor in sensors:
        current_time = start_time
        reading = np.random.uniform(
            sensor_types[sensor["sensor_type"]]["min"],
            sensor_types[sensor["sensor_type"]]["max"],
        )
        data.append(
            {
                "sensor_id": sensor["sensor_id"],
                "timestamp": current_time,
                "sensor_type": sensor["sensor_type"],
                "sensor_reading": round(reading, 2),
                "unit": sensor_types[sensor["sensor_type"]]["unit"],
            }
        )
        current_time += time_interval

    return data

def send_producer(producer, data):
    producer.send('iot-sensor-data', value=data)
    time.sleep(0.1)
    


if __name__ == "__main__":
    # Parse arguments
    args = parser.parse_args()

    # Generate data for 24 hours (1440 minutes, 1 reading per minute per sensor)
    num_readings = args.num_readings
    
    while
    sensor_data = generate_sensor_data(num_readings)



    # Convert to pandas DataFrame and save as CSV
    if args.save_csv:
        df = pd.DataFrame(sensor_data)
        df.to_csv("synthetic_iot_sensor_data.csv", index=False)

        print(
            f"Generated {len(df)} rows of sensor data and saved to 'synthetic_iot_sensor_data.csv'."
        )
