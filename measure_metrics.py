import csv
import time
from cassandra.cluster import Cluster

# Connect to Cassandra
def get_cassandra_session():
    cluster = Cluster(['127.0.0.1'])  # Adjust for your Cassandra cluster
    session = cluster.connect('iot_data')  # Connect to the keyspace
    return session

# Measure query response time
def measure_query_response_time(session):
    start_time = time.time()
    rows = session.execute("SELECT * FROM iot_data.sensor_readings LIMIT 10;")
    end_time = time.time()
    response_time = end_time - start_time
    return response_time

# Measure throughput
def measure_throughput(session, num_queries):
    start_time = time.time()
    for _ in range(num_queries):
        session.execute("SELECT * FROM iot_data.sensor_readings LIMIT 1;")
    end_time = time.time()
    throughput = num_queries / (end_time - start_time)
    return throughput

# Kafka-Cassandra latency simulation
def simulate_kafka_latency():
    producer_time = time.time()
    time.sleep(0.1)  # Simulating Kafka processing delay
    consumer_time = time.time()
    return consumer_time - producer_time

# Save evaluation results to a CSV file
def save_results_to_csv(results, csv_file='evaluation_results.csv'):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['Metric', 'Value'])
        # Write results
        for metric, value in results.items():
            writer.writerow([metric, value])
    print(f"Evaluation results saved to {csv_file}.")

# Run evaluations
if __name__ == "__main__":
    session = get_cassandra_session()

    # Number of queries for throughput evaluation
    num_queries = 100

    # Perform evaluations
    results = {
        "Query Response Time (seconds)": measure_query_response_time(session),
        "Throughput (queries/second)": measure_throughput(session, num_queries),
        "Kafka-Cassandra Latency (seconds)": simulate_kafka_latency()
    }

    # Save results to CSV
    save_results_to_csv(results)
