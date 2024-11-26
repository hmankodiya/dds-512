# dds-512

pip install pandas numpy faker kafka-python cassandra-driver

CREATE KEYSPACE iot_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE iot_data.sensor_readings (
    sensor_id text,
    timestamp timestamp,
    sensor_type text,
    sensor_reading float,
    unit text,
    PRIMARY KEY ((sensor_id, sensor_type), timestamp)
);	