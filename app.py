from flask import Flask, request, jsonify
from cassandra.cluster import Cluster

app = Flask(__name__)


# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])
session = cluster.connect("iot_data")


# Define API endpoint to fetch sensor data
@app.route("/<sensor_id>", methods=["GET"])
def get_sensor_data(sensor_id):
    # start_time = request.args.get("start_time")
    # end_time = request.args.get("end_time")

    # print('yo', start_time, end_time)

    query = f"""
        SELECT * FROM iot_data.sensor_readings WHERE sensor_id = '{sensor_id}'
    """

    rows = session.execute(query)
    result = [
        {
            "sensor_id": row.sensor_id,
            "timestamp": row.timestamp,
            "sensor_type": row.sensor_type,
            "sensor_reading": row.sensor_reading,
            "unit": row.unit,
        }
        for row in rows
    ]

    return jsonify(result)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


if __name__ == "__main__":
    app.run(debug=True, port=5000)
