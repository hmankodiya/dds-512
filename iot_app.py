from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
from datetime import datetime

app = Flask(__name__)

# Connect to Cassandra
cluster = Cluster(["127.0.0.1"])  # Replace with your Cassandra cluster IP
session = cluster.connect()
session.set_keyspace("iot_data")  # Keyspace where your data is stored


@app.route("/query", methods=["GET"])
def query_sensor_data():
    # Extract query parameters (all are optional)
    sensor_id = request.args.get("sensor_id")
    start_time = request.args.get("start_time")  # 'YYYY-MM-DD HH:MM:SS'
    end_time = request.args.get("end_time")  # 'YYYY-MM-DD HH:MM:SS'
    min_reading = request.args.get("min_reading")  # Minimum sensor reading
    max_reading = request.args.get("max_reading")  # Maximum sensor reading

    # Optional fields to retrieve (default is all)
    fields = request.args.get(
        "fields", "*"
    )  # Example: "sensor_id,timestamp,sensor_reading"

    # Start building the query
    query = f"SELECT {fields} FROM sensor_readings WHERE"
    conditions = []
    params = []

    # Add conditions dynamically based on the parameters provided
    if sensor_id:
        conditions.append("sensor_id = %s")
        params.append(sensor_id)
    if start_time:
        try:
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            conditions.append("timestamp >= %s")
            params.append(start_time)
        except ValueError:
            return (
                jsonify(
                    {"error": "Invalid start_time format. Use 'YYYY-MM-DD HH:MM:SS'"}
                ),
                400,
            )
    if end_time:
        try:
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
            conditions.append("timestamp <= %s")
            params.append(end_time)
        except ValueError:
            return (
                jsonify(
                    {"error": "Invalid end_time format. Use 'YYYY-MM-DD HH:MM:SS'"}
                ),
                400,
            )
    if min_reading:
        conditions.append("sensor_reading >= %s")
        params.append(float(min_reading))
    if max_reading:
        conditions.append("sensor_reading <= %s")
        params.append(float(max_reading))

    # Combine conditions into the query
    if conditions:
        query += " " + " AND ".join(conditions)
    else:
        return jsonify({"error": "At least one query parameter must be provided"}), 400

    # Allow filtering for non-primary-key conditions
    query += " ALLOW FILTERING;"

    # Execute the query
    try:
        rows = session.execute(query, tuple(params))
        result = []
        for row in rows:
            if fields == "*":  # Return all fields
                result.append(
                    {
                        "sensor_id": row.sensor_id,
                        "timestamp": row.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "sensor_type": row.sensor_type,
                        "sensor_reading": row.sensor_reading,
                        "unit": row.unit
                    }
                )
            else:  # Return only requested fields
                field_list = fields.split(",")
                row_data = {
                    field: getattr(row, field)
                    for field in field_list
                    if hasattr(row, field)
                }
                if "timestamp" in row_data:
                    row_data["timestamp"] = row.timestamp.strftime("%Y-%m-%d %H:%M:%S")
                result.append(row_data)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


if __name__ == "__main__":
    app.run(debug=True)
