from flask import Flask, jsonify
from influxdb_client import InfluxDBClient
import os

app = Flask(__name__)

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "irrigacao-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "fazenda")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "irrigation")

client = InfluxDBClient(url=INFLUX_URL,
            token = INFLUX_TOKEN,
            org = INFLUX_ORG
            )
query_api = client.query_api()


@app.route("/")
def zones_status():

    query = f'''
    from(bucket:"{INFLUX_BUCKET}")
      |> range(start:-1h)
      |> filter(fn:(r) => r._measurement == "soil_readings")
      |> last()
    '''

    tables = query_api.query(query)

    result = []

    for table in tables:
        for record in table.records:
            result.append({
                "zone": record.values.get("zone_id"),
                "field": record.get_field(),
                "value": record.get_value(),
                "timestamp": str(record.get_time())
            })

    return jsonify(result)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000
    )