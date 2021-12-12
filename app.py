from flask import Flask, render_template, request, Response
from flask.json import jsonify
import json
import sqlite3
import time

app = Flask(__name__)


# # Setup the SQLite DB
# conn = sqlite3.connect('database.db')
# conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
# conn.close()


def db_connection():
    # Set the db that we want and open the connection
    if app.config['TESTING']:
        conn = sqlite3.connect('test_database.db')
    else:
        conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    return cur, conn


def valid_uuid(uuid):
    return True


def valid_sensor_type(sensor_type):
    return sensor_type in ("humidity", "temperature")


def valid_epoch(epoch):
    if epoch is None:
        return True
    if not isinstance(int(epoch), int):
        return False

    try:
        time.gmtime(int(epoch))
    except (OSError, OverflowError):
        return False
    else:
        return True


def get_metric(metric, device_uuid, sensor_type, start, end):
    cur, _ = db_connection()

    maybe_start_clause = ""
    maybe_end_clause = ""
    maybe_sensor_clause = ""
    if start is not None:
        maybe_start_clause = f"AND date_created > {start}"
    if end is not None:
        maybe_end_clause = f"AND date_created < {end}"
    if sensor_type is not None:
        maybe_sensor_clause = f'AND type = "{sensor_type}"'

    if metric == "min":
        cur.execute(
            f'SELECT MIN(value) FROM readings WHERE '
            f'device_uuid= "{device_uuid}" {maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause}')
    elif metric == "max":
        cur.execute(
            f'SELECT MAX(value) FROM readings WHERE '
            f'device_uuid= "{device_uuid}" {maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause}')
    elif metric == "median":
        cur.execute(
            f'SELECT value FROM readings WHERE device_uuid="{device_uuid}" '
            f'{maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause} ORDER BY '
            f'value LIMIT 1 OFFSET (SELECT COUNT(*) FROM readings WHERE '
            f'device_uuid="{device_uuid}" AND type="{sensor_type}" {maybe_start_clause} {maybe_end_clause}) / 2')
    elif metric == "mean":
        cur.execute(
            f'SELECT AVG(value) FROM readings WHERE device_uuid="{device_uuid}" '
            f'{maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause}')
    elif metric == "mode":
        cur.execute(
            f'SELECT value, COUNT(value) as value FROM readings '
            f'WHERE device_uuid="{device_uuid}" '
            f'{maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause} '
            f'GROUP BY value ORDER BY value DESC')

    row = cur.fetchone()
    return row


def get_quartiles(device_uuid, sensor_type, start, end):
    cur, _ = db_connection()
    maybe_start_clause = ""
    maybe_end_clause = ""
    maybe_sensor_clause = ""
    if start is not None:
        maybe_start_clause = f"AND date_created > {start}"
    if end is not None:
        maybe_end_clause = f"AND date_created < {end}"
    if sensor_type is not None:
        maybe_sensor_clause = f'AND type = "{sensor_type}"'

    # get total number of items matching clause,
    cur.execute(f'SELECT value FROM readings WHERE device_uuid="{device_uuid}"'
                f'{maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause}')
    rows = cur.fetchall()
    values = [cols[0] for cols in rows]

    quartile_1 = None
    quartile_3 = None

    if values:
        quartile_1_index = int(len(values) * 0.25)
        quartile_3_index = int(len(values) * 0.75)
        quartile_1 = values[quartile_1_index]
        quartile_3 = values[quartile_3_index]

    return [quartile_1, quartile_3]


def get_number_of_readings(device_uuid):
    cur, _ = db_connection()
    cur.execute(f'SELECT COUNT(*) FROM readings WHERE device_uuid="{device_uuid}"')
    number = cur.fetchone()
    return number[0]


def get_device_uuids():
    cur, _ = db_connection()
    cur.execute(f'SELECT device_uuid FROM readings')
    uuids = cur.fetchall()
    # flatten and unique
    return sorted(list(set([item for sublist in uuids for item in sublist])))


@app.route('/devices/<string:device_uuid>/readings/', methods=['POST'])
def post_request_device_readings(device_uuid):
    """
    This endpoint allows clients to POST data specific sensor types.

    POST Parameters:
    * type -> The type of sensor (temperature or humidity)
    * value -> The integer value of the sensor reading
    * date_created -> The epoch date of the sensor reading.
        If none provided, we set to now.
    """
    cur, conn = db_connection()

    if request.method == 'POST':
        # Grab the post parameters
        post_data = json.loads(request.data)
        sensor_type = post_data.get('type')
        value = post_data.get('value')
        date_created = post_data.get('date_created') or int(time.time())

        if 0 < value < 100:
            return f'Value outside of bounds: {value}', 400
        elif not valid_sensor_type(sensor_type):
            return f'Invalid sensor type: {sensor_type}', 400

        # Insert data into db
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (device_uuid, sensor_type, value, date_created))

        conn.commit()

        # Return success
        return 'success', 201


@app.route('/devices/<string:device_uuid>/readings/', methods=['GET'])
def get_request_device_readings(device_uuid):
    """
    This endpoint allows clients to GET data specific sensor types.

    Optional Query Parameters:
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    * type -> The type of sensor value a client is looking for
    """
    cur, _ = db_connection()
    data = request.args
    sensor_type, start, end = data.get('type'), data.get('start'), data.get('end')
    maybe_start_clause = ""
    maybe_end_clause = ""
    maybe_sensor_clause = ""

    if not valid_uuid(device_uuid):
        return f"Invalid uuid: {device_uuid}", 400
    elif sensor_type and not valid_sensor_type(sensor_type):
        return f"Invalid sensor type: {sensor_type}", 400
    elif not valid_epoch(start):
        return f"Invalid start time: {start}", 400
    elif not valid_epoch(end):
        return f"Invalid end time: {end}", 400

    if sensor_type is not None:
        maybe_sensor_clause = f'AND type = "{sensor_type}"'
    if start is not None:
        maybe_start_clause = f"AND date_created > {start}"
    if end is not None:
        maybe_end_clause = f"AND date_created < {end}"
    # Execute the query
    cur.execute(f'SELECT * FROM readings WHERE device_uuid="{device_uuid}" '
                f'{maybe_sensor_clause} {maybe_start_clause} {maybe_end_clause}')
    rows = cur.fetchall()

    # Return the JSON
    return jsonify([dict(zip(['device_uuid', 'type', 'value', 'date_created'], row)) for row in rows]), 200


# incorporate max, median, mean into one route
@app.route('/devices/<string:device_uuid>/readings/<string:metric>/', methods=['GET'])
def request_device_readings_operation(device_uuid, metric):
    """
        This endpoint allows clients to GET the max, median, or mean sensor reading for a device.

        Mandatory Query Parameters:
        * type -> The type of sensor value a client is looking for

        Optional Query Parameters
        * start -> The epoch start time for a sensor being created
        * end -> The epoch end time for a sensor being created
        """
    data = request.args
    sensor_type, start, end = data.get('type'), data.get('start'), data.get('end')
    if metric not in ("min", "max", "median", "mean", "mode"):
        return f"Metric not found: {metric}", 400
    elif not valid_uuid(device_uuid):
        return f"Invalid uuid: {device_uuid}", 400
    elif not valid_sensor_type(sensor_type):
        return f"Invalid sensor type: {sensor_type}", 400
    elif not valid_epoch(start):
        return f"Invalid start time: {start}", 400
    elif not valid_epoch(end):
        return f"Invalid end time: {end}", 400

    if metric == "min":
        row = get_metric(metric, device_uuid, sensor_type, start, end)
    elif metric == "max":
        row = get_metric(metric, device_uuid, sensor_type, start, end)
    elif metric == "median":
        row = get_metric(metric, device_uuid, sensor_type, start, end)
    elif metric == "mean":
        row = get_metric(metric, device_uuid, sensor_type, start, end)
    elif metric == "mode":
        row = get_metric(metric, device_uuid, sensor_type, start, end)

    # Return success
    return jsonify({"value": float(f'{row[0]:.2f}')}), 200


@app.route('/devices/<string:device_uuid>/readings/quartiles/', methods=['GET'])
def request_device_readings_quartiles(device_uuid):
    """
    This endpoint allows clients to GET the 1st and 3rd quartile
    sensor reading value for a device.

    Mandatory Query Parameters:
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    data = request.args
    sensor_type, start, end = data.get('type'), data.get('start'), data.get('end')
    if not valid_uuid(device_uuid):
        return f"Invalid uuid: {device_uuid}", 400
    elif not valid_sensor_type(sensor_type):
        return f"Invalid sensor type: {sensor_type}", 400
    elif not valid_epoch(start):
        return f"Invalid start time: {start}", 400
    elif not valid_epoch(end):
        return f"Invalid end time: {end}", 400

    quartiles = get_quartiles(device_uuid, sensor_type, start, end)

    return jsonify({"quartile_1": quartiles[0], "quartile_3": quartiles[1]})


@app.route('/devices/readings/summary/', methods=['GET'])
def request_readings_summary():
    """
    This endpoint allows clients to GET a full summary
    of all sensor data in the database per device.

    Optional Query Parameters
    * type -> The type of sensor value a client is looking for
    * start -> The epoch start time for a sensor being created
    * end -> The epoch end time for a sensor being created
    """
    data = request.args
    sensor_type, start, end = data.get('type'), data.get('start'), data.get('end')

    if sensor_type and not valid_sensor_type(sensor_type):
        return f"Invalid sensor type: {sensor_type}", 400
    elif not valid_epoch(start):
        return f"Invalid start time: {start}", 400
    elif not valid_epoch(end):
        return f"Invalid end time: {end}", 400

    output = []
    uuids = get_device_uuids()

    for device_uuid in uuids:
        number_of_readings = get_number_of_readings(device_uuid)
        metric_max = get_metric('max', device_uuid, sensor_type, start, end)
        metric_median = get_metric('median', device_uuid, sensor_type, start, end)
        metric_mean = get_metric('mean', device_uuid, sensor_type, start, end)
        quartiles = get_quartiles(device_uuid, sensor_type, start, end)

        reading = {
            'device_uuid': device_uuid,
            'number_of_readings': number_of_readings,
            'max_reading_value': float(f'{metric_max[0]:.2f}'),
            'median_reading_value': float(f'{metric_median[0]:.2f}'),
            'mean_reading_value': float(f'{metric_mean[0]:.2f}'),
            'quartile_1_value': quartiles[0],
            'quartile_3_value': quartiles[1]
        }

        output.append(reading)

    # Return success
    return jsonify(output), 200


if __name__ == '__main__':
    app.run()
