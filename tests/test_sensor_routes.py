import json
import pytest
import sqlite3
import time
import unittest

from app import app

class SensorRoutesTestCases(unittest.TestCase):

    def setUp(self):
        # Setup the SQLite DB
        conn = sqlite3.connect('test_database.db')
        conn.execute('DROP TABLE IF EXISTS readings')
        conn.execute('CREATE TABLE IF NOT EXISTS readings (device_uuid TEXT, type TEXT, value INTEGER, date_created INTEGER)')
        
        self.device_uuid = 'test_device'

        # Setup some sensor data
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'temperature', 100, int(time.time())))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 22, int(time.time()) - 100))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 50, int(time.time()) - 50))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    (self.device_uuid, 'humidity', 100, int(time.time())))

        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'temperature', 22, int(time.time())))
        cur.execute('insert into readings (device_uuid,type,value,date_created) VALUES (?,?,?,?)',
                    ('other_uuid', 'humidity', 42, int(time.time())))
        conn.commit()
        conn.close()

        app.config['TESTING'] = True

        self.client = app.test_client

    def test_device_readings_get(self):
        # Given a device UUID
        # When we make a request with the given UUID
        request = self.client().get('/devices/{}/readings/'.format(self.device_uuid))

        # Then we should receive a 200
        self.assertEqual(request.status_code, 200)

        # And the response data should have three sensor readings
        assert len(request.json) == 6

    def test_device_readings_post(self):
        # Given a device UUID
        # When we make a request with the given UUID to create a reading
        request = self.client().post('/devices/{}/readings/'.format(self.device_uuid), data=
            json.dumps({
                'type': 'temperature',
                'value': 100 
            }))

        # Then we should receive a 201
        self.assertEqual(201, request.status_code, request.data)

        # And when we check for readings in the db
        conn = sqlite3.connect('test_database.db')
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('select * from readings where device_uuid="{}"'.format(self.device_uuid))
        rows = cur.fetchall()

        # We should have three
        assert len(rows) == 7

    def test_device_readings_get_temperature(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's temperature data only.
        """

        request = self.client().get(f'/devices/{self.device_uuid}/readings/?type=temperature')

        assert request.status_code == 200
        assert len(request.json) == 3

    def test_device_readings_get_humidity(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's humidity data only.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/?type=humidity')

        assert request.status_code == 200
        assert len(request.json) == 3

    def test_device_readings_get_past_dates(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's sensor data over
        a specific date range. We should only get the readings
        that were created in this time range.
        """
        start = int(time.time()) - 120
        end = int(time.time()) - 50

        request = self.client().get(f'/devices/{self.device_uuid}/readings/?start={start}&end={end}')

        assert request.status_code == 200
        assert len(request.json) == 2

    def test_device_readings_min(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's min sensor reading.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/min/?type=temperature')

        assert request.status_code == 200
        assert request.json['value'] == 22

    def test_device_readings_max(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's max sensor reading.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/max/?type=temperature')

        assert request.status_code == 200
        assert request.json['value'] == 100

    def test_device_readings_median(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's median sensor reading.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/median/?type=temperature')

        assert request.status_code == 200
        assert request.json['value'] == 50

    def test_device_readings_mean(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mean sensor reading value.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/mean/?type=temperature')

        assert request.status_code == 200
        assert request.json['value'] == 57.33

    def test_device_readings_mode(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's mode sensor reading value.
        """
        request = self.client().get(f'/devices/{self.device_uuid}/readings/mode/?type=temperature')

        assert request.status_code == 200
        assert request.json['value'] == 100

    def test_device_readings_quartiles(self):
        """
        This test should be implemented. The goal is to test that
        we are able to query for a device's 1st and 3rd quartile
        sensor reading value.
        """
        start = int(time.time()) - 120
        end = int(time.time())
        request = self.client().get(f'/devices/{self.device_uuid}/readings/quartiles/?type=temperature&start={start}&end={end}')

        assert request.status_code == 200
        assert request.json == {'quartile_1': 22, 'quartile_3': 50}
