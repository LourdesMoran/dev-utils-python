"""
Usage:

locust -f load_test_sqs/locustfile.py --headless -u 1000 -r 100 --run-time 3m

-u specifies the number of Users to spawn, and
-r specifies the hatch rate (number of users to spawn per second).
--run-time duration of the tests.

Alternatively, go to http://localhost:8089/ to use the UI.
"""
import datetime
import json
import os
import random
import string

import boto3
from locust import User, task

from auth import MQTTClient

# Parameters
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")


class MQTTUser(User):
    min_wait = 0
    max_wait = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = MQTTClient()
        self.client._locust_environment = self.environment

    @task()
    def send_data(self):
        self.client.request(self.generate_payload(), MQTT_HOST, MQTT_USERNAME, MQTT_PASSWORD)

    def generate_payload(self) -> str:
        return json.dumps(
            {"frequency": 10, "timestamp": str(datetime.datetime.utcnow().isoformat()) + "Z",}
        )
