"""
Usage:

locust -f load_test_sqs/locustfile.py --headless -u 1000 -r 100 --run-time 3m

-u specifies the number of Users to spawn, and
-r specifies the hatch rate (number of users to spawn per second).
--run-time duration of the tests.
"""
import datetime
import json
import os
import random
import string

import boto3
from locust import User, task

from auth import SQSClient

# Parameters
REGION = os.getenv("AWS_REGION")
SQS_URL = os.getenv("SQS_URL")
SQS_CONN = boto3.client("sqs", region_name=REGION)


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


class SQSUser(User):
    min_wait = 0
    max_wait = 0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = SQSClient()
        self.client._locust_environment = self.environment

    @task()
    def send_data(self):
        self.client.request(
            SQS_URL, self.generate_payload(), SQS_CONN,
        )

    def generate_payload(self) -> str:
        return json.dumps(
            {"frequency": 10, "timestamp": str(datetime.datetime.utcnow().isoformat()) + "Z",}
        )
