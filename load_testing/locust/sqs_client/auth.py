import datetime
import random
import time

from locust import User, between, task

random.seed(a=(datetime.time()))


class SQSClient(object):
    """SQS Client to send msgs to a queue."""

    _locust_environment = None
    wait_time = between(0, 1)

    def __getattr__(self, name):
        func = SQSClient.__getattr__(self, name)

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                func(*args, **kwargs)
            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                self._locust_environment.events.request_failure.fire(
                    request_type="send_msg", name=name, response_time=total_time, exception=e
                )
            else:
                total_time = int((time.time() - start_time) * 1000)
                self._locust_environment.events.request_success.fire(
                    request_type="send_msg", name=name, response_time=total_time, response_length=0
                )

        return wrapper

    def request(self, queue_url, message_body, client):
        args = {
            "QueueUrl": queue_url,
            "MessageBody": message_body,
            "DelaySeconds": 0,
        }
        if queue_url.endswith(".fifo"):
            args.update(
                {"MessageDeduplicationId": str(random.random()), "MessageGroupId": "load-test"}
            )

        response = client.send_message(**args)
        return response
