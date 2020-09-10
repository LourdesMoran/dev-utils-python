import random
import ssl
import time
from datetime import datetime
from typing import Dict, List

from locust import between
from paho.mqtt import publish

random.seed(a=(datetime.time()))


class MQTTClient(object):
    """SQS Client to send msgs to a queue."""

    _locust_environment = None
    wait_time = between(0, 1)

    def __getattr__(self, name):
        func = MQTTClient.__getattr__(self, name)

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

    def request(
        self,
        msgs: List[Dict],
        mqtt_host: str,
        mqt_username: str,
        mqtt_password: str,
        mqtt_port: int = 8883,
    ):
        client_id = "mqtt_bridge_" + datetime.now().strftime("%s")
        return publish.multiple(
            msgs,
            hostname=mqtt_host,
            auth={"username": mqt_username, "password": mqtt_password},
            port=mqtt_port,
            client_id=client_id,
            tls={
                "ca_certs": self.MQTT_CERT_PATH,
                "tls_version": ssl.PROTOCOL_TLSv1_2,
                "insecure": True,  # only for non prod environments
            },
            keepalive=30,
        )
