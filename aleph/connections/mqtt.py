import json
import logging
import time
import uuid

from typing import Dict, List
from aleph.utils.mqtt_client import MqttClient, MqttUtils
from aleph.models.record_set import Record


class MqttConnection:

    def __init__(self, client_name: str):
        self.client_name = client_name
        self.read_timeout = 10

        self.mqtt_client = None
        self._read_request_response = {}

    def open(self) -> None:
        self._create_client()
        self.mqtt_client.connect()

    def close(self) -> None:
        if self.mqtt_client:
            self.mqtt_client.disconnect()

    def read(self, key: str, **kwargs) -> List[Record]:
        request = self._send_read_request(key, **kwargs)
        request_id = request.get("request_id")

        t = time.time()
        while request_id not in self._read_request_response:
            time.sleep(0.1)
            if time.time() - t > self.read_timeout:
                raise TimeoutError("Read request timeout")

        return self._read_request_response.pop(request_id, None)

    def write(self, key: str, records: List[Record]) -> None:
        topic = MqttUtils.namespace_key_to_topic(key, "w")
        payload = json.dumps(records)
        self.mqtt_client.publish(topic, payload)

    def subscribe(self, key: str) -> None:
        topic = MqttUtils.namespace_key_to_topic(key, "w")
        self.mqtt_client.subscribe(topic)

    def on_message(self, key: str, records: List[Record]) -> None:
        return

    def _create_client(self):
        if self.mqtt_client is not None:
            return

        self.mqtt_client = MqttClient()
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

    def _on_connect(self):
        logging.info("Connected")

    def _on_disconnect(self):
        logging.info("Disconnected")

    def _on_message(self, topic: str, message: str):
        key = MqttUtils.topic_to_namespace_key(topic)
        payload = json.loads(message)
        records = payload["records"]
        request_id = payload.get("request_id")
        if request_id:
            self._read_request_response[request_id] = records
        else:
            self.on_message(key, records)

    def _send_read_request(self, key: str, **kwargs) -> Dict:
        topic = MqttUtils.namespace_key_to_topic(key, "r")
        response_topic = MqttUtils.namespace_key_to_topic(key, self.client_name)
        request = {"key": key, "response_topic": response_topic, "request_id": str(uuid.uuid4())}
        self.mqtt_client.subscribe_once(response_topic)
        self.mqtt_client.publish(topic, json.dumps(request))
        return request
