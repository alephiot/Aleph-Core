import logging

from aleph_core import Service
from aleph_core.connections.namespace.mqtt import MqttNamespaceConnection

logger = logging.getLogger(__name__)


class MqttEndpoint(Service):
    endpoint_keys = []
    link_connection: MqttNamespaceConnection

    def load(self):
        self.main_connection_subscribe_keys = {}
        self.link_connection_subscribe_keys = {}
        super().load()

        for key in self.endpoint_keys:
            read_request_topic = self.link_connection.key_to_topic(key, 'r')
            self.link_connection.client.subscribe(read_request_topic)

    def on_new_data_from_link_connection(self, key, data):
        args: dict = data[0] if isinstance(data, list) else data
        response_code: str = args.pop("response_code", None)

        if response_code:
            response = self.link_connection.data_to_mqtt_message(self.on_read_request(key, **args))
            topic = self.link_connection.key_to_topic(key, response_code)
            self.link_connection.client.publish(topic, response)

    def on_read_request(self, key, **kwargs):
        logger.info(f"Received read request for key '{key}' with kwargs {kwargs}")
        return self.main_connection.safe_read(key, **kwargs)
