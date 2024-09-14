import logging
import json

from aleph.utils.docker import DockerManager
from aleph.utils.mqtt_client import MqttClient, MqttUtils
from aleph.utils.time import loop


class TestBroker:

    def __init__(self, fixture_factory):
        self.docker_manager = DockerManager()
        self.mqtt_client = MqttClient()
        self.fixture_factory = fixture_factory

    def on_connect(self):
        logging.info("Connected")
        topic = MqttUtils.namespace_key_to_topic("#", "r")
        self.mqtt_client.subscribe(topic)

    def on_disconnect(self):
        logging.info("Disconnected")

    def on_message(self, topic: str, message: str):
        logging.info(f"Received on topic '{topic}': {message}")

        # Get fixtures
        key = MqttUtils.topic_to_namespace_key(topic)
        response = self.fixture_factory.generate(key)

        # Send response
        parsed_response = json.dumps([record.dict() for record in response])
        parsed_message = json.loads(message)
        response_topic = parsed_message.get("response_topic")
        if not response_topic:
            raise ValueError(f"Response topic not found in message ({message})")

        logging.info(f"Sending response to {response_topic} ({len(response)} records)")
        return self.mqtt_client.publish(response_topic, parsed_response)

    def run(self):
        self.docker_manager.run_mosquitto_server()

        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.connect()

        loop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TestBroker().run()
