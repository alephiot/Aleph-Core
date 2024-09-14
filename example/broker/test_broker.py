import logging

from aleph.utils.docker import DockerManager
from aleph.utils.mqtt_client import MqttClient
from aleph.utils.time import loop

from example.models.user import User


fake_user = User(name="John Doe", email="johndoe@example.org")


class TestBroker:

    def __init__(self):
        self.docker_manager = DockerManager()
        self.mqtt_client = MqttClient()

    def on_connect(self):
        logging.info("Connected")
        self.mqtt_client.subscribe("#")

    def on_disconnect(self):
        logging.info("Disconnected")

    def on_message(self, message: str, topic: str):
        logging.info(f"Received on topic '{topic}': {message}")
        # Get key
        # Match key to fixtures
        #

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
