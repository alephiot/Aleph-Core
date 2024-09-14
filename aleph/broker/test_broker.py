from aleph.utils.docker import DockerManager
from aleph.utils.mqtt_client import MqttClient


class TestBroker:

    def __init__(self):
        self.docker_manager = DockerManager()
        self.mqtt_client = MqttClient()

    def run(self):
        self.docker_manager.run_mosquitto_server()

        self.mqtt_client.on_connect = lambda: print("Connected")

        self.mqtt_client.connect()


if __name__ == "__main__":
    TestBroker().run()
