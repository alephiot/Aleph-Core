import time
import json
import threading
from unittest import TestCase

from aleph_core import Connection
from aleph_core.connections.namespace.mqtt import MqttNamespaceConnection
from aleph_core.services.endpoint.mqtt import MqttEndpoint
from aleph_core.utils.docker import MosquittoContainer
from aleph_core.utils.mqtt_client import MqttClient


MESSAGE = {"r": 0, "a": "Hello", "b": True, "c": 213546847.1, "d": 5}
KEY = "my.test.key"
ARGS = {"x": "HELLO", "y": 234}
RECEIVED_MESSAGE = None


class TestMainConnection(Connection):
    def read(self, key: str, **kwargs):
        result = {}
        result.update(MESSAGE)
        result.update(kwargs)
        return result


class TestEndpoint(MqttEndpoint):
    endpoint_keys = [KEY]
    link_connection = MqttNamespaceConnection()
    main_connection = TestMainConnection()


class ServiceTestCase(TestCase):
    mosquitto_server = MosquittoContainer()
    mqtt_client = MqttClient()

    @classmethod
    def setUpClass(cls):
        cls.mosquitto_server.run()
        cls.mqtt_client.on_new_message = cls.mqtt_client_new_message
        cls.mqtt_client.connect()

    @classmethod
    def tearDownClass(cls):
        cls.mqtt_client.disconnect()
        cls.mosquitto_server.stop()

    @staticmethod
    def mqtt_client_new_message(topic, message):
        global RECEIVED_MESSAGE
        RECEIVED_MESSAGE = message

    def test_endpoint(self):
        test_endpoint = TestEndpoint()
        threading.Thread(target=test_endpoint.run, daemon=True).start()

        time.sleep(2)
        key_ = KEY.replace('.', '/')
        self.mqtt_client.subscribe_once(f"alv1/1234/{key_}")

        args_ = {}
        args_.update(ARGS)
        args_.update({"response_code": "1234"})
        self.mqtt_client.publish(f"alv1/r/{key_}", json.dumps(args_))

        time.sleep(2)
        msg = json.loads(RECEIVED_MESSAGE)
        self.assertIsNotNone(msg)
        self.assertTrue("data" in msg)

        expected_message = {}
        expected_message.update(ARGS)
        expected_message.update(MESSAGE)
        self.assertEqual(msg["data"][0], expected_message)
