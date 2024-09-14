import json
import time
import asyncio
from random import randint

from aleph_core import Connection
from aleph_core.utils.mqtt_client import MqttClient
from aleph_core.utils.exceptions import Exceptions


class MqttNamespaceConnection(Connection):
    client_id = ""
    broker = "localhost"
    port = 1883
    username = ""
    password = ""
    qos = 1
    keepalive = 10
    persistent = False
    tls_enabled = False
    ca_cert = ""
    client_cert = ""
    client_key = ""

    read_timeout = 10
    __read_request_data__: dict = {}
    client: MqttClient = None

    def open(self):
        self.__create_client__()
        self.client.connect()

    def close(self):
        if self.client:
            self.client.disconnect()

    def read(self, key, **kwargs):
        self.__send_read_request__(key, **kwargs)

        t = time.time()
        while key not in self.__read_request_data__:
            time.sleep(0.1)
            if time.time() - t > self.read_timeout:
                raise Exceptions.ConnectionReadingTimeout

        return self.__read_request_data__.pop(key, None)

    async def _read(self, key, **kwargs):
        self.__send_read_request__(key, **kwargs)

        t = time.time()
        while key not in self.__read_request_data__:
            await asyncio.sleep(0.1)
            if time.time() - t > self.read_timeout:
                raise Exceptions.ConnectionReadingTimeout

        return self.__read_request_data__.pop(key, None)

    def write(self, key, data):
        msg_info = self.client.publish(self.key_to_topic(key), self.data_to_mqtt_message(data))

        if msg_info.rc == 1:
            raise Exception("Connection refused, unacceptable protocol version (r = 1)")
        elif msg_info.rc == 2:
            raise Exception("Connection refused, identifier rejected (r = 2)")
        elif msg_info.rc == 3:
            raise Exception("Connection refused, server unavailable (r = 3)")
        elif msg_info.rc == 4:
            raise Exception("Connection refused, bad username or password (r = 4)")
        elif msg_info.rc == 5:
            raise Exception("Connection refused, not authorized (r = 5)")
        elif msg_info.rc > 0:
            raise Exception(f"Mqtt error (r = {msg_info.rc})")

    def is_open(self):
        return self.client and self.client.connected

    def open_async(self, time_step=None):
        self.__create_client__()
        self.client.connect_async()

    def subscribe_async(self, key, time_step=None):
        self.client.subscribe(self.key_to_topic(key))

    def write_async(self, key, data):
        self.write(key, data)

    def unsubscribe(self, key):
        self.client.unsubscribe(self.key_to_topic(key))

    def __create_client__(self):
        if self.client is not None:
            return

        self.client = MqttClient()
        self.client.client_id = self.client_id
        self.client.broker = self.broker
        self.client.port = self.port
        self.client.username = self.username
        self.client.password = self.password
        self.client.qos = self.qos
        self.client.keepalive = self.keepalive
        self.client.persistent = self.persistent
        self.client.tls_enabled = self.tls_enabled
        self.client.ca_cert = self.ca_cert
        self.client.client_cert = self.client_cert
        self.client.client_key = self.client_key

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_new_message = self.__on_new_message__

    def __send_read_request__(self, key, **kwargs):
        request = {'t': time.time(), 'response_code': str(randint(0, 999999999))}
        request.update(kwargs)
        self.client.subscribe_once(self.key_to_topic(key, request['response_code']))
        self.client.publish(self.key_to_topic(key, 'r'), self.data_to_mqtt_message(request))
        return request

    def __on_new_message__(self, topic, message):
        key = self.topic_to_key(topic)
        data = self.mqtt_message_to_data(message)

        if topic.startswith("alv1/") and not topic.startswith("alv1/w") and not topic.startswith("alv1/r"):
            self.__read_request_data__[key] = data
        else:
            self.on_new_data(key, data)

    def topic_to_key(self, topic):
        topic = str(topic)
        if topic.startswith("alv1") and len(topic) > 7:
            s_index = topic.index("/", 5) + 1
            topic = topic[s_index:]
        return topic.replace("/", ".")

    def key_to_topic(self, key, mode="w"):
        return f"alv1/{mode}/{str(key).replace('.', '/')}"

    def data_to_mqtt_message(self, data):
        data = {
            "sender": self.client_id,
            "data": data,
        }
        return json.dumps(data, default=str)

    def mqtt_message_to_data(self, message):
        data = json.loads(message)
        sender = data.get("sender")
        if sender == self.client_id:
            return None
        elif sender:
            return data.get("data")
        else:
            return data
