import paho.mqtt.client as mqtt
from typing import Optional
import time


class MqttClient:
    """
    Wrapper around pago mqtt client
    """

    def __init__(self, **kwargs):
        self.client_id = kwargs.get("client_id", "")
        self.broker = kwargs.get("client_id", "localhost")
        self.port = kwargs.get("port", 1883)
        self.username = kwargs.get("username", "")
        self.password = kwargs.get("password", "")

        self.on_connect = None  # callback function()
        self.on_disconnect = None  # callback function()
        self.on_new_message = None  # callback function(topic, message)

        self.qos = kwargs.get("qos", 1)
        self.keepalive = kwargs.get("keepalive", 10)
        self.persistent = kwargs.get("persistent", False)

        self.tls_enabled = False
        self.ca_cert = ""
        self.client_cert = ""
        self.client_key = ""

        self.birth_topic = None
        self.birth_message = None
        self.last_will_topic = None
        self.last_will_message = None

        self.connected = False
        self.connecting = False
        self.client: Optional[mqtt.Client] = None

        self.__subscribe_topics__ = set()
        self.__subscribe_topics_once__ = set()

    def __on_connect__(self, client, userdata, flags, rc):
        self.connected = True
        self.connecting = False

        for topic in self.__subscribe_topics__ :
            self.client.subscribe(topic, qos=self.qos)

        if self.birth_topic is not None and self.birth_message is not None:
            self.publish(self.birth_topic, self.birth_message, qos=2)

        if self.on_connect is not None:
            self.on_connect()

    def __on_disconnect__(self, client, userdata, rc):
        self.connected = False
        self.connecting = False

        if self.on_disconnect is not None:
            self.on_disconnect()

    def __on_new_message__(self, client, userdata, msg):
        topic = str(msg.topic)
        message = str(msg.payload.decode())

        if (
            topic not in self.__subscribe_topics__ and
            topic not in self.__subscribe_topics_once__ and
            True not in [topic.startswith(t[:-1]) for t in self.__subscribe_topics__ ]
        ):
            return

        self.__subscribe_topics_once__.discard(topic)

        if self.on_new_message is not None:
            self.on_new_message(topic, message)

    def __setup__(self):
        if self.client is not None:
            return

        self.client = mqtt.Client(self.client_id, clean_session=not self.persistent)
        self.client.username_pw_set(username=self.username, password=self.password)
        self.client.on_connect = self.__on_connect__
        self.client.on_disconnect = self.__on_disconnect__
        self.client.on_message = self.__on_new_message__

        if self.tls_enabled:
            self.client.tls_set(
                ca_certs=self.ca_cert,
                certfile=self.client_cert,
                keyfile=self.client_key
            )

        if self.last_will_topic and self.last_will_topic:
            self.client.will_set(
                topic=self.last_will_topic,
                payload=self.last_will_message,
                qos=1,
            )

    def connect(self, timeout=10):
        if self.connected or self.connecting:
            return False

        try:
            self.connecting = True
            self.__setup__()
            t0 = time.time()

            while not self.connected:
                self.client.connect(self.broker, self.port, keepalive=self.keepalive)
                time.sleep(0.1)
                self.client.loop()
                if 0 < timeout < time.time() - t0:
                    self.connecting = False
                    raise Exception(f"Mqtt Client (client_id={self.client_id}) failed to connect")

        except Exception:
            self.connecting = False
            raise

        self.client.loop_start()
        return True

    def connect_async(self):
        if self.connected or self.connecting:
            return False

        try:
            self.__setup__()
            self.connecting = True
            self.client.connect_async(self.broker, self.port, keepalive=self.keepalive)
            self.client.loop_start()

        except Exception:
            self.connecting = False
            raise

        return True

    def disconnect(self):
        if self.client is None:
            return False

        self.client.disconnect()
        self.client = None
        return True

    def publish(self, topic: str, payload: str, qos=None):
        msg_info = self.client.publish(topic, payload, qos if qos else self.qos)
        return msg_info

    def subscribe(self, topic):
        self.client.subscribe(topic, qos=self.qos)
        self.__subscribe_topics__.add(topic)

    def unsubscribe(self, topic):
        self.client.unsubscribe(topic)
        self.__subscribe_topics__.discard(topic)
        self.__subscribe_topics_once__.discard(topic)

    def subscribe_once(self, topic):
        self.client.subscribe(topic)
        self.__subscribe_topics_once__.add(topic)
