from typing import Callable, Optional
import paho.mqtt.client as mqtt
import time


class MqttClient:
    """
    Wrapper around paho mqtt client
    """

    def __init__(self, **kwargs):
        self.client_id = kwargs.get("client_id", "")
        self.broker = kwargs.get("client_id", "localhost")
        self.port = kwargs.get("port", 1883)
        self.username = kwargs.get("username", "")
        self.password = kwargs.get("password", "")

        self.on_connect: Callable = None  # callback function()
        self.on_disconnect: Callable = None  # callback function()
        self.on_message: Callable[[str, str]] = None  # callback function(topic, message)

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
        self.client: mqtt.Client = None

        self.__subscribe_topics__ = set()
        self.__subscribe_topics_once__ = set()

    def __on_connect__(self, client, userdata, flags, rc):
        self.connected = True
        self.connecting = False

        for topic in self.__subscribe_topics__:
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

    def __on_message__(self, client, userdata, msg):
        topic = str(msg.topic)
        message = str(msg.payload.decode())

        if (
            topic not in self.__subscribe_topics__
            and topic not in self.__subscribe_topics_once__
            and True not in [topic.startswith(t[:-1]) for t in self.__subscribe_topics__]
        ):
            return
        self.__subscribe_topics_once__.discard(topic)

        if self.on_message is not None:
            self.on_message(topic, message)

    def __setup__(self):
        if self.client is not None:
            return

        self.client = mqtt.Client(self.client_id, clean_session=not self.persistent)
        self.client.username_pw_set(username=self.username, password=self.password)
        self.client.on_connect = self.__on_connect__
        self.client.on_disconnect = self.__on_disconnect__
        self.client.on_message = self.__on_message__

        if self.tls_enabled:
            self.client.tls_set(
                ca_certs=self.ca_cert, certfile=self.client_cert, keyfile=self.client_key
            )

        if self.last_will_topic and self.last_will_topic:
            self.client.will_set(
                topic=self.last_will_topic,
                payload=self.last_will_message,
                qos=1,
            )

    def connect(self, timeout: int = 10) -> bool:
        """
        Connect to the broker. This is a blocking call, and will retry until it connects or times
        out. It returns a boolean indicating if the connection was successful.
        """
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
                    raise TimeoutError(f"Mqtt Client (id: {self.client_id}) failed to connect")

        except Exception:
            self.connecting = False
            raise

        self.client.loop_start()
        return True

    def connect_async(self):
        """
        Same as connect, but non-blocking.
        """
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

    def disconnect(self) -> None:
        """
        Disconnect from the broker
        """
        if self.client is None:
            return False

        self.client.disconnect()
        self.client = None
        return True

    def publish(self, topic: str, payload: str, qos: Optional[int] = None) -> None:
        """
        Publish a message to a topic. Raises a runtime error if the message is not published.
        """
        msg_info = self.client.publish(topic, payload, qos if qos else self.qos)
        if msg_info.rc == 1:
            raise RuntimeError("Connection refused, unacceptable protocol version (r = 1)")
        elif msg_info.rc == 2:
            raise RuntimeError("Connection refused, identifier rejected (r = 2)")
        elif msg_info.rc == 3:
            raise RuntimeError("Connection refused, server unavailable (r = 3)")
        elif msg_info.rc == 4:
            raise RuntimeError("Connection refused, bad username or password (r = 4)")
        elif msg_info.rc == 5:
            raise RuntimeError("Connection refused, not authorized (r = 5)")
        elif msg_info.rc > 0:
            raise RuntimeError(f"Mqtt error (r = {msg_info.rc})")

    def subscribe(self, topic: str) -> None:
        """
        Subscribe to a topic
        """
        self.client.subscribe(topic, qos=self.qos)
        self.__subscribe_topics__.add(topic)

    def unsubscribe(self, topic: str) -> None:
        """
        Unsubscribe from a topic
        """
        self.client.unsubscribe(topic)
        self.__subscribe_topics__.discard(topic)
        self.__subscribe_topics_once__.discard(topic)

    def subscribe_once(self, topic: str) -> None:
        """
        Subscribe to a topic and disconnect after receiving a message
        """
        self.__subscribe_topics_once__.add(topic)
        self.client.subscribe(topic)


class MqttUtils:
    ALEPH_V1_PROTOCOL = "alv1"

    @classmethod
    def topic_to_namespace_key(cls, topic: str) -> str:
        """
        Derive a namespace key from a topic, according to the Aleph v1 protocol
        """
        topic = str(topic)
        if topic.startswith(cls.ALEPH_V1_PROTOCOL) and len(topic) > 7:
            s_index = topic.index("/", 5) + 1
            topic = topic[s_index:]
        return topic.replace("/", ".")

    @classmethod
    def namespace_key_to_topic(cls, key: str, verb: str = "w") -> str:
        """
        Derive a topic from a namespace key, according to the Aleph v1 protocol
        """
        assert verb in ["w", "r", "c"], f"Invalid verb '{verb}'"
        return f"{cls.ALEPH_V1_PROTOCOL}/{verb}/{str(key).replace('.', '/')}"
