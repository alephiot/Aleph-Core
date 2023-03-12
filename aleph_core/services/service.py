import logging
import time

from aleph_core import Connection, Error

logger = logging.getLogger(__name__)


class Service:
    def on_error(self):
        pass

    def on_new_data(self, key, data):
        self.mqtt_connection.write_async(key, data)

    def on_status_change(self):
        self.status = 2

    def load(self):
        device_connection = DeviceConnection()
        mqtt_connection = MqttConnection()

        device_connection.on_error = self.on_error
        mqtt_connection.on_error = self.on_error

        device_connection.on_connect = self.on_status_change
        mqtt_connection.on_connect = self.on_status_change
        device_connection.on_disconnect = self.on_status_change
        mqtt_connection.on_disconnect = self.on_status_change

        device_connection.on_new_data = self.on_new_data

        device_connection.open_async()
        mqtt_connection.open_async()

        device_connection.subscribe_async()

        pass
