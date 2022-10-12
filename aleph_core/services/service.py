import logging
import time

from aleph_core import Connection

logger = logging.getLogger(__name__)


class Service:
    """
    TODO
    """

    main_connection: Connection
    link_connection: Connection
    main_connection_subscribe_keys = {}
    link_connection_subscribe_keys = {}

    __status__ = None

    def __init__(self, service_id=""):
        self.service_id = service_id

    def on_new_data_from_main_connection(self, key, data):
        self.link_connection.write_async(key, data)

    def on_new_data_from_link_connection(self, key, data):
        self.main_connection.write_async(key, data)

    def on_error(self, error):
        return

    def on_status_change(self, status_code: int):
        """
        0: both connection and namespace_connection are connected
        1: connection is not connected
        2: namespace_connection is not connected
        3: neither is connected
        """
        return

    @property
    def status(self):
        return self.__status__

    def __on_status_change__(self):
        status0 = self.main_connection.is_open()
        status1 = self.link_connection.is_open()

        current_status = None
        if status0 and status1:
            current_status = 0
        elif status0 and not status1:
            current_status = 1
        elif not status0 and status1:
            current_status = 2
        else:
            current_status = 3

        if current_status != self.__status__:
            self.__status__ = current_status
            self.on_status_change(self.__status__)

    def load(self):
        """Connect callbacks"""
        logger.info("Loading service")

        self.main_connection.on_new_data = self.on_new_data_from_main_connection
        self.main_connection.on_read_error = self.on_error
        self.main_connection.on_write_error = self.on_error

        self.link_connection.on_new_data = self.on_new_data_from_link_connection
        self.link_connection.on_read_error = self.on_error
        self.link_connection.on_write_error = self.on_error

        self.link_connection.open_async()
        self.main_connection.open_async()

        time.sleep(1)
        self.main_connection.on_connect = self.__on_status_change__
        self.link_connection.on_connect = self.__on_status_change__
        self.main_connection.on_disconnect = self.__on_status_change__
        self.link_connection.on_disconnect = self.__on_status_change__
        self.__on_status_change__()

        for key in self.main_connection_subscribe_keys:
            if isinstance(self.main_connection_subscribe_keys, dict):
                time_step = self.main_connection_subscribe_keys.get(key)
            else:
                time_step = self.main_connection.time_step
            self.main_connection.subscribe_async(key, time_step)

        for key in self.link_connection_subscribe_keys:
            if isinstance(self.link_connection_subscribe_keys, dict):
                time_step = self.link_connection_subscribe_keys.get(key)
            else:
                time_step = self.link_connection.time_step
            self.link_connection.subscribe_async(key, time_step)

    def run(self, max_runtime: int = None):
        self.load()
        logger.info("Starting service")
        start_time = time.time()
        while True:
            time.sleep(1)
            if max_runtime and time.time() - start_time > max_runtime:
                logger.info("Stopping service")
                self.main_connection.close()
                self.link_connection.close()
                break
