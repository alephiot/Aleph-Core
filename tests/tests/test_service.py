import time
from unittest import TestCase

from aleph_core import Connection
from aleph_core import Service

T0 = time.time()


class Events:
    OPENING_MAIN_CONNECTION = "Opening Main Connection"
    OPENING_LINK_CONNECTION = "Opening Link Connection"
    CLOSING_MAIN_CONNECTION = "Closing Main Connection"
    CLOSING_LINK_CONNECTION = "Closing Link Connection"
    READING_MAIN_CONNECTION = "Reading Main Connection"
    WRITING_LINK_CONNECTION = "Writing Link Connection"
    ERROR = "Service error"
    STATUS_CHANGE = "Service status changed"

    __events__ = []

    def add(self, event, msg=""):
        if msg:
            event += ": " + str(msg)
        self.__events__.append(event)

    def all(self):
        return self.__events__


events = Events()


class TestMainConnection(Connection):
    i = 0
    t0: float
    time_step = 1
    connected = False

    def __init__(self):
        super().__init__()
        self.t0 = time.time()

    def open(self):
        if 3 < time.time() - self.t0 < 7:
            raise Exception("Cannot Open Main Connection")
        self.connected = True
        events.add(Events.OPENING_MAIN_CONNECTION)

    def close(self):
        if self.connected:
            events.add(Events.CLOSING_MAIN_CONNECTION)
            self.connected = False

    def is_open(self):
        if 3 < time.time() - self.t0 < 7:
            return False
        return self.connected

    def read(self, key, **kwargs):
        self.i += 1

        if not self.connected:
            raise Exception("Main Connection Not Connected")

        events.add(Events.READING_MAIN_CONNECTION, f'i = {self.i}')
        return {"i": self.i}


class TestLinkConnection(Connection):
    t0: float
    time_step = 1
    connected = False

    def __init__(self):
        super().__init__()
        self.t0 = time.time()

    def open(self):
        if 5 < time.time() - self.t0 < 9:
            raise Exception("Cannot Open Link Connection")
        self.connected = True
        events.add(Events.OPENING_LINK_CONNECTION)

    def close(self):
        if self.connected:
            events.add(Events.CLOSING_LINK_CONNECTION)
            self.connected = False

    def is_open(self):
        if 5 < time.time() - self.t0 < 9:
            return False
        return self.connected

    def write(self, key, data):
        if not self.connected:
            raise Exception("Link Connection Not Connected")

        i = data[0]["i"]
        events.add(Events.WRITING_LINK_CONNECTION, f'i = {i}')


class TestService(Service):
    max_runtime = 10
    main_connection = TestMainConnection()
    link_connection = TestLinkConnection()
    main_connection_subscribe_keys = ["default"]
    link_connection_subscribe_keys = {}

    def on_error(self, error):
        events.add(Events.ERROR, error.title)

    def on_status_change(self, status_code: int):
        events.add(Events.STATUS_CHANGE, str(status_code))


class ServiceTestCase(TestCase):

    # TODO

    def test_service_run(self):
        test_service = TestService()
        test_service.main_connection_subscribe_keys = []
        test_service.link_connection_subscribe_keys = []
        test_service.run()

        for event in events.all():
            print(event)
