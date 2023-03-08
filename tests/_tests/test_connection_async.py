import time

from aleph_core.connections.testing.random_connection import RandomConnection
from aleph_core.connections.testing.simple_connection import SimpleConnection
from aleph_core.connections.connection import Connection
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.exceptions import Error
from aleph_core.utils.data import RecordSet


class SomeConnection(Connection):
    opened = False
    force_close = False
    events = []

    def open(self):
        # time.sleep(1)
        if self.force_close:
            raise Exceptions.ConnectionOpeningTimeout()
        self.opened = True
        self.events.append("OPENED")

    def close(self):
        self.opened = False
        self.force_close = True
        self.events.append("CLOSED")

    def is_open(self):
        return self.opened and not self.force_close

    def on_connect(self):
        self.events.append("CONNECTED")

    def on_disconnect(self):
        self.events.append("DISCONNECTED")

    def read(self, key, **kwargs):
        time.sleep(1)
        self.events.append("READ")

    def write(self, key, data):
        time.sleep(1)
        self.events.append("WRITE")


def test_open_async():
    some_connection = SomeConnection()
    assert some_connection.is_open() == False

    # Open async
    some_connection.open_async(1)
    time.sleep(2)
    assert some_connection.is_open() == True

    # Force close
    some_connection.force_close = True
    time.sleep(2)
    assert some_connection.is_open() == False

    # Open again
    some_connection.force_close = False
    time.sleep(2)
    assert some_connection.is_open() == True

    # Close
    some_connection.close()
    time.sleep(2)
    assert some_connection.is_open() == False

    assert some_connection.events == [
        "OPENED",
        "CONNECTED",
        "DISCONNECTED",
        "OPENED",
        "CONNECTED",
        "CLOSED",
        "DISCONNECTED",
    ]


def test_subscribe_async():
    pass


def test_write_async():
    pass
