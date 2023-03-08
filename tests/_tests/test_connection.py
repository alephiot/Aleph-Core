import pytest
import time

from aleph_core.connections.testing.random_connection import RandomConnection
from aleph_core.connections.testing.simple_connection import SimpleConnection
from aleph_core.connections.connection import Connection
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.exceptions import Error
from aleph_core.utils.data import RecordSet


class SomeConnection(Connection):
    ONLY_VALID_KEY = "xyz"
    errors = []
    events = []

    def open(self):
        self.events.append("OPENING")

    def close(self):
        self.events.append("CLOSING")

    def on_connect(self):
        self.events.append("CONNECT")

    def on_disconnect(self):
        self.events.append("DISCONNECT")

    def read(self, key, **kwargs):
        self.events.append("READING")
        if key == self.ONLY_VALID_KEY:
            return {"r": 1}

    def write(self, key, data):
        self.events.append("WRITING")
        if key == self.ONLY_VALID_KEY:
            time.sleep(1)

    def on_error(self, error: Error):
        self.errors.append(error)


def test_open_close():
    """Test the connection open and closes correctly"""
    simple_connection = SimpleConnection()

    with pytest.raises(Exceptions.ConnectionNotOpen):
        simple_connection.read()

    simple_connection.open()
    simple_connection.read()
    simple_connection.close()

    with pytest.raises(Exceptions.ConnectionNotOpen):
        simple_connection.read()


def test_open_connection_on_safe_read():
    """Test the connection open and closes on safe read / write"""
    simple_connection = SimpleConnection()

    with pytest.raises(Exceptions.ConnectionNotOpen):
        simple_connection.read()

    simple_connection.safe_read()
    simple_connection.close()

    with pytest.raises(Exceptions.ConnectionNotOpen):
        simple_connection.read()


def test_read_and_safe_read():
    """Test the connection can read and write"""
    random_connection = RandomConnection()
    random_connection.open()

    random_connection.seed(0)
    read_result = random_connection.read()
    random_connection.seed(0)
    safe_read_result = random_connection.read()

    for field in ["str_", "float_", "int_", "bool_"]:
        assert read_result[0][field] == safe_read_result[0][field]


def test_read_fails():
    some_connection = SomeConnection()

    safe_read_result = some_connection.safe_read("INVALID_KEY")
    assert safe_read_result is None
    assert len(some_connection.errors) == 1


def test_write_and_safe_write():
    simple_connection = SimpleConnection()
    simple_connection.open()
    key = "key"
    record = {"r": 1}

    simple_connection.write(key, RecordSet(record))
    assert simple_connection.written[key][-1]["r"] == record["r"]

    simple_connection.safe_write(key, record)
    assert simple_connection.written[key][-1]["r"] == record["r"]


def test_open_async():
    some_connection = SomeConnection()
    


def test_subscribe_async():
    pass


def test_write_async():
    pass
