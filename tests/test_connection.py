import random
import time

from unittest import TestCase

from aleph_core import Connection
from aleph_core import Model


class TestModel(Model):
    a: str
    b: int
    c: float
    d: bool


class CaughtReadException(Exception):
    pass


class CaughtWriteException(Exception):
    pass


class TestConnection(Connection):
    connected = False
    force_close = False

    connection_events = []
    time_step = 1

    random.seed(1)
    models = {"A": TestModel}

    written_values = {}

    c = -1
    sequence_c = [
        [{"id_": "1", "value": "a"}, {"id_": "2", "value": "x"}],
        [{"id_": "1", "value": "b"}, {"id_": "2", "value": "y"}],
        [{"id_": "1", "value": "b"}],
        [{"id_": "1", "value": "c"}, {"id_": "2", "value": "x"}],
    ]

    d = -1
    sequence_d = [
        [{"value": "a"}],
        [{"value": "b"}],
        [{"value": "b"}],
        [{"value": "c"}],
    ]

    x = -1
    y = -1

    def read(self, key, **kwargs):
        if key == "A":
            return {
                "a": ''.join(random.choice("abcdefghijklmnopqrstuvwxyz") for i in range(9)),
                "b": random.randint(0, 100),
                "c": random.random() * 100,
                "d": random.random() > 0.5,
            }

        elif key == "B":
            return {
                "a": ''.join(random.choice("abcdefghijklmnopqrstuvwxyz") for i in range(9)),
                "b": random.randint(0, 100),
                "c": random.random() * 100,
                "d": random.random() > 0.5,
            }

        elif key == "C":
            self.c += 1
            return self.sequence_c[self.c]

        elif key == "D":
            self.d += 1
            return self.sequence_c[self.d]

    def open(self):
        if self.force_close:
            raise Exception("Could not open")

        self.connection_events.append("open")
        self.connected = True

    def close(self):
        self.connection_events.append("close")
        self.connected = False

    def is_open(self):
        return self.connected

    def on_connect(self):
        self.connection_events.append("on_connect")

    def on_disconnect(self):
        self.connection_events.append("on_disconnect")

    def write(self, key, data):
        if key == "X":
            self.x += 1
            if self.x < 2:
                raise Exception("X")

        if key == "Y":
            self.y += 1
            if self.y < 2:
                raise Exception("Y")

        self.written_values[key] = data

    def on_read_error(self, error):
        raise CaughtReadException(error.exception)

    def on_write_error(self, error):
        raise CaughtWriteException(error.exception)


class ConnectionTestCase(TestCase):
    conn = None

    def setUp(self):
        self.conn = TestConnection()
        self.conn.open()

    def tearDown(self):
        self.conn.close()

    def test_read(self):
        data = self.conn.read("A")
        self.assertTrue(isinstance(data, dict))
        self.assertEqual(len(data), 4)

    def test_write(self):
        self.conn.write("A", [{"a": "test"}])
        self.assertEqual(self.conn.written_values.get("A"), [{"a": "test"}])

        self.conn.write("B", [{"b": "test"}])
        self.assertEqual(self.conn.written_values.get("B"), [{"b": "test"}])

    def test_safe_read(self):
        data = self.conn.safe_read("A")
        self.assertTrue(isinstance(data, list))
        self.assertEqual(len(data), 1)
        self.assertEqual(len(data[0]), 4)

        try:
            data = self.conn.safe_read("Z")
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtReadException))
            self.assertTrue("Reading function returned None" in str(e))

    def test_safe_write(self):
        try:
            self.conn.safe_write("A", {"a": "test"})
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtWriteException))
            self.assertTrue("3 validation errors for TestModel" in str(e))

        try:
            self.conn.safe_write("Z", {"a": "test"})
        except Exception as e:
            raise

        test_record = {
            "a": "hello",
            "b": 1,
            "c": 2.5,
            "d": False,
        }
        self.conn.safe_write("A", [test_record])
        written_record = self.conn.written_values.get("A")[-1]
        self.assertTrue(all([test_record[r] == written_record[r] for r in test_record]))

        altered_record = {
            "a": "hello",
            "b": "1",
            "c": "2.5",
            "d": "False",
        }
        self.conn.safe_write("A", [altered_record])
        written_record = self.conn.written_values.get("A")[-1]
        self.assertTrue(all([test_record[r] == written_record[r] for r in test_record]))

        self.conn.safe_write("B", {"b": "test"})
        self.assertTrue("t" in self.conn.written_values.get("B")[0])
        self.assertTrue("id_" in self.conn.written_values.get("B")[0])

    def test_report_by_exception(self):
        self.conn.report_by_exception = True
        _data = [
            {"id_": "1", "a": "alpha", "b": 1},
            {"id_": "2", "a": "gamma", "b": 1},
            {"id_": "3", "a": "delta", "b": 1},
        ]
        self.conn.safe_write("C", _data)
        self.assertEqual(self.conn.written_values.get("C"), _data)

        _data = [
            {"id_": "1", "a": "hello", "b": 1},
            {"id_": "2", "a": "gamma", "b": 2}
        ]
        self.conn.safe_write("C", _data)
        self.assertEqual(self.conn.written_values.get("C"), [
            {"id_": "1", "a": "hello"},
            {"id_": "2", "b": 2}
        ])

        # Now the same but without report by exception
        self.conn.report_by_exception = False
        _data = [
            {"id_": "1", "a": "alpha", "b": 1},
            {"id_": "2", "a": "gamma", "b": 1},
            {"id_": "3", "a": "delta", "b": 1},
        ]
        self.conn.safe_write("D", _data)
        self.assertEqual(self.conn.written_values.get("D"), _data)

        _data = [
            {"id_": "1", "a": "hello", "b": 1},
            {"id_": "2", "a": "gamma", "b": 2}
        ]
        self.conn.safe_write("D", _data)
        self.assertNotEqual(self.conn.written_values.get("D"), [
            {"id_": "1", "a": "hello"},
            {"id_": "2", "b": 2}
        ])

    def test_store_and_forward(self):
        self.conn.store_and_forward = True

        try:
            self.conn.safe_write("X", {"a": 1})
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtWriteException))

        time.sleep(1)
        try:
            self.conn.safe_write("X", {"a": 2})
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtWriteException))

        time.sleep(1)
        self.conn.safe_write("X", {"a": 3})
        _data = self.conn.written_values.get("X")
        self.assertTrue(len(_data), 3)
        self.assertTrue(_data[0].get("a"), 1)
        self.assertTrue(_data[1].get("a"), 2)
        self.assertTrue(_data[2].get("a"), 3)

        # Now the same but without store and forward
        self.conn.store_and_forward = False
        try:
            self.conn.safe_write("Y", {"a": 1})
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtWriteException))

        time.sleep(1)
        try:
            self.conn.safe_write("Y", {"a": 2})
        except Exception as e:
            self.assertTrue(isinstance(e, CaughtWriteException))

        time.sleep(1)
        self.conn.safe_write("Y", {"a": 3})
        _data = self.conn.written_values.get("Y")
        self.assertTrue(len(_data), 1)
        self.assertTrue(_data[0].get("a"), 3)

    def test_connection_events(self):
        self.conn.connection_events.clear()

        self.conn.open_async()
        time.sleep(3)

        self.conn.close()
        self.conn.force_close = True
        time.sleep(3)

        self.conn.force_close = False
        time.sleep(3)

        self.assertEqual(
            self.conn.connection_events,
            ['on_connect', 'close', 'on_disconnect', 'open', 'on_connect']
        )
