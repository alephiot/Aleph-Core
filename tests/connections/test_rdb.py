import time
import os
from enum import Enum
from unittest import TestCase

from aleph_core.connections.rdb import RDBConnection
from aleph_core import Model

NOW = time.time()


class TestEnum(str, Enum):
    option_1 = "A"
    option_2 = "B"


class TestModel(Model, table=True):
    a: int
    b: str
    c: TestEnum = TestEnum.option_2

    @staticmethod
    def samples(count=None):
        samples = [
            TestModel(t=NOW - 100, a=1, b="hi", c=TestEnum.option_1),
            TestModel(t=NOW - 80,  a=2, b="by"),
            TestModel(t=NOW - 60,  a=3, b="ax"),
            TestModel(t=NOW - 40,  a=4, b="dw"),
            TestModel(t=NOW - 20,  a=5, b="rr"),
            TestModel(t=NOW - 0,   a=6, b="zu"),
        ]

        if count and count < len(samples):
            samples = samples[:count]

        return [x.dict() for x in samples]


class TestConnection(RDBConnection):
    url = "sqlite:///test.db"
    models = {"test.key": TestModel}


class RDBTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # if os.path.isfile("test.db"):
        #     os.remove("test.db")
        pass

    def test_read_write(self):
        conn = TestConnection()
        conn.open()

        # Write
        conn.write("test.key", TestModel.samples())

        # Read
        a = conn.read("test.key", limit=1)
        self.assertEqual(len(a), 1)

        b = conn.read("test.key", limit=1, offset=1)
        self.assertEqual(len(b), 1)
        self.assertNotEqual(a[0]["id_"], b[0]["id_"])

        c = conn.read("test.key", since=NOW - 50)
        self.assertEqual(len(c), 3)

        d = conn.read("test.key", since=NOW - 50, until=NOW - 30)
        self.assertEqual(len(d), 1)

        e = conn.read("test.key", order="b")
        f = conn.read("test.key", order="-b")
        self.assertEqual(e[0], f[-1])
        self.assertEqual(e[-1], f[0])

        conn.close()
