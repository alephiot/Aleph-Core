import time
import os
from enum import Enum
from unittest import TestCase

from aleph_core.connections.rdb import RDBConnection
from aleph_core import Model


NOW = time.time()
FILE = "test.db"
KEY = "test.key"


class TestEnum(str, Enum):
    option_1 = "A"
    option_2 = "B"


class TestModel(Model):
    __key__ = KEY

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
    url = f"sqlite:///{FILE}"
    models = [TestModel]


class RDBTestCase(TestCase):

    @classmethod
    def setUp(cls):
        cls.delete_file(FILE)

    @classmethod
    def tearDownClass(cls):
        cls.delete_file(FILE)

    @staticmethod
    def delete_file(file):
        if os.path.isfile(file):
            os.remove(file)

    def test_read_write(self):
        conn = TestConnection()
        conn.open()

        # Write
        records = TestModel.samples()
        conn.write(KEY, records)

        # Read
        a = conn.read(KEY, limit=1)
        self.assertEqual(len(a), 1)

        b = conn.read(KEY, limit=1, offset=1)
        self.assertEqual(len(b), 1)
        self.assertNotEqual(a[0]["id_"], b[0]["id_"])

        c = conn.read(KEY, since=NOW - 50)
        self.assertEqual(len(c), 3)

        d = conn.read(KEY,  since=NOW - 50, until=NOW - 30)
        self.assertEqual(len(d), 1)

        e = conn.read(KEY, order="b")
        f = conn.read(KEY, order="-b")
        self.assertEqual(e[0], f[-1])
        self.assertEqual(e[-1], f[0])

        conn.close()

    def test_run_sql(self):

        with TestConnection() as conn:
            records = TestModel.samples()
            conn.write(KEY, records)

            r = conn.run_sql_query("SELECT * FROM testtable")
            self.assertIsNotNone(r)
            self.assertEqual(len(r), 6)

            r = conn.run_sql_query("DELETE FROM testtable")
            self.assertIsNone(r)

            r = conn.run_sql_query("SELECT * FROM testtable")
            self.assertIsNotNone(r)
            self.assertEqual(len(r), 0)

        conn = TestConnection()
        with conn.get_session() as session:
            model = TestModel.to_table_model()
            new_instance = model(a=1, b="hello", c=TestEnum.option_1)
            session.add(new_instance)

            self.assertIsNone(new_instance.id_)
            session.flush()
            self.assertIsNotNone(new_instance.id_)
            session.commit()
