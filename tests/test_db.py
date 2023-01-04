import time
import os
from enum import Enum
from unittest import TestCase
from typing import Optional

from aleph_core.connections.db.rds import RDSConnection
from aleph_core import Model, DataSet


NOW = int(time.time() * 1000)
KEY = "test.key"


class TestEnum(str, Enum):
    option_1 = "A"
    option_2 = "B"


class TestModel(Model):
    __key__ = KEY

    a: Optional[int] = None
    b: Optional[str] = None
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


class SQLiteConnection(RDSConnection):
    FILE = "test.db"
    url = f"sqlite:///{FILE}"
    models = [TestModel]

    def on_read_error(self, error):
        error.raise_exception()

    def on_write_error(self, error):
        error.raise_exception()


class RDSGenericTestCase(TestCase):
    conn = SQLiteConnection

    def setUp(self):
        self.delete_file(self.conn.FILE)

    def tearDown(self):
        self.delete_file(self.conn.FILE)

    @staticmethod
    def delete_file(file):
        if os.path.isfile(file):
            os.remove(file)

    def test_read_write(self):
        conn = self.conn()
        conn.open()

        # Write
        records = TestModel.samples()
        conn.write(KEY, DataSet(records))

        # Read
        a = conn.read(KEY, limit=1)
        self.assertIsNotNone(a)
        self.assertEqual(len(a), 1)
        self.assertTrue("deleted_" not in a[0])
        self.assertTrue("_id" not in a[0])

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

        g = conn.read(KEY, filter={"b": "zu"})
        self.assertEqual(len(g), 1)
        g = conn.read(KEY, filter={"b": "zz"})
        self.assertEqual(len(g), 0)
        g = conn.read(KEY, filter={"b": ["zu", "rr", "zz"]})
        self.assertEqual(len(g), 2)
        g = conn.read(KEY, filter={"a": 5})
        self.assertEqual(len(g), 1)
        g = conn.read(KEY, filter={"a": 12})
        self.assertEqual(len(g), 0)
        # TODO

        conn.close()

    def test_run_sql(self):

        with self.conn() as conn:
            records = TestModel.samples()
            conn.write(KEY, records)

            r = conn.run_sql_query("SELECT * FROM testmodel")
            self.assertIsNotNone(r)
            self.assertEqual(len(r), 6)

            r = conn.run_sql_query("DELETE FROM testmodel")
            self.assertIsNone(r)

            r = conn.run_sql_query("SELECT * FROM testmodel")
            self.assertIsNotNone(r)
            self.assertEqual(len(r), 0)

        conn = self.conn()
        with conn.get_session() as session:
            model = TestModel.to_table_model()
            new_instance = model(a=1, b="hello", c=TestEnum.option_1, id_=None)
            session.add(new_instance)

            self.assertIsNone(new_instance.id_)
            session.flush()
            self.assertIsNotNone(new_instance.id_)
            session.commit()

    def test_deleted(self):
        conn = self.conn()
        conn.open()
        id_ = "item0"

        conn.safe_write(KEY, TestModel(a=8, b="x", c=TestEnum.option_2, id_=id_))
        self.assertEqual(len(conn.read(KEY)), 1)

        conn.safe_write(KEY, {"id_": id_, "deleted_": True})
        self.assertEqual(len(conn.read(KEY)), 0)

        conn.safe_write(KEY, {"id_": id_, "deleted_": False})
        r = conn.read(KEY)
        self.assertEqual(len(r), 1)
        self.assertTrue("deleted_" not in r[0])
        self.assertTrue("_id" not in r[0])

        conn.delete(KEY, id_)
        self.assertEqual(len(conn.read(KEY)), 0)

        conn.close()

    def test_update(self):
        conn = self.conn()
        conn.open()
        r = TestModel(b="x").dict()

        conn.write(KEY, [r])
        self.assertEqual(len(conn.read(KEY)), 1)
        self.assertEqual(conn.read(KEY)[0]["b"], "x")

        r["b"] = "w"
        conn.write(KEY, [r])
        self.assertEqual(len(conn.read(KEY)), 1)
        self.assertEqual(conn.read(KEY)[0]["b"], "w")

        conn.close()
