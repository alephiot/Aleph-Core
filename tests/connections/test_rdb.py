import time
import os

from aleph_core.connections.rdb import RDBConnection
from unittest import TestCase
from tests.context.models import TestModelA


NOW = time.time()


class DBTestConnection(RDBConnection):
    url = "sqlite:///test.db"
    models = {"test.key": TestModelA}


class RDBConnectionTest(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        if os.path.isfile("test.db"):
            os.remove("test.db")

    def test_read_write(self):
        conn = DBTestConnection()
        conn.open()

        # Write
        conn.write("test.key", TestModelA.samples())

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


if __name__ == "__main__":
    RDBConnectionTest().run()
