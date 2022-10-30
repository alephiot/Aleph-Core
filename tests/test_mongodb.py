from aleph_core.connections.db.mongodb import MongoDBConnection

from utils.docker import MongoDBContainer
from test_db import TestModel
from test_db import RDSGenericTestCase
import time


NOW = time.time()
KEY = "test.key"


class TestConnection(MongoDBConnection):
    database = "main"
    models = [TestModel]

    def drop_all(self):
        self.get_collection(self.database).drop()

    def on_read_error(self, error):
        error.raise_exception()

    def on_write_error(self, error):
        error.raise_exception()


class MongoDBTestCase(RDSGenericTestCase):
    conn = TestConnection
    container = MongoDBContainer()

    @classmethod
    def setUpClass(cls):
        cls.container.run()

    @classmethod
    def tearDownClass(cls):
        cls.container.stop()

    def setUp(self):
        conn = self.conn()
        conn.open()
        conn.drop_all()

    def tearDown(self):
        conn = self.conn()
        conn.open()
        conn.drop_all()

    def test_run_sql(self):
        pass
