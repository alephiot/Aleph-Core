import time

from aleph_core.connections.db.mongodb import MongoDBConnection

from utils.docker import MongoDBContainer
from tests.test_db import TestModel
from tests.test_db import DBGenericTestCase


NOW = time.time()
KEY = "test.key"


class TestConnection(MongoDBConnection):
    database = "main"
    models = [TestModel]

    def drop_all(self):
        self.client.drop_database(self.database)

    def on_error(self, error):
        error.raise_exception()


class MongoDBTestCase(DBGenericTestCase):
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

