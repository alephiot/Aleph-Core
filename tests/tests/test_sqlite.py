import os

from aleph_core.connections.db.rds import RDSConnection
from tests.test_db import RDSGenericTestCase, TestModel


class SQLiteConnection(RDSConnection):
    FILE = "test.db"

    url = f"sqlite:///{FILE}"
    models = [TestModel]

    def on_error(self, error):
        error.raise_exception()


class SQLiteTestCase(RDSGenericTestCase):
    conn = SQLiteConnection

    @staticmethod
    def delete_file(file):
        if os.path.isfile(file):
            os.remove(file)
    
    def setUp(self):
        self.delete_file(self.conn.FILE)

    def tearDown(self):
        self.delete_file(self.conn.FILE)
