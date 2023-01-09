from aleph_core.connections.db.rds import RDSConnection

from utils.docker import MariaDBContainer
from tests.test_db import TestModel, RDSGenericTestCase


class MariaDBConnection(RDSConnection):
    user = "user"
    password = "1234"
    db = "main"
    url = f"mysql+pymysql://{user}:{password}@localhost/{db}?charset=utf8mb4"
    models = [TestModel]

    def drop_all(self):
        for table in self.__tables__:
            table = self.__tables__[table].__table__
            self.run_sql_query(f"DROP TABLE {table};")

    def on_read_error(self, error):
        error.raise_exception()

    def on_write_error(self, error):
        error.raise_exception()


class MariaDBTestCase(RDSGenericTestCase):
    conn = MariaDBConnection
    container = MariaDBContainer()

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
