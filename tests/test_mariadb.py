from aleph_core.connections.db.rds import RDSConnection

from utils.docker import MariaDBContainer
from test_db import TestModel, RDSGenericTestCase
import time


NOW = time.time()
KEY = "test.key"


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


class MariaDBTestCase(RDSGenericTestCase):
    conn = MariaDBConnection
    container = MariaDBContainer()

    @classmethod
    def setUpClass(cls):
        pass#cls.container.run()

    @classmethod
    def tearDownClass(cls):
        pass#cls.container.stop()

    def setUp(self):
        conn = self.conn()
        conn.open()
        conn.drop_all()
        pass

    def tearDown(self):
        conn = self.conn()
        conn.open()
        conn.drop_all()
        pass
