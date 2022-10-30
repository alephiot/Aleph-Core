from utils.docker import MongoDBContainer
from test_db import TestModel
import time
from unittest import TestCase

from aleph_core.connections.db.rds import RDSConnection


NOW = time.time()
KEY = "test.key"


class MariaDBConnection(RDSConnection):
    user = "user"
    password = "1234"
    db = "main"
    url = f"mysql+pymysql://{user}:{password}@localhost/{db}?charset=utf8mb4"
    models = [TestModel]


# TODO
