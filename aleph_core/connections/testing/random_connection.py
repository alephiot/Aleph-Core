import random

from aleph_core.connections.connection import Connection
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.data import RecordSet


class RandomConnection(Connection):
    opened = False

    def seed(self, seed):
        random.seed(seed)

    def open(self):
        self.opened = True

    def close(self):
        self.opened = False

    def is_open(self) -> bool:
        return self.opened

    def read(self, key: str = "", **kwargs):
        letters = "abcdefghijklmnopqrstuvwxyz"
        if not self.opened:
            raise Exceptions.ConnectionNotOpen()

        return RecordSet(
            {
                "str_": "".join(random.choice(letters) for i in range(10)),
                "float_": random.random(),
                "int_": random.randint(0, 100),
                "bool_": random.random() > 0.5,
            }
        )
