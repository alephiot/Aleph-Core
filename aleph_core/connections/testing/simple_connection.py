from aleph_core.connections.connection import Connection
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.data import RecordSet


class SimpleConnection(Connection):
    opened = False
    written = {}

    def open(self):
        self.opened = True

    def close(self):
        self.opened = False

    def is_open(self) -> bool:
        return self.opened

    def read(self, key: str = "", **kwargs):
        if not self.opened:
            raise Exceptions.ConnectionNotOpen()

        return RecordSet(self.written.get(key, []))

    def write(self, key: str = "", data: RecordSet = None):
        if not self.opened:
            raise Exceptions.ConnectionNotOpen()

        self.written[key] = self.written.get(key, []) + list(data)
