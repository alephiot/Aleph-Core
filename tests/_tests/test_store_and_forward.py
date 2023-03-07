import pytest

from aleph_core.utils.store_and_forward import StoreAndForward
from aleph_core.utils.local_storage import LocalStorage


class StoreAndForwardExtended:
    written = {}

    def __init__(self):
        self.local_storage = LocalStorage()
        self.store_and_forward = StoreAndForward("", self.local_storage)

    def write(self, key, data):
        self.written[key] = self.written.get(key, []) + data

    def get_buffer(self):
        key = self.store_and_forward.local_storage_key
        return self.local_storage.get(key)
