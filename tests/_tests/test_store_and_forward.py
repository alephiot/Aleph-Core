import pytest

from aleph_core.utils.data import RecordSet
from aleph_core.utils.store_and_forward import StoreAndForward
from aleph_core.utils.local_storage import LocalStorage


class StoreAndForwardTestHelper:
    key = "key"
    written = {}
    written_tries = 0

    def __init__(self):
        self.local_storage = LocalStorage()
        self.store_and_forward = StoreAndForward("", self.write, self.local_storage)

    def write(self, key, data):
        self.written_tries += 1
        if self.written_tries < 3:
            raise RuntimeError("Could not write")
        self.written[key] = self.written.get(key, []) + list(data)

    def get_buffer(self):
        key = self.store_and_forward.local_storage_key
        return self.local_storage.get(key, {}).get(self.key)

    def get_written(self):
        return self.written.get(self.key, [])

    def get_record(self):
        records = [
            {"a": 1, "b": 2},
            {"a": 2, "b": 22},
            {"a": 3, "b": 13},
            {"a": 4, "b": 32},
        ]

        return records[self.written_tries]


def test_store_and_forward_add_and_flush():
    helper = StoreAndForwardTestHelper()

    # Should fail
    record = helper.get_record()
    with pytest.raises(RuntimeError):
        helper.store_and_forward.add_and_flush(helper.key, RecordSet(record))
    assert len(helper.get_buffer()) == 1
    assert len(helper.get_written()) == 0
    assert helper.get_buffer()[0] == record

    # Should fail
    record = helper.get_record()
    with pytest.raises(RuntimeError):
        helper.store_and_forward.add_and_flush(helper.key, RecordSet(record))
    assert len(helper.get_buffer()) == 2
    assert len(helper.get_written()) == 0

    # Should succeed
    record = helper.get_record()
    helper.store_and_forward.add_and_flush(helper.key, RecordSet(record))
    assert len(helper.get_buffer()) == 0
    assert len(helper.get_written()) == 3


def test_store_and_forward_flush_all():
    helper = StoreAndForwardTestHelper()

    record = helper.get_record()
    with pytest.raises(RuntimeError):
        helper.store_and_forward.add_and_flush(helper.key, RecordSet(record))

    errors = helper.store_and_forward.flush_all()
    assert len(errors) == 1

    errors = helper.store_and_forward.flush_all()
    assert len(errors) == 0
