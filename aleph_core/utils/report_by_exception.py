import json
from aleph_core.utils.typing import Record
from aleph_core.utils.data import RecordSet
from aleph_core.utils.local_storage import LocalStorage


class ReportByException:
    LOCAL_STORAGE_KEY = "REPORT_BY_EXCEPTION"
    MAX_RECORDS_SIZE = 100

    def __init__(self, local_storage=None):
        local_storage = local_storage or LocalStorage()

    def next(self, key: str, record_set: RecordSet) -> RecordSet:
        local_storage_key = f"{self.LOCAL_STORAGE_KEY}_{key}"
        hashes: dict = self.local_storage.get(local_storage_key, {})
        filtered: dict[key, Record] = {}

        for record in record_set:
            id_ = record.get("id_", "None")
            prev_hash = hashes.get(id_)
            new_hash = hash(json.dumps({**record, "t": None}))

            if prev_hash is None or prev_hash != new_hash:
                filtered[id_] = record

            hashes[id_] = new_hash

        self.local_storage.set(local_storage_key, hashes)
        return RecordSet(filtered.values(), record_set.model)
