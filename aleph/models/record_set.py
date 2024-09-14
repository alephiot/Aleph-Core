from typing import List, Optional, Type
from aleph.models.model import Model
from aleph.utils.time import current_timestamp

Value = str | float | int | bool | None
Record = dict[str, Value]


class RecordSet:
    def __init__(self, model: Optional[Type[Model]] = None):
        self.model = model
        self.records: List[Record] = []

    def project(self) -> List[Record]:
        """
        Project all records with the same id_ to the latest state. This will combine the
        fields of all records with the same id_ into a single record.
        """
        records = sorted(self.records, key=lambda r: r.get("t"))
        projection = {}
        for record in records:
            id_ = record.get("id_")
            if id_ not in projection:
                projection[id_] = {}
            projection[id_].update(record)
        return list(projection.values())

    def update(self, records: Record | List[Record] | Model | List[Model]) -> "RecordSet":
        """
        Returns a new record set with the records updated
        """
        now = current_timestamp()
        model = self.model.to_all_optionals_model() if self.model else None
        new_records = {(r["t"], r["id_"]): r for r in self.records}

        if not isinstance(records, list):
            records = [records]

        for record in records:
            if model and not isinstance(record, model):
                record = model(**record).dict()

            assert isinstance(record, dict)
            record = record.copy()
            record["t"] = record.get("t", now)
            record["id_"] = record.get("id_")

            record_id = (record["t"], record["id_"])
            new_records[record_id] = record

        record_set = RecordSet(self.model)
        record_set.records = list(sorted(new_records.values(), key=lambda r: r.get("t")))
        return record_set

    def __getitem__(self, item) -> Record:
        return self.records[item]

    def __setitem__(self, item, value):
        self.records[item] = value

    def __iter__(self) -> Record:
        for i in range(len(self.records)):
            yield self.records[i]

    def __len__(self) -> int:
        return len(self.records)

    def __repr__(self) -> str:
        model_name = self.model.__name__ if self.model else ""
        return f"DataSet<{model_name}>({len(self)})"

    def __str__(self) -> str:
        return self.__repr__()
