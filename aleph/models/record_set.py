from typing import List, Optional, Type
from aleph.models.model import Model

Value = str | float | int | bool | None
Record = dict[str, Value]


class RecordSet:
    def __init__(
        self,
        records: Optional[List[Record]] = None,
        model: Optional[Type[Model]] = None,
    ):
        self.model = model
        self._records: dict[Any, Record] = {}

        if records is not None:
            self.update(records)

    @property
    def records(self):
        return [record for record in self._records.values()]

    @records.setter
    def records(self, records: list[Record]):
        self._records = {}
        self.update(records)

    def update(self, records: Record | list[Record], sort=True):
        """ """
        if not isinstance(records, list):
            records = [records]

        for record in records:
            if self.model is None and isinstance(record, Model):
                self.model = type(record)

            if not isinstance(record, dict):
                record = dict(record)

            record = record.copy()

            if self.model is not None:
                record = self.model(**record).dict()
            else:
                if "t" not in record:
                    record["t"] = now()
                if "id_" not in record:
                    record["id_"] = generate_id()

            record_id = record.get("id_", record.get("t"))
            self._records.update({record_id: record})

        if sort:
            items = self._records.items()
            sorted_items = sorted(items, key=lambda item: item[1].get("t"))
            self._records = {key: value for key, value in sorted_items}

    def get(self, id_, default=None) -> Optional[Record]:
        """
        Get the record that matches the id_
        """
        for record in self._records.values():
            if id_ == record.get("id_"):
                return record
        return default

    def __getitem__(self, item) -> Record:
        return self.records[item]

    def __setitem__(self, item, value):
        self.records[item] = value

    def __iter__(self) -> Record:
        for r in self._records:
            yield self._records[r]

    def __len__(self):
        return len(self._records)

    def __repr__(self):
        model_str = self.model.__name__ if self.model is not None else "None"
        return f"DataSet<{model_str}>({len(self)})"

    def __str__(self):
        records_str = "\n".join([str(record) for record in self._records.values()])
        if len(records_str):
            records_str = ":\n" + records_str

        return repr(self) + records_str
