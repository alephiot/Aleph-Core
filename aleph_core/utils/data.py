import pydantic
import json
import os

from pathlib import Path

from typing import Optional, Type, Any
from uuid import uuid4

from aleph_core.utils.time import now
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.typing import Record, Value


def generate_id():
    return str(uuid4())


class Model(pydantic.BaseModel):
    id_: Optional[str] = pydantic.Field(default_factory=generate_id, index=True)
    t: Optional[int] = pydantic.Field(default_factory=now, index=True)

    __key__: Optional[str] = None
    __optional__: Optional[Type[pydantic.BaseModel]] = None

    class Config:
        use_enum_values = True

    @classmethod
    @property
    def key(cls):
        return cls.__key__ if cls.__key__ else None

    @classmethod
    def to_all_optionals_model(cls) -> Type[pydantic.BaseModel]:
        if cls.__optional__ is None:
            cls.__optional__ = type(f"{cls.__name__}Optional", (cls,), {})
            for field in cls.__optional__.__fields__:
                cls.__optional__.__fields__[field].required = False
        return cls.__optional__

    @classmethod
    def validate_record(cls, record: Record):
        """
        Receives a dict and checks if it matches the model
        Otherwise it throws an InvalidModel error
        """
        cls(**record)

    @classmethod
    def validate_subrecord(cls, subrecord: Record):
        cls_ = cls.to_all_optionals_model()
        cls_(**subrecord)


class RecordSet:
    def __init__(
        self,
        records: Optional[list[Record]] = None,
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
        if not isinstance(records, list):
            records = [records]

        for record in records:
            if self.model is None and isinstance(record, Model):
                self.model = type(record)

            if not isinstance(record, dict):
                record = dict(record)

            if self.model is not None:
                record = self.model(**record).dict()
            else:
                if "t" not in record:
                    record["t"] = now()
                if "id_" not in record:
                    record["id_"] = generate_id()

            self._records.update({self._get_record_id(record): record})

        if sort:
            items = self._records.items()
            sorted_items = sorted(items, key=lambda item: item[1].get("t"))
            self._records = {key: value for key, value in sorted_items}

    def get_by_id(self, id_, default=None):
        for r in self._records:
            if id_ == self._records[r].get("id_"):
                return self._records[r]
        return default

    def get_by_t(self, t, default=None):
        for r in self._records:
            if t == self._records[r].get("t"):
                return self._records[r]
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

    def __str__(self):
        records_str = "\n".join([str(record) for record in self._records.values()])
        if len(records_str):
            records_str = ":\n" + records_str

        return repr(self) + records_str

    def __repr__(self):
        if self.model is None:
            model_str = "None"
        else:
            model_str = self.model.__name__

        return f"DataSet<{model_str}>({len(self)})"

    def _get_record_id(self, record: Record):
        id_ = record.get("id_", None)
        if id_ is None:
            id_ = record.get("t", None)
        return id_


class FixtureFactory:
    file: str = "fixtures.json"
    dt: int = 10000  # 10 seconds

    def __init__(self):
        _path = str(Path(__file__).resolve().parent)
        _file = os.path.join(_path, self.file)
        with open(_file) as fixtures:
            self.fixtures = json.load(fixtures)

        self.t0 = now()
        self.i0 = int(0.9 * len(self.fixtures))
        self.i = self.i0

        t = self.t0
        for i in range(self.i0, len(self.fixtures)):
            self.fixtures[i]["t"] = t
            t = t + self.dt

    def historic(self):
        return self.fixtures[0 : self.i0 + 1]

    def next(self):
        self.i += 1
        item = self.fixtures[self.i]
        item["t"] = now()
        return item
