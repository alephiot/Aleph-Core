import pydantic
import sqlmodel
import json
import os

from uuid import uuid4
from pathlib import Path
from typing import Optional, Type, Any
from sqlalchemy import Column, BigInteger

from aleph_core.utils.typing import Record
from aleph_core.utils.time import now


def generate_id():
    return str(uuid4())


class TableModel(sqlmodel.SQLModel):
    id_: Optional[str] = sqlmodel.Field(default_factory=generate_id, primary_key=True)
    t: Optional[int] = sqlmodel.Field(
        default_factory=now, index=True, sa_column=Column(BigInteger())
    )
    deleted_: Optional[bool] = sqlmodel.Field(default=False)

    __table_args__ = {"extend_existing": True}


class Model(pydantic.BaseModel):
    id_: Optional[str] = pydantic.Field(default_factory=generate_id, index=True)
    t: Optional[int] = pydantic.Field(default_factory=now, index=True)

    __optional__: Optional[Type[pydantic.BaseModel]] = None
    __table__: Optional[Type[TableModel]] = None

    class Config:
        use_enum_values = True

    @classmethod
    def to_sqlalchemy_table(cls) -> Type[TableModel]:
        if cls.__table__ is None:
            cls.__table__ = type(cls.__name__, (TableModel, cls), {}, table=True)
        return cls.__table__ 
    
    @classmethod
    def to_all_optionals_model(cls) -> Type[pydantic.BaseModel]:
        if cls.__optional__ is None:
            cls.__optional__ = type(f"{cls.__name__}Optional", (cls,), {})
            for field in cls.__optional__.__fields__:
                cls.__optional__.__fields__[field].required = False
        return cls.__optional__

    @classmethod
    def validate_record(cls, record: Record) -> Record:
        """
        Checks if record fits the model
        """
        return cls(**record).dict(exclude_defaults=True, exclude_unset=True)

    @classmethod
    def validate_subrecord(cls, subrecord: Record) -> Record:
        """
        Checks if subrecord fits the model (ignoring fields that are not present)
        """
        cls_ = cls.to_all_optionals_model()
        print(cls_(**subrecord))
        return cls_(**subrecord).dict(exclude_defaults=True, exclude_unset=True)


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

            record = record.copy()

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
