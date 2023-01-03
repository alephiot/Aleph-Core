import pydantic
import sqlmodel

from typing import Optional, Type
from uuid import uuid4

from aleph_core.utils.time import now
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.typing import Record, RecordList, Value


def generate_id():
    return str(uuid4())


class TableModel(sqlmodel.SQLModel):
    id_: Optional[str] = sqlmodel.Field(default_factory=generate_id, primary_key=True)
    t: Optional[int] = sqlmodel.Field(default_factory=now, index=True)
    deleted_: Optional[bool] = sqlmodel.Field(default=False)

    __table_args__ = {'extend_existing': True}


class Model(pydantic.BaseModel):
    id_: Optional[str] = pydantic.Field(default_factory=generate_id, index=True)
    t: Optional[int] = pydantic.Field(default_factory=now, index=True)

    __key__: Optional[str] = None
    __table__: Optional[Type[TableModel]] = None
    __optional__: Optional[Type[pydantic.BaseModel]] = None

    def to_dict(self):
        return self.dict(exclude_none=True, exclude_defaults=True)

    @property
    def key(self):
        return self.__key__ if self.__key__ else None

    @key.setter
    def key(self, value: str):
        self.__key__ = value

    @classmethod
    def to_table_model(cls) -> Type[TableModel]:
        if cls.__table__ is None:
            cls.__table__ = type(cls.__name__, (TableModel, cls), {}, table=True)
        return cls.__table__ 
    
    @classmethod
    def to_all_optionals_model(cls) -> Type[pydantic.BaseModel]:
        if cls.__optional__ is None:
            cls.__optional__ = type(f'{cls.__name__}Optional', (cls,), {})
            for field in cls.__optional__.__fields__:
                cls.__optional__.__fields__[field].required = False
        return cls.__optional__

    @classmethod
    def validate_record(cls, record: Record, exclude_unset=False) -> Record:
        """
        Receives a dict and checks if it matches the model
        Otherwise it throws an InvalidModel error
        """
        try:
            cls_ = cls if not exclude_unset else cls.to_all_optionals_model()
            return cls_(**record).dict(exclude_none=True, exclude_defaults=True, exclude_unset=True)
        except pydantic.ValidationError as validation_error:
            raise Exceptions.InvalidModel(str(validation_error))


class DataSet:

    def __init__(self, records: Optional[RecordList] = None, model: Optional[Type[Model]] = None):
        self.model = model
        self._records = {}

        if records is not None:
            self.update(records)

    @property
    def records(self):
        return list(self._records.values())  # TODO sort?

    @records.setter
    def records(self, records: RecordList):
        self._records = {}
        self.update(records)

    def update(self, records: RecordList):
        for record in records:
            if self.model is not None:
                record = self.model(**record).dict()
            else:
                if "t" not in record:
                    record["t"] = now()
                if "id_" not in record:
                    record["id_"] = generate_id()

            self._records.update({self.__get_record_id__(record): record})

    def __iter__(self):
        for r in self._records:  # TODO sorted?
            yield self._records[r]

    def __len__(self):
        return len(self._records)

    def most_recent(self, field, timestamp_threshold_in_seconds: Optional[int] = None) -> Value:
        last_record = max(self._records.values(), key=lambda item: (item.get("t", 0) if item.get(field, None) is not None else 0))
        if timestamp_threshold_in_seconds is not None:
            if now() - last_record.get("t", None) > timestamp_threshold_in_seconds:
                return None
        
        return last_record.get(field)

    @classmethod
    def __get_record_id__(cls, record: Record):
        id_ = record.get("id_", None)
        if id_ is None:
            id_ = record.get("t", None)
        return id_
