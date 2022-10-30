import pydantic
import sqlmodel

from typing import Optional, Dict
from uuid import uuid4

from aleph_core.utils.datetime_functions import now
from aleph_core.utils.exceptions import Exceptions


def generate_id():
    return str(uuid4())


class Model(pydantic.BaseModel):
    id_: Optional[str] = pydantic.Field(default_factory=generate_id, index=True)
    t: Optional[int] = pydantic.Field(default_factory=now, index=True)

    __key__: str = None
    __table__ = None  # TODO Type??

    @property
    def key(self):
        return self.__key__ if self.__key__ else None

    @key.setter
    def key(self, value: str):
        self.__key__ = value

    @classmethod
    def to_table_model(cls):
        if cls.__table__ is None:
            cls.__table__ = type(cls.__name__, (TableModel, cls), {}, table=True)
        return cls.__table__

    def to_dict(self):
        return self.dict(exclude_none=True, exclude_defaults=True)


    @classmethod
    def validate(cls, record_as_dict: Dict):
        """Receives a dict and checks if it matches the model, otherwise it throws an InvalidModel error"""
        try:
            cls.parse_obj(record_as_dict)
        except pydantic.ValidationError as validation_error:
            raise Exceptions.InvalidModel(str(validation_error))

    def update(self, **kwargs):
        for field in kwargs:
            setattr(self, field, kwargs[field])


class TableModel(sqlmodel.SQLModel):
    id_: Optional[str] = sqlmodel.Field(default_factory=generate_id, primary_key=True)
    deleted_: Optional[bool] = sqlmodel.Field(default=False)
    # TODO: Created?

    __table_args__ = {'extend_existing': True}
