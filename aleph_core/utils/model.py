import pydantic
import sqlmodel

from typing import Optional, Dict
from uuid import uuid4

from aleph_core.utils.datetime_functions import now
from aleph_core.utils.exceptions import Exceptions


def generate_id():
    return str(uuid4())


class Model(pydantic.BaseModel):
    id_: Optional[str] = None
    t: Optional[int] = pydantic.Field(default_factory=now, index=True)

    # Associated key
    __key__ = None

    @property
    def key(self):
        return self.__key__ if self.__key__ else None

    @key.setter
    def key(self, value: str):
        self.__key__ = value

    @classmethod
    def to_table_model(cls):
        return type("TestTable", (TableModel, cls), {}, table=True)

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
    id_: Optional[str] = sqlmodel.Field(default=None, primary_key=True)
    delete_: Optional[bool] = sqlmodel.Field(default=False)

    __table_args__ = {'extend_existing': True}
