"""

"""

from sqlmodel import SQLModel, Field
from typing import Optional
from uuid import uuid4

from aleph_core.utils.datetime_functions import now


class Model(SQLModel):
    id_: Optional[str] = Field(default_factory=lambda: str(uuid4()), primary_key=True, index=True)
    t: Optional[int] = Field(default_factory=now, index=True)

    # Associated key
    __key__ = None

    @property
    def key(self):
        return self.__key__ if self.__key__ else self.__tablename__

    @key.setter
    def key(self, value: str):
        self.__key__ = value

    @classmethod
    def validate(cls, record_as_dict):
        cls.parse_obj(record_as_dict)

    def update(self, **kwargs):
        for field in kwargs:
            setattr(self, field, kwargs[field])
