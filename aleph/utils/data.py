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
    def get_fields(cls) -> dict[str, pydantic.fields.ModelField]:
        return cls.__fields__

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
        return cls_(**subrecord).dict(exclude_defaults=True, exclude_unset=True)
