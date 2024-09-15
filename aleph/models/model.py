from typing import Optional, Type
from pydantic import BaseModel, Field
from uuid import uuid4
from time import time


def generate_id() -> str:
    """
    Returns a new uuid4 string
    """
    return str(uuid4())


def current_timestamp() -> int:
    """
    Returns the current timestamp in milliseconds
    """
    return int(time() * 1000)


class Model(BaseModel):
    id_: Optional[str] = Field(default_factory=generate_id)
    t: Optional[int] = Field(default_factory=current_timestamp)

    __optional__: Optional[Type[BaseModel]] = None

    class Config:
        use_enum_values = True

    @classmethod
    def to_all_optionals_model(cls) -> Type[BaseModel]:
        """
        Creates a new model with the same fields, but all fields are optional.
        """
        if cls.__optional__ is None:
            cls.__optional__ = type(f"{cls.__name__}Optional", (cls,), {})
            for field in cls.__optional__.__fields__:
                cls.__optional__.__fields__[field].required = False
        return cls.__optional__
