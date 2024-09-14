from typing import Optional
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
