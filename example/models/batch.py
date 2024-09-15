from enum import Enum
from datetime import date
from pydantic import root_validator

from aleph.models import Model


class Recipe(Enum):
    SIMPLE = 0
    DOUBLE = 1


class Batch(Model):
    batch_number: int
    date: date
    assignee_id: str
    recipe: Recipe

    @root_validator
    def validate_date(cls, values):
        values["id_"] = f"b{values['batch_number']}-{values['date']}"
        return values

    @property
    def assignee(self):
        return self.get_related("assignee_id")
