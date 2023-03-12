import json
import random

from enum import Enum
from typing import Type, Callable

from aleph_core.utils.data import Model
from aleph_core.utils.typing import Record
from aleph_core.utils.time import now


class RandomGenerator:
    _letters = "abcdefghijklmnopqrstuvwxyz"

    def __init__(self, value_min: int = 0, value_max: int = 100, choices: list = None):
        self.min = value_min
        self.max = value_max
        self.choices = choices

    def random_bool(self):
        return random.random() > 0.5

    def random_int(self):
        return random.randint(self.min, self.max)

    def random_float(self):
        return self.min + random.random() * (self.max - self.min)

    def random_string(self):
        length = random.randint(self.min, self.max)
        return "".join(random.choice(self._letters) for i in range(length))

    def random_choice(self):
        return random.choice(self.choices)


class FixtureFactory:
    def __init__(self, fixtures: list[Record], time_delta: int = 10000):
        self.fixtures = fixtures
        self.current_index = int(0.9 * len(self.fixtures))
        t0 = now()

        t = t0
        for i in range(self.current_index, 0, -1):
            fixtures[i - 1]["t"] = t
            t = t - time_delta

        t = t0
        for i in range(self.current_index, len(self.fixtures)):
            fixtures[i]["t"] = t
            t = t + time_delta

    def all(self):
        return self.fixtures

    def past(self):
        return self.fixtures[0 : self.current_index + 1]

    def next(self):
        self.current_index += 1
        if self.current_index >= len(self.fixtures):
            self.current_index = 0

        item = self.fixtures[self.current_index]
        item["t"] = now()
        return item

    @classmethod
    def from_json(cls, file: str, time_delta: int = 10000) -> "FixtureFactory":
        fixtures = None
        with open(file) as json_fixtures:
            fixtures = json.load(json_fixtures)

        return FixtureFactory(fixtures, time_delta)

    @classmethod
    def from_model(
        cls,
        model: Type[Model],
        time_delta: int = 10000,
        count: int = 1000,
        generators: dict[str, Callable] = {},
    ) -> "FixtureFactory":

        fields = model.get_fields()
        fixtures = []

        for field, field_properties in fields.items():
            field_type = field_properties.type_

            if not field_properties.required and random.random() < 0.1:
                generators[field] = lambda: None
            elif field_type == str:
                generators[field] = RandomGenerator().random_string
            elif field_type == int:
                generators[field] = RandomGenerator().random_int
            elif field_type == float:
                generators[field] = RandomGenerator().random_float
            elif field_type == bool:
                generators[field] = RandomGenerator().random_bool
            elif Enum in field_type.__mro__:
                choices = list(field_type)
                generators[field] = RandomGenerator(choices=choices).random_choice
            else:
                raise TypeError(f"Cannot find a generator for field {field_properties}")

        for i in range(0, count):
            record = {field: generator() for field, generator in generators.items()}
            fixtures.append(model(**record).dict())

        return FixtureFactory(fixtures, time_delta)
