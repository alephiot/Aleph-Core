from aleph_core import Model
from enum import Enum
import time

NOW = time.time()


class TestEnum(str, Enum):
    option_1 = "A"
    option_2 = "B"


class TestModelA(Model, table=True):
    a: int
    b: str
    c: TestEnum = TestEnum.option_2

    @staticmethod
    def samples(count=None):
        samples = [
            TestModelA(t=NOW - 100, a=1, b="hi", c=TestEnum.option_1),
            TestModelA(t=NOW - 80,  a=2, b="by"),
            TestModelA(t=NOW - 60,  a=3, b="ax"),
            TestModelA(t=NOW - 40,  a=4, b="dw"),
            TestModelA(t=NOW - 20,  a=5, b="rr"),
            TestModelA(t=NOW - 0,   a=6, b="zu"),
        ]

        if count and count < len(samples):
            samples = samples[:count]

        return [x.dict() for x in samples]
