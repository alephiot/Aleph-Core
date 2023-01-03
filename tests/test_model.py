from typing import Optional
from unittest import TestCase

from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.data import Model


class TestModelA(Model):
    a: str
    b: int
    c: float
    d: bool


class TestModelB(Model):
    x: str = "hello"
    y: int


class TestModelC(Model):
    u: int
    v: Optional[int]
    w: int = 1
    z: Optional[int] = 2


class ModelTestCase(TestCase):

    def test_validate(self):
        self.assertRaises(
            Exceptions.InvalidModel,
            lambda: TestModelA.validate_record({"a": "hello"}),
        )

        record = TestModelA.validate_record({"a": "hello", "b": 1, "c": 2.5, "d": True})
        self.assertTrue("id_" not in record)
        self.assertTrue("t" not in record)

        record = TestModelA.validate_record({"a": "hello"}, exclude_unset=True)
        self.assertTrue("id_" not in record)



