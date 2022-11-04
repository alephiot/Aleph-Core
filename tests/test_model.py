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
    a: TestModelA
    b: TestModelB


class ModelTestCase(TestCase):

    def test_validate(self):
        self.assertRaises(
            Exceptions.InvalidModel,
            lambda: TestModelA.validate({"a": "hello"}),
        )

        TestModelA.validate({"a": "hello", "b": 1, "c": 2.5, "d": True})

