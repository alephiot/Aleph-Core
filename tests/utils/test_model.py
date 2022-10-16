from unittest import TestCase
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.model import Model


class TestModel(Model):
    a: str
    b: int
    c: float
    d: bool


class ReportByExceptionTestCase(TestCase):

    def test_validate(self):
        self.assertRaises(
            Exceptions.InvalidModel,
            lambda: TestModel.validate({"a": "hello"}),
        )

        TestModel.validate({"a": "hello", "b": 1, "c": 2.5, "d": True})

