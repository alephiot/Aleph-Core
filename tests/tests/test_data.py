from typing import Optional
from unittest import TestCase

from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.data import Model, DataSet


class TestModelA(Model):
    a: str
    b: int
    c: Optional[float] = None


class DataSetTestCase(TestCase):

    def test_data_set(self):
        data_set = DataSet(model=TestModelA)
        self.assertEqual(len(data_set.records), 0)

        record1 = TestModelA(id_="one", a="one", b=3).dict()
        record2 = TestModelA(id_="two", a="two", b=2).dict()
        record3 = TestModelA(id_="one", a="three", b=1, c=2).dict()
        data_set.update([record1, record2, record3])
        self.assertEqual(len(data_set.records), 2)

        self.assertEqual(len(list(data_set)), 2)

    def test_most_recent(self):
        record1 = TestModelA(a="1", b=2, c=None, t=1).dict()
        record2 = TestModelA(a="2", b=4, c=3, t=2).dict()
        record3 = TestModelA(a="3", b=3, c=None, t=3).dict()
        record4 = TestModelA(a="4", b=1, c=None, t=4).dict()
        data_set = DataSet(records=[record1, record2, record3, record4], model=TestModelA)
        
        most_recent = data_set.most_recent("b")
        self.assertEqual(most_recent, 1)

        most_recent = data_set.most_recent("c")
        self.assertEqual(most_recent, 3)
