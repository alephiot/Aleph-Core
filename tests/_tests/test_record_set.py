import pytest

from aleph_core import Model
from aleph_core import RecordSet
from aleph_core import Exceptions
from enum import Enum


class SomeModel(Model):
    str_: str
    int_: int
    float_: float


class Fixtures:
    records = [
        {"t": 2, "a": 1, "b": 2},
        {"t": 3, "a": 2, "b": 33},
        {"t": 1, "a": 4},
    ]

    more_records = [
        {"x": 1, "y": 2},
        {"x": 2, "y": -2},
        {"x": 3, "y": 0},
    ]

    records_with_model = [
        SomeModel(str_="hello", int_=2, float_=1.1),
        SomeModel(str_="bye", int_=3, float_=2.1),
    ]


def test_record_set_can_create_without_model():
    """Test we can create a record set without a model"""
    record_set = RecordSet(Fixtures.records)
    assert len(record_set) == 3
    assert record_set[0]["id_"] is not None
    assert record_set[1]["id_"] is not None
    assert record_set[2]["id_"] is not None


def test_record_set_can_update():
    """Test we can update the records in a record set"""
    records = Fixtures.records
    record_set = RecordSet()
    assert len(record_set) == 0

    record_set.update(records[0])
    assert len(record_set) == 1
    assert record_set[0]["a"] == 1

    record_set.update(records[1])
    assert len(record_set) == 2
    assert record_set[0]["a"] == 1
    assert record_set[1]["a"] == 2

    record_set.update(records[2])
    assert len(record_set) == 3
    assert record_set[0]["a"] == 4
    assert record_set[1]["a"] == 1
    assert record_set[2]["a"] == 2

    record_set.records = []
    assert len(record_set) == 0


def test_record_set_is_sorted():
    """Test that the record_set sorts the values on update"""
    record_set = RecordSet(Fixtures.records)
    assert record_set.model == None

    assert record_set[0]["a"] == 4
    assert record_set[1]["a"] == 1
    assert record_set[2]["a"] == 2

    assert "b" not in record_set[0]
    assert record_set[1]["b"] == 2
    assert record_set[2]["b"] == 33


def test_record_set_updates_id_and_timestamp():
    """Test that when updating a record set the records get a timestamp and id"""
    record_set = RecordSet(Fixtures.more_records)
    assert len(record_set) == 3

    assert record_set[0]["id_"] is not None
    assert record_set[1]["id_"] is not None
    assert record_set[2]["id_"] is not None

    assert record_set[0]["t"] > 0
    assert record_set[1]["t"] > 0
    assert record_set[2]["t"] > 0


def test_record_set_can_iterate():
    """Test we can iterate through the record set"""
    records = Fixtures.more_records
    record_set = RecordSet(records)
    assert isinstance(record_set.records, list)

    for record in record_set:
        assert isinstance(record, dict)

    for i in range(0, len(record_set)):
        assert record_set[i] == records[i]


def test_record_set_can_create_with_model():
    """Test we can create a record set with a model"""
    objs = Fixtures.records_with_model
    record_set = RecordSet(objs)

    assert len(record_set) == 2
    assert record_set.model == SomeModel
    assert isinstance(record_set.records, list)

    for record in record_set:
        assert isinstance(record, dict)
        obj = SomeModel(**record)
        assert isinstance(obj, SomeModel)

    record_set.update({"str_": "xxx", "int_": 1, "float_": 2.2})

    with pytest.raises(Exceptions.ModelValidationError):
        record_set.update({"str_": "xxx", "int_": 1})


def test_record_set_get_by_id_timestamp():
    """Test get_by_id and get_by_t methods"""
    record_set = RecordSet(Fixtures.records)

    r0 = record_set[0]
    r1 = record_set[1]

    assert record_set.get_by_id(r0["id_"]) == r0
    assert record_set.get_by_t(r0["t"]) == r0

    assert record_set.get_by_id(r0["id_"]) != r1
    assert record_set.get_by_t(r0["t"]) != r1
