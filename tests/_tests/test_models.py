import pytest

from aleph_core import Model
from aleph_core import Exceptions
from enum import Enum


class SomeEnum(str, Enum):
    A = "Value A"
    B = "Value B"
    C = "Value C"


class SomeModel(Model):
    str_: str
    int_: int
    float_: float
    bool_: bool
    enum_: SomeEnum
    default_none: str = None
    default_one: int = 1


class AnotherModel(Model):
    __key__ = "my.model"
    a: str
    b: float


def test_model_can_create():
    """Test the model can be created"""
    obj = SomeModel(str_="text", int_=9, float_=8.0, bool_=False, enum_=SomeEnum.A)
    assert obj.str_ == "text"
    assert obj.int_ == 9
    assert obj.float_ == 8.0
    assert obj.bool_ == False
    assert obj.default_none is None
    assert obj.enum_ == SomeEnum.A
    assert obj.default_one == 1
    assert obj.id_ is not None
    assert obj.t is not None


def test_model_add_id_on_create():
    """Test the an unique id is assigned on creation"""
    objA = AnotherModel(a="a", b=2)
    objB = AnotherModel(a="a", b=2)
    assert objA.id_ != objB.id_


def test_model_can_parse_values():
    """Test the model can parse data types on creation"""
    obj = SomeModel(str_=3, int_="9", float_="8.0", bool_=0, enum_="Value A")
    assert obj.str_ == "3"
    assert obj.int_ == 9
    assert obj.float_ == 8.0
    assert obj.bool_ == False
    assert obj.default_none is None
    assert obj.enum_ == SomeEnum.A
    assert obj.default_one == 1


def test_model_creation_raises_validation_error():
    """Test the model raises a Validation Error when created with"""

    with pytest.raises(Exceptions.ModelValidationError):
        SomeModel()

    with pytest.raises(Exceptions.ModelValidationError):
        SomeModel(str_="text")


def test_model_can_be_converted_to_dict():
    """Test the model can be converted to dict correctly"""
    obj = SomeModel(str_="text", int_=9, float_=8.0, bool_=False, enum_=SomeEnum.A)
    obj_as_dict = obj.dict()
    assert obj.dict() == dict(obj)

    assert obj_as_dict["str_"] == "text"
    assert obj_as_dict["int_"] == 9
    assert obj_as_dict["float_"] == 8.0
    assert obj_as_dict["bool_"] == False
    assert obj_as_dict["default_none"] is None
    assert obj_as_dict["default_one"] == 1


def test_model_can_set_key():
    """Test we can set and read the key"""
    obj = AnotherModel(a="", b=2)
    assert obj.key == "my.model"
    assert AnotherModel.key == "my.model"


def test_model_can_validate_record():
    """Test a record can be validated with a model"""
    AnotherModel.validate_record({"a": "hello", "b": 2})
    AnotherModel.validate_subrecord({"a": "no b"})

    with pytest.raises(Exceptions.ModelValidationError):
        AnotherModel.validate({"a": "no b"})

    with pytest.raises(Exceptions.ModelValidationError):
        AnotherModel.validate_subrecord({"b": "unparseable"})
