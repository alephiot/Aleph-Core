import os
import logging

from unittest import TestCase
from sqlmodel import create_engine, SQLModel, Field
from pydantic.error_wrappers import ValidationError
from aleph_core.utils.model import Model


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MyModel(Model):
    field1: int
    field2: str


class Test(TestCase):

    def test_create(self):
        my_model = MyModel(field1=1, field2="hello")
        logger.info(my_model)

    def test_validate(self):
        MyModel.validate({'field1': 2, 'field2': "bye"})
        self.assertRaises(
            ValidationError,
            lambda: MyModel.validate({'field1': 0})
        )

    def test_engine(self):
        my_model = MyModel(field1=1, field2="hello")

        db_file = "test.db"
        if os.path.isfile(db_file):
            os.remove(db_file)

        engine = create_engine(f"sqlite:///{db_file}", echo=True)

        SQLModel.metadata.create_all(engine)

