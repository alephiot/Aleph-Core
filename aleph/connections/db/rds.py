import sqlalchemy
import sqlmodel
import json

from sqlalchemy.orm import sessionmaker, scoped_session, query
from typing import Optional

from aleph_core.connections.connection import Connection
from aleph_core.utils.exceptions import Exceptions
from aleph_core.utils.data import TableModel
from aleph_core.utils.data import Model


class RDSConnection(Connection):
    url: str
    models: dict[str, Model]

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.__tables__ = {}  # map key: table
        self.__engine__ = None

        for key, model in self.models.items():
            self.__tables__[key] = model.to_sqlalchemy_table()

    def open(self):
        if self.__engine__ is None:
            self.__engine__ = sqlalchemy.create_engine(self.url)
            sqlmodel.SQLModel.metadata.create_all(self.__engine__)

    def close(self):
        if self.__engine__ is not None:
            self.__engine__ = None

    def is_open(self):
        return self.__engine__ is not None

    def get_table(self, key: str) -> Optional[TableModel]:
        return self.__tables__.get(key)

    def get_scoped_session(self):
        if self.__engine__ is None:
            self.open()
        Session = scoped_session(sessionmaker(self.__engine__))
        return Session()

    def query(self, key: str) -> query.Query:
        """Query the database"""
        table = self.get_table(key)
        if table is None:
            raise Exceptions.InvalidKey(f"Cannot match '{key}' with a model")
        return self.get_scoped_session().query(table)

    def read(self, key, **kwargs):
        table = self.get_table(key)
        if table is None:
            raise Exceptions.InvalidKey(f"Cannot match '{key}' with a model")
        if not self.is_open():
            raise Exceptions.ConnectionNotOpen()

        since = kwargs.get("since", None)
        until = kwargs.get("until", None)
        limit = kwargs.get("limit", None)
        offset = kwargs.get("offset", None)
        order = kwargs.get("order", "-t")
        where = kwargs.get("filter", None)

        result = []

        with self.get_scoped_session() as session:
            statement = session.query(table)
            statement = statement.where(getattr(table, "deleted_") != True)

            if since:
                statement = statement.filter(getattr(table, "t") >= since)
            if until:
                statement = statement.filter(getattr(table, "t") < until)
            if where:
                statement = self.__filter_statement__(table, statement, where)
            if order:
                if order[0] == "-":
                    statement = statement.order_by(getattr(table, order[1:]).desc())
                else:
                    statement = statement.order_by(getattr(table, order).asc())

            if offset:
                statement = statement.offset(offset)
            if limit:
                statement = statement.limit(limit)

            records = statement.all()
            for record in records:
                record = record.dict()
                record.pop("deleted_", None)
                result.append(record)

        return result

    def write(self, key, data):
        table = self.get_table(key)
        if key is None:
            raise Exceptions.InvalidKey(f"Cannot match '{key}' with a model")
        if not self.is_open():
            raise Exceptions.ConnectionNotOpen()

        with self.get_scoped_session() as session:
            for record in data:
                instance = None
                if "id_" in record and record["id_"]:
                    instance = session.query(table).get(record["id_"])
                if instance is None:
                    instance = table(**record)
                else:
                    for field in record:
                        setattr(instance, field, record[field])

                session.add(instance)
            session.commit()

    def __filter_statement__(self, table, statement, where):
        if isinstance(where, str):
            where = json.loads(where)
        if not isinstance(where, dict):
            raise Exceptions.InvalidArgs(f"Filter '{where}' is not a dict")

        and_conditions = self.__filter_to_conditions__(table, where)
        statement = statement.filter(sqlalchemy.and_(*and_conditions))
        return statement

    def __filter_to_conditions__(self, table, where: dict):
        conditions = []
        for field in where:
            condition = where[field]
            field = getattr(table, field)

            if isinstance(condition, list):
                conditions.append(field.in_(condition))
            elif isinstance(condition, float) or isinstance(condition, int):
                conditions.append(field == condition)
            elif condition.startswith("=="):
                conditions.append(field == condition[2:])
            elif condition.startswith("!="):
                conditions.append(field != condition[2:])
            elif condition.startswith(">="):
                conditions.append(field >= condition[2:])
            elif condition.startswith("<="):
                conditions.append(field <= condition[2:])
            elif condition.startswith(">"):
                conditions.append(field > condition[1:])
            elif condition.startswith("<"):
                conditions.append(field < condition[1:])
            else:
                conditions.append(field == condition)

        return conditions
