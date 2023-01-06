import sqlalchemy
import sqlmodel
import json

from aleph_core import Connection
from aleph_core import Exceptions
from aleph_core import Model


class RDSConnection(Connection):
    url: str = ""
    models: dict[str, Model] = {}

    def __init__(self, client_id=""):
        super().__init__(client_id)
        self.__tables__ = {}
        self.__engine__ = None

        if len(self.models) == 0:
            raise Exception("The Relational Database needs models to work properly")

        for key in self.models:
            self.__tables__[key] = self.models[key].to_table_model()

    def open(self):
        if self.__engine__ is not None:
            return
        self.__engine__ = sqlmodel.create_engine(self.url)
        sqlmodel.SQLModel.metadata.create_all(self.__engine__)

    def close(self):
        if self.__engine__ is not None:
            self.__engine__ = None

    def is_open(self):
        return self.__engine__ is not None

    def get_session(self):
        if self.__engine__ is None:
            self.open()
        return sqlmodel.Session(self.__engine__)

    def run_sql_query(self, query):
        with sqlmodel.Session(self.__engine__) as session:
            result = session.exec(sqlmodel.text(query))
            try:
                result = result.all()
            except sqlalchemy.exc.ResourceClosedError:
                result = None

            session.commit()
        return result

    def read(self, key, **kwargs):
        if key not in self.__tables__:
            return None

        if self.__engine__ is None:
            raise Exceptions.ConnectionNotOpen()

        table = self.__tables__.get(key)

        since = kwargs.get("since", None)
        until = kwargs.get("until", None)
        limit = kwargs.get("limit", None)
        offset = kwargs.get("offset", None)
        order = kwargs.get("order", "-t")
        where = kwargs.get("filter", None)

        result = []

        with sqlmodel.Session(self.__engine__) as session:
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
        if key not in self.__tables__:
            return None

        if self.__engine__ is None:
            raise Exceptions.ConnectionNotOpen()

        table = self.__tables__.get(key)

        with sqlmodel.Session(self.__engine__) as session:
            for record in data:
                instance = None
                if "id_" in record and record["id_"]:
                    instance = session.query(table).get(record["id_"])

                if instance is None:
                    instance = table(**record)
                else:
                    instance.update(**record)

                session.add(instance)
            session.commit()

    def delete(self, key, id_):
        # TODO
        self.write(key, [{"id_": id_, "deleted_": True}])

    def __filter_statement__(self, table, statement, where):
        if isinstance(where, str):
            where = json.loads(where)

        if not isinstance(where, dict):
            raise Exception("Filter needs to be a dict")

        and_conditions = self.__filter_to_conditions__(table, where)
        statement = statement.filter(sqlalchemy.and_(*and_conditions))
        return statement

    def __filter_to_conditions__(self, table, where: dict):
        # TODO: Decide on filter
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
