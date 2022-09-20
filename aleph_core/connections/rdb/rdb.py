from aleph_core import Connection
from aleph_core import Exceptions
import sqlalchemy
import sqlmodel


class RDBConnection(Connection):
    __engine__ = None
    url = ""

    def __init__(self, client_id=""):
        super().__init__(client_id)
        if len(self.models) == 0:
            raise Exception("The Relational Database needs models to work properly")

    def open(self):
        if self.__engine__ is not None:
            return
        self.__engine__ = sqlmodel.create_engine(self.url)
        sqlmodel.SQLModel.metadata.create_all(self.__engine__)

    def close(self):
        if self.__engine__ is None:
            return
        self.__engine__ = None

    def is_open(self):
        return self.__engine__ is not None

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
        if key not in self.models:
            return None

        if self.__engine__ is None:
            raise Exceptions.ConnectionNotOpen()

        model = self.models.get(key)

        since = kwargs.get("since", None)
        until = kwargs.get("until", None)
        limit = kwargs.get("limit", None)
        offset = kwargs.get("offset", None)
        order = kwargs.get("order", None)
        where = kwargs.get("filter", None)

        result = []

        with sqlmodel.Session(self.__engine__) as session:
            statement = session.query(model)

            if since: statement = statement.filter(getattr(model, "t") >= since)
            if until: statement = statement.filter(getattr(model, "t") < until)
            if offset: statement = statement.offset(offset)
            if limit: statement = statement.limit(limit)

            if order:
                if order[0] == "-":
                    statement = statement.order_by(getattr(model, order[1:]).desc())
                else:
                    statement = statement.order_by(getattr(model, order).asc())

            if where:
                pass

            all_instances = statement.all()
            for instance in all_instances:
                result.append(instance.dict())

        return result

    def write(self, key, data):
        if key not in self.models:
            return None

        if self.__engine__ is None:
            raise Exceptions.ConnectionNotOpen()

        model = self.models.get(key)

        with sqlmodel.Session(self.__engine__) as session:
            for record in data:
                instance = session.query(model).get(record.get("id_"))
                if instance is None:
                    instance = model(**record)
                else:
                    instance.update(**record)

                session.add(instance)
            session.commit()

