import json
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from urllib import parse

from aleph_core import Connection


class MongoDBConnection(Connection):
    server = "localhost"
    port = 27017
    username = None
    password = None
    database = "main"

    client: pymongo.MongoClient = None

    def open(self):
        url = "mongodb://"
        if self.username is not None:
            url += parse.quote(self.username)
        if self.password is not None:
            url += ":" + parse.quote(self.password)
        if self.username is not None:
            url += "@"

        url += f"{self.server}:{self.port}"
        self.client = pymongo.MongoClient(url, serverSelectionTimeoutMS=10)

    def close(self):
        if self.client is not None:
            self.client.close()

    def is_open(self):
        if self.client is None:
            return False
        try:
            self.client.server_info()
        except ServerSelectionTimeoutError:
            return False

        return True

    def read(self, key, **kwargs):
        since = kwargs.get("since", None)
        until = kwargs.get("until", None)
        limit = kwargs.get("limit", 0)
        offset = kwargs.get("offset", 0)
        order = kwargs.get("order", None)
        where = kwargs.get("filter", None)

        filters = [{"deleted_": {"$ne": True}}]

        if since and until:
            filters.append({"t": {"$gte": since, "$lte": until}})
        elif since and not until:
            filters.append({"t": {"$gte": since}})
        elif not since and until:
            filters.append({"t": {"$lt": until}})

        if where:
            filters.append(self.__deparse_filter__(where))

        projection = {"_id": False, "deleted_": False}

        collection = self.get_collection(key)
        result = collection.find({"$and": filters}, projection=projection, limit=limit, skip=offset)

        if order:
            if order[0] == "-":
                result = result.sort([(order[1:], pymongo.ASCENDING)])
            else:
                result = result.sort([(order, pymongo.DESCENDING)])

        return list(result)

    def write(self, key, data):
        collection = self.get_collection(key)
        for record in data:
            if "id_" in record and record["id_"]:
                collection.update_one({"id_": record["id_"]}, {
                    "$set": record,
                    # "$setOnInsert": {"deleted_": False},
                }, upsert=True)
            else:
                collection.insert_one(record)

    def delete(self, key, id_):
        # TODO
        self.write(key, [{"id_": id_, "deleted_": True}])

    def get_collection(self, key) -> pymongo.collection.Collection:
        if key in self.models:
            key = self.models[key].__class__.__name__.lower()
        else:
            key = key.replace("/", ".").replace(".", "_")

        return self.client[self.database][key]

    def __deparse_filter__(self, where):
        if isinstance(where, str):
            where = json.loads(where)

        if not isinstance(where, dict):
            raise Exception("Filter needs to be a dict")

        return {"$and": self.__filter_to_conditions__(where)}

    def __filter_to_conditions__(self, where):
        # TODO
        conditions = []
        for field in where:
            condition = where[field]

            if isinstance(condition, list):
                conditions.append({field: {"$in": condition}})
            elif isinstance(condition, float) or isinstance(condition, int):
                conditions.append({field: condition})
            elif condition.startswith("=="):
                conditions.append({field: condition[2:]})
            elif condition.startswith("!="):
                conditions.append({field: {"$ne": condition[2:]}})
            elif condition.startswith(">="):
                conditions.append({field: {"$gte": condition[2:]}})
            elif condition.startswith("<="):
                conditions.append({field: {"$lte": condition[2:]}})
            elif condition.startswith(">"):
                conditions.append({field: {"$gt": condition[1:]}})
            elif condition.startswith("<"):
                conditions.append({field: {"$lt": condition[1:]}})
            else:
                conditions.append({field: condition})

        return conditions
