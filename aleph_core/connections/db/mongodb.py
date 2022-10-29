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

        filters = [{"deleted_": False}]

        if since and until:
            filters.append({"t": {"$gte": since, "$lte": until}})
        elif since and not until:
            filters.append({"t": {"$gte": since}})
        elif not since and until:
            filters.append({"t": {"$lte": until}})

        if where:
            # TODO
            pass

        collection = self.get_collection(key)
        result = collection.find({"$and": filters}, limit=limit, skip=offset)

        if order:
            if order[0] == "-":
                result = result.sort([(order[1:], pymongo.ASCENDING)])
            else:
                result = result.sort([(order, pymongo.DESCENDING)])

        return list(result)

    def write(self, key, data):
        collection = self.get_collection(key)
        for record in data:
            if "id_" in record:
                collection.update_one({"id_": record["id_"]}, {
                    "$set": record,
                    "$setOnInsert": {"deleted_": False},
                }, upsert=True)
            else:
                collection.insert_one(record)

    def get_collection(self, key) -> pymongo.collection.Collection:
        if key in self.models:
            key = self.models[key].__class__.__name__.lower()
        else:
            key = key.replace("/", ".").replace(".", "_")

        return self.client[self.database][key]

    def deparse_filter(self, filter_):
        """
        Gets a filter
        """
        if isinstance(filter_, str):
            filter_ = json.loads(filter_)

        filters = []






