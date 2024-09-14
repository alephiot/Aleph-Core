from aleph_core.connections.db.mongodb import MongoDBConnection


class MongoDBTimeSeriesConnection(MongoDBConnection):

    def read(self, key, **kwargs):
        pass

    def write(self, key, data):
        pass


