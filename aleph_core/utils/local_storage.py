import pickle
import json


class LocalStorage:
    """
    In-memory storage
    """

    def __init__(self):
        self.data = {}
        self.load()

    def load(self):
        pass

    def get(self, key, null_value=None):
        if key not in self.data:
            return null_value
        return self.data[key]

    def set(self, key, value):
        self.data[key] = value
        return value


class FileLocalStorage(LocalStorage):
    """
    Local Storage that uses pickle and saves data to a local file
    """

    def __init__(self, file):
        self.file = file
        super().__init__()

    def load(self):
        import os
        if os.path.isfile(self.file):
            with open(self.file, 'rb') as f:
                self.data = pickle.load(f)

    def get(self, key, null_value=None):
        return super().get(key, null_value)

    def set(self, key, value):
        super().set(key, value)
        with open(self.file, "wb+") as f:
            pickle.dump(self.data, f)
        return value


class JsonLocalStorage(LocalStorage):
    """
    Local Storage that saves data to a JSON file
    """
    def __init__(self, file):
        self.file = file
        super().__init__()

    def load(self):
        import os
        if os.path.isfile(self.file):
            with open(self.file, 'r') as f:
                self.data = json.load(f)

    def get(self, key, null_value=None):
        return super().get(key, null_value)

    def set(self, key, value):
        super().set(key, value)
        with open(self.file, "w+") as f:
            json.dump(self.data, f)
        return value


class SqliteDictLocalStorage(LocalStorage):
    """
    Local Storage that uses pickle and sqlite
    """

    def __init__(self, file):
        self.file = file
        self.sqlitedict = None
        super().__init__()

    def load(self):
        from sqlitedict import SqliteDict
        self.sqlitedict = SqliteDict(self.file, autocommit=True)

    def get(self, key, null_value=None):
        if key not in self.sqlitedict: return null_value
        return self.sqlitedict[key]

    def set(self, key, value):
        self.sqlitedict[key] = value
        return value


# ===================================================================================
# Redis Storage
# ===================================================================================
class RedisLocalStorage(LocalStorage):
    """
    Local Storage that uses redis
    """

    def __init__(self, prefix=""):
        self.red = None
        self.prefix = prefix
        super().__init__()

    def load(self):
        import redis
        self.red = redis.Redis()  # host='localhost', port=6379, db=0

    def get(self, key, null_value=None):
        if not self.red.exists(self.prefix + key):
            return null_value
        else:
            return pickle.loads(self.red.get(self.prefix + key))

    def set(self, key, value):
        self.red.set(self.prefix + key, pickle.dumps(value))
        return value
