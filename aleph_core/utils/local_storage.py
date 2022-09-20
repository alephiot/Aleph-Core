"""
Different ways to store key:value pairs on the local storage
"""

import pickle
import json


# ===================================================================================
# Parent Class
# ===================================================================================
class LocalStorage:
    # Predefined keys
    SNF_BUFFER = "_snf_buffer"
    LAST_TIME_READ = "_last_time"
    PAST_VALUES = "_past_values"

    def __init__(self):
        self.data = {}
        self.load()

    def load(self):
        pass

    def get(self, key, null_value=None):
        if key not in self.data: return null_value
        return self.data[key]

    def set(self, key, value):
        self.data[key] = value
        return value


# ===================================================================================
# File Storage
# ===================================================================================
class FileLocalStorage(LocalStorage):

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


# ===================================================================================
# Sqlite Dict Storage (Uses pickle and sqlite)
# ===================================================================================
class SqliteDictLocalStorage(LocalStorage):

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
