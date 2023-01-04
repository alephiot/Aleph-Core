import os
from unittest import TestCase
from utils.docker import RedisContainer

from aleph_core.utils.local_storage import LocalStorage


class LocalStorageTestCase(TestCase):
    redis_container = RedisContainer()

    @classmethod
    def setUpClass(cls):
        cls.redis_container.run()

    @classmethod
    def tearDownClass(cls):
        cls.redis_container.stop()

    @staticmethod
    def delete_file(file):
        if os.path.isfile(file):
            os.remove(file)

    def generic_test(self, local_storage: LocalStorage):
        local_storage.load()

        key = "key"
        value = "value"
        local_storage.set(key, value)
        self.assertEqual(local_storage.get(key), value)

        value = {"a": 1, "b": True, "c": "hello", "d": None}
        local_storage.set(key, value)
        self.assertEqual(local_storage.get(key), value)

        value = {"a": {"b": 2}}
        local_storage.set(key, value)
        self.assertEqual(local_storage.get(key), value)

    def test_local_storage(self):
        self.generic_test(LocalStorage())

    def test_file_local_storage(self):
        from aleph_core.utils.local_storage import FileLocalStorage

        file = "file.dat"
        self.delete_file(file)
        self.generic_test(FileLocalStorage(file))
        self.delete_file(file)

    def test_json_local_storage(self):
        from aleph_core.utils.local_storage import JsonLocalStorage

        file = "file.json"
        self.delete_file(file)
        self.generic_test(JsonLocalStorage(file))
        self.delete_file(file)

    def test_sqlite_local_storage(self):
        from aleph_core.utils.local_storage import SqliteDictLocalStorage

        file = "file.db"
        self.delete_file(file)
        self.generic_test(SqliteDictLocalStorage(file))
        self.delete_file(file)

    def test_redis_local_storage(self):
        from aleph_core.utils.local_storage import RedisLocalStorage
        self.generic_test(RedisLocalStorage())
