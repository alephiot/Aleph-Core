import os

from utils.docker import RedisContainer
from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.local_storage import FileLocalStorage
from aleph_core.utils.local_storage import JsonLocalStorage
from aleph_core.utils.local_storage import SqliteDictLocalStorage
from aleph_core.utils.local_storage import RedisLocalStorage


def _delete_file(file):
    if os.path.isfile(file):
        os.remove(file)


def _start_redis():
    RedisContainer().run()


def _test(local_storage: LocalStorage):
    local_storage.load()

    key = "key"
    value = "value"
    local_storage.set(key, value)
    assert local_storage.get(key) == value

    value = {"a": 1, "b": True, "c": "hello", "d": None}
    local_storage.set(key, value)
    assert local_storage.get(key) == value

    value = {"a": {"b": 2}}
    local_storage.set(key, value)
    assert local_storage.get(key) == value


def test_local_storage():
    _test(LocalStorage())


def test_file_local_storage():
    file = "file.dat"
    _delete_file(file)
    _test(FileLocalStorage(file))
    _delete_file(file)


def test_json_local_storage():
    file = "file.json"
    _delete_file(file)
    _test(JsonLocalStorage(file))
    _delete_file(file)


def test_sqlite_local_storage():
    file = "file.db"
    _delete_file(file)
    _test(SqliteDictLocalStorage(file))
    _delete_file(file)


def test_redis_local_storage():
    _start_redis()
    _test(RedisLocalStorage())
