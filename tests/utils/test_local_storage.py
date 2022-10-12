from unittest import TestCase
from aleph_core.utils.local_storage import *


class LocalStorageTestCase(TestCase):

    def generic_test(self, local_storage: LocalStorage):
        local_storage.load()
        local_storage.set("A", "value")
        self.assertEqual(local_storage.get("A"), "value")

    def test_local_storage(self):
        self.generic_test(LocalStorage())

    def test_file_local_storage(self):
        self.generic_test(FileLocalStorage())

    def test_json_local_storage(self):
        self.generic_test()

    def test__local_storage(self):
        pass

    def test_redis_local_storage(self):
        pass
