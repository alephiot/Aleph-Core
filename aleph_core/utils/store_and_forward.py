from typing import Callable, Coroutine

from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.exceptions import Error
from aleph_core.utils.data import RecordSet


class StoreAndForward:
    """
    Class that executes the write function and, if it fails, stores the data on a
    local storage.
    """

    LOCAL_STORAGE_KEY = "STORE_AND_FORWARD"

    def __init__(
        self, name: str, write: Callable | Coroutine, local_storage: LocalStorage = None
    ):
        self.name = name
        self.local_storage = local_storage or LocalStorage()
        self.write = write

    @property
    def local_storage_key(self):
        return f"{self.LOCAL_STORAGE_KEY}_{self.name}"

    def flush_all(self) -> list[Error]:
        """
        Tries to call the write function for all keys in the buffer.
        Returns a list of the errors raised for each key.
        """
        try:
            buffer = self.local_storage.get(self.local_storage_key, {})
        except Exception as e:
            return [Error(e)]

        errors = []
        for key in buffer:
            data = RecordSet(buffer.get(key))
            try:
                self.write(key, data)
                buffer[key] = []
                self.local_storage.set(self.local_storage_key, buffer)
            except Exception as e:
                errors.append(Error(e, key=key, data=data))

        return errors

    def add_and_flush(self, key: str, data: RecordSet):
        """
        Add data to buffer and try to write.
        If it fails, it raises an exeception.
        """
        buffer = self.local_storage.get(self.local_storage_key, {})
        if key not in buffer:
            buffer[key] = []
        buffer[key] = buffer[key] + list(data)
        self.local_storage.set(self.local_storage_key, buffer)

        self.write(key, RecordSet(buffer.get(key)))
        buffer[key] = []
        self.local_storage.set(self.local_storage_key, buffer)

    async def flush_all_async(self) -> list[Error]:
        try:
            buffer = self.local_storage.get(self.local_storage_key, {})
        except Exception as e:
            return [Error(e)]

        errors = []
        for key in buffer:
            data = RecordSet(buffer.get(key))
            try:
                await self.write(key, data)
                buffer[key] = []
                self.local_storage.set(self.local_storage_key, buffer)
            except Exception as e:
                errors.append(Error(e, key=key, data=data))

        return errors

    async def add_and_flush_async(self, key: str, data: RecordSet):
        buffer = self.local_storage.get(self.local_storage_key, {})
        if key not in buffer:
            buffer[key] = []
        buffer[key] = buffer[key] + list(data)
        self.local_storage.set(self.local_storage_key, buffer)

        await self.write(key, RecordSet(buffer.get(key)))
        buffer[key] = []
        self.local_storage.set(self.local_storage_key, buffer)
