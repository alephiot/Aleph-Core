from inspect import iscoroutinefunction as is_coroutine
from typing import Optional
from abc import ABC
import logging

from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.wait_one_step import WaitOneStep
from aleph_core.utils.report_by_exception import ReportByException
from aleph_core.utils.store_and_forward import StoreAndForward
from aleph_core.utils.async_helper import AsyncHelper
from aleph_core.utils.exceptions import Exceptions, Error
from aleph_core.utils.data import RecordSet

logger = logging.getLogger(__name__)


class Connection(ABC):
    time_step = 10
    local_storage = LocalStorage()
    async_helper = AsyncHelper()
    store_and_forward = False
    report_by_exception = False
    multi_thread = False

    def __init__(self, client_id=""):
        self.client_id = client_id
        self.__subscribed_keys__ = set()
        self.__report_by_exception__ = ReportByException(self.local_storage)
        self.__store_and_forward__ = StoreAndForward(self.write, self.local_storage)

    # ----------------------------------------------------------------------------------
    # Main methods
    # ----------------------------------------------------------------------------------

    def open(self):
        """Opens the connection. Should raise an error if it fails to do so"""
        return

    def close(self):
        """Closes the connection"""
        return

    def read(self, key: str = "", **kwargs) -> Optional[RecordSet]:
        """Read the connection with the given key"""
        return None

    def write(self, key: str = "", data: Optional[RecordSet] = None):
        """Write on the connection with the given key"""
        return

    def is_open(self) -> bool:
        """Returns True if the connection is open, False otherwise"""
        return True

    def on_new_data(self, key: str, data: RecordSet):
        """Callback function for when a new message arrives"""
        return

    def on_error(self, error: Error):
        """Callback function for when a safe function fails"""
        return

    def on_connect(self):
        """Callback function for when the connection is open"""
        return

    def on_disconnect(self):
        """Callback function for when the connection is closed"""
        return

    # ----------------------------------------------------------------------------------
    # Dunder methods
    # ----------------------------------------------------------------------------------

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ----------------------------------------------------------------------------------
    # Open
    # ----------------------------------------------------------------------------------

    async def _open_async(self, time_step):
        wait_one_step = WaitOneStep(time_step)
        previous_state = False

        while True:
            await wait_one_step.async_wait()
            current_state = self.is_open()

            if not current_state:
                try:
                    if is_coroutine(self.open):
                        await self.open()
                    else:
                        self.open()
                    current_state = True

                except Exception as e:
                    current_state = False
                    self.on_error(Error(e, client_id=self.client_id))

            if current_state and not previous_state:
                self.on_connect()
                if self.store_and_forward:
                    if is_coroutine(self.write):
                        errors = await self.__store_and_forward__.flush_all_async()
                    else:
                        errors = self.__store_and_forward__.flush_all()

                    for error in errors:
                        error.args["client_id"] = self.client_id
                        self.on_error(error)

            elif not current_state and previous_state:
                self.on_disconnect()

            previous_state = current_state

    def open_async(self, time_step=None):
        """
        Executes the open function without blocking the main thread
        Calls is_connected on a loop and tries to reconnect if disconnected
        """
        time_step = time_step or self.time_step
        self.async_helper.run_coroutine_threadsafe(self._open_async(time_step))

    # ----------------------------------------------------------------------------------
    # Read
    # ----------------------------------------------------------------------------------

    def __subscribe_async(self, key, time_step):
        wait_one_step = WaitOneStep(time_step)
        while True:
            wait_one_step.wait()
            if key not in self.__subscribed_keys__:
                break
            data = self.safe_read(key)
            if data is None or len(data) == 0:
                continue
            self.on_new_data(key, data)

    async def _subscribe_async(self, key, time_step):
        wait_one_step = WaitOneStep(time_step)
        while True:
            await wait_one_step.async_wait()
            if key not in self.__subscribed_keys__:
                break

            data = await self._safe_read(key)
            if data is None or len(data) == 0:
                continue
            self.on_new_data(key, data)

    def subscribe_async(self, key: str = "", time_step: int = None):
        """Executes the safe_read function without blocking the main thread"""
        if key in self.__subscribed_keys__:
            return

        time_step = time_step or self.time_step
        self.__subscribed_keys__.add(key)

        if self.multi_thread and not is_coroutine(self.safe_read):
            self.async_helper.run_on_thread(self.__subscribe_async, key, time_step)
        else:
            coroutine = self._subscribe_async(key, time_step)
            self.async_helper.run_coroutine_threadsafe(coroutine)

    def unsubscribe(self, key: str = ""):
        """Reverse the effect of subscribe_async"""
        self.__subscribed_keys__.discard(key)

    async def _safe_read(self, key: str = "", **kwargs) -> Optional[RecordSet]:
        try:
            if not self.is_open():
                if is_coroutine(self.open):
                    await self.open()
                else:
                    self.open()

            if is_coroutine(self.read):
                data = await self.read(key, **kwargs)
            else:
                data = self.read(key, **kwargs)

            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            if not isinstance(data, RecordSet):
                data = RecordSet(data)

            return data

        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, args=kwargs))
            return None

    def safe_read(self, key: str = "", **kwargs) -> Optional[RecordSet]:
        """
        Tries to open the connection and read.
        If an exception is raised, the on_error function i
        s called.
        """
        try:
            if not self.is_open():
                self.open()

            data = self.read(key, **kwargs)

            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            if not isinstance(data, RecordSet):
                data = RecordSet(data)

            return data

        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, args=kwargs))
            return None

    # ----------------------------------------------------------------------------------
    # Write
    # ----------------------------------------------------------------------------------

    async def _safe_write(self, key: str = "", data: RecordSet = None):
        try:
            if data is None:
                return
            if not isinstance(data, RecordSet):
                data = RecordSet(data)
            if self.report_by_exception:
                data = self.__report_by_exception__.next(key, data)
            if len(data) == 0:
                return
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

        try:
            if not self.is_open():
                if is_coroutine(self.open):
                    await self.open()
                else:
                    self.open()

            if self.store_and_forward:
                if is_coroutine(self.write):
                    await self.__store_and_forward__.add_and_flush_async(key, data)
                else:
                    self.__store_and_forward__.add_and_flush(key, data)

            else:
                self.write(key, data)
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

    def write_async(self, key: str = "", data: Optional[RecordSet] = None):
        """Executes the safe_write function without blocking the main thread"""
        if self.multi_thread and not is_coroutine(self.safe_write):
            self.async_helper.run_on_thread(self.safe_write, key, data)
        else:
            self.async_helper.run_coroutine_threadsafe(self._safe_write(key, data))

    def safe_write(self, key: str = "", data: Optional[RecordSet] = None):
        """
        Tries to open the connection and write.
        If report_by_exception is enabled, only writes the difference with last call.
        If store_and_forward is enabled, the buffer will be flushed on success.
        If an exception is raised, the on_error function is called.
        """
        self.async_helper.run(self._safe_write(key, data))


class AsyncConnection(Connection):
    multi_thread = False

    async def open(self):
        return super().open()

    async def close(self):
        return super().close()

    async def read(self, key: str = "", **kwargs) -> Optional[RecordSet]:
        return super().read(key, **kwargs)

    async def write(self, key: str = "", data: Optional[RecordSet] = None):
        return super().write(key, data)

    async def safe_read(self, key: str = "", **kwargs) -> Optional[RecordSet]:
        return await self._safe_read(key, **kwargs)

    async def safe_write(self, key: str = "", data: Optional[RecordSet] = None):
        return await self._safe_write(key, data)
