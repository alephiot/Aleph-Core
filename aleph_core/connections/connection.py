from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.wait_one_step import WaitOneStep
from aleph_core.utils.exceptions import Exceptions, Error
from aleph_core.utils.time import now
from aleph_core.utils.data import generate_id, DataSet
from aleph_core.utils.report_by_exception import ReportByExceptionHelper
from aleph_core.utils.typing import Record

from typing import List, Dict, Any, Union
from abc import ABC
import threading
import asyncio
import logging


logger = logging.getLogger(__name__)


class Connection(ABC):
    time_step = 10
    local_storage = LocalStorage()
    report_by_exception = False
    store_and_forward = False
    multi_thread = False

    def __init__(self, client_id=""):
        self.client_id = client_id
        self.__async_loop__ = None
        self.__subscribed_keys__ = set()
        self.__report_by_exception_helpers__: dict[str, ReportByExceptionHelper] = {}

    def open(self):
        """
        Opens the connection. If it fails, it must raise an Exception
        """
        return

    def close(self):
        """
        Closes the connection.
        """
        return

    def read(self, key: str, **kwargs) -> DataSet:
        """
        Must return a list (data, a list of records) or a dict (single record)
        """
        return []

    def write(self, key: str, data: DataSet):
        """
        Returns None.
        """
        return

    def is_open(self):
        """
        Returns a boolean (True if open, False if not open)
        """
        return True

    def on_new_data(self, key: str, data: DataSet):
        """
        Callback function for when a new message arrives. Data can be a dict or a list of
        dict. This function is used by the read_async, subscribe async and subscribe
        methods.
        """
        return

    def on_error(self, error: Error):
        """
        Callback function for when a safe function fails
        """
        return


    def on_connect(self):
        """
        Callback function for when the connection is open
        """
        return

    def on_disconnect(self):
        """
        Callback function for when the connection is closed
        """
        return

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __run_on_async_thread__(self, coroutine):
        if self.__async_loop__ is None:
            logging.info("Starting async thread")
            self.__async_loop__ = asyncio.new_event_loop()
            threading.Thread(target=self.__async_loop__.run_forever, daemon=True).start()

        asyncio.run_coroutine_threadsafe(coroutine, self.__async_loop__)

    async def __open_async__(self, time_step):
        wait_one_step = WaitOneStep(time_step)

        previous_state = False
        while True:
            current_state = self.is_open()
            if not current_state:
                try:
                    self.open()
                    current_state = True
                except:
                    current_state = False

            try:
                if current_state and not previous_state:
                    logger.info("Connected")
                    self.on_connect()
                    await self._flush_buffer()
                if not current_state and previous_state:
                    logger.info("Disconnected")
                    self.on_disconnect()
            except Exception as e:
                # TODO Handle?
                print(Error(e).message_and_traceback)

            previous_state = current_state
            await wait_one_step.async_wait()

    def __subscribe__(self, key, time_step):
        wait_one_step = WaitOneStep(time_step)
        while True:
            try:
                wait_one_step.wait()
                if key not in self.__subscribed_keys__:
                    break
                data = self.safe_read(key)
                if data is None or len(data) == 0:
                    continue
                self.on_new_data(key, data)
            except Exception:
                self.unsubscribe(key)
                raise

    async def __subscribe_async__(self, key, time_step):
        wait_one_step = WaitOneStep(time_step)
        while True:
            try:
                await wait_one_step.async_wait()
                if key not in self.__subscribed_keys__:
                    break
                data = await self._safe_read(key)
                if data is None or len(data) == 0:
                    continue
                self.on_new_data(key, data)
            except Exception:
                self.unsubscribe(key)
                raise

    async def _open(self):
        self.open()

    async def _read(self, key, **kwargs):
        return self.read(key, **kwargs)

    async def _write(self, key, data):
        self.write(key, data)

    def open_async(self, time_step=None):
        """
        Executes the open function without blocking the main thread
        Calls is_connected on a loop and tries to reconnect if disconnected
        """
        if time_step is None:
            time_step = self.time_step
        self.__run_on_async_thread__(self.__open_async__(time_step))

    def subscribe_async(self, key, time_step=None):
        """Executes the read function on a loop, without blocking the main thread"""
        if time_step is None:
            time_step = self.time_step
        if key in self.__subscribed_keys__:
            return
        self.__subscribed_keys__.add(key)

        if self.multi_thread:
            logger.info("Creating subscribe_async thread for key %s", key)
            threading.Thread(target=self.__subscribe__, args=(key, time_step,), daemon=True).start()
        else:
            logger.info("Adding subscribe_async coroutine for key %s", key)
            self.__run_on_async_thread__(self.__subscribe_async__(key, time_step))

    def write_async(self, key: str, data: DataSet):
        """
        Executes the safe_write function without blocking the main thread
        If the connection does not allow multithreading, we use the async thread
        """
        if self.multi_thread:
            threading.Thread(target=self.safe_write, args=(key, data,), daemon=True).start()
        else:
            self.__run_on_async_thread__(self._safe_write(key, data))

    def unsubscribe(self, key: str):
        self.__subscribed_keys__.discard(key)

    def safe_read(self, key: str, **kwargs) -> DataSet | None:
        try:
            if not self.is_open():
                self.open()
            data = self.read(key, **kwargs)
            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            return data

        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, args=kwargs))
            return None

    async def _safe_read(self, key: str, **kwargs):
        try:
            if not self.is_open():
                self.open()
            data = await self._read(key, **kwargs)
            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            return data

        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, args=kwargs))
            return None

    def safe_write(self, key: str, data: DataSet):
        try:
            # TODO: Report by exception
            if len(data) == 0:
                return
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

        try:
            if not self.is_open():
                self.open()
            self.flush_buffer(key=key, data=data)
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

    async def _safe_write(self, key: str, data: DataSet):
        try:
            # TODO: Report by exception
            if len(data) == 0:
                return
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

        try:
            if not self.is_open():
                self.open()
            await self._flush_buffer(key=key, data=data)
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

    def flush_buffer(self, key=None, data=None):
        if key is not None and data is not None and not self.store_and_forward:
            self.write(key, data)
            return

        buffer = self.local_storage.get(self.__class__.__name__, {})
        if key is None:
            for key_ in buffer:
                self.flush_buffer(key=key_)
            return

        # Add new data to buffer and save
        if key not in buffer:
            buffer[key] = []
        if isinstance(data, list):
            buffer[key] = buffer[key] + data
        self.local_storage.set(self.__class__.__name__, buffer)

        # Write data in buffer
        if buffer.get(key):
            self.write(key, buffer.get(key))
            buffer[key] = []
            self.local_storage.set(self.__class__.__name__, buffer)

    async def _flush_buffer(self, key=None, data=None):
        if key is not None and data is not None and not self.store_and_forward:
            await self._write(key, data)
            return

        buffer = self.local_storage.get(self.__class__.__name__, {})
        if key is None:
            for key_ in buffer:
                await self._flush_buffer(key=key_)
            return

        # Add new data to buffer and save
        if key not in buffer:
            buffer[key] = []
        if isinstance(data, list):
            buffer[key] = data + buffer[key]
        self.local_storage.set(self.__class__.__name__, buffer)

        # Write data in buffer
        if buffer.get(key):
            await self._write(key, buffer.get(key))
            buffer[key] = []
            self.local_storage.set(self.__class__.__name__, buffer)
