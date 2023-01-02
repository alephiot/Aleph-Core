from typing import Dict, Optional
from abc import ABC
import threading
import asyncio
import logging

from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.wait_one_step import WaitOneStep
from aleph_core.utils.exceptions import Exceptions, Error
from aleph_core.utils.data import DataSet

logger = logging.getLogger(__name__)


class Connection(ABC):
    time_step = 10
    local_storage = LocalStorage()
    store_and_forward = False
    multi_thread = False

    def __init__(self, client_id=""):
        self.client_id = client_id
        self.__async_loop__ = None
        self.__subscribed_keys__ = set()

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

    def read(self, key: str, **kwargs) -> Optional[DataSet]:
        """
        Must return a list (data, a list of records) or a dict (single record)
        """
        return DataSet()

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
        Callback function for when a new message arrives
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

    async def _open_async(self, time_step):
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
                    self._buffer_flush_all()
                elif not current_state and previous_state:
                    logger.info("Disconnected")
                    self.on_disconnect()

            except Exception as e:
                logger.error("Failed on _open_async")
                self.on_error(Error(e))

            previous_state = current_state
            wait_one_step.wait()

    def open_async(self, time_step=None):
        """
        Executes the open function without blocking the main thread
        Calls is_connected on a loop and tries to reconnect if disconnected
        """
        if time_step is None:
            time_step = self.time_step
        self.__run_on_async_thread__(self._open_async(time_step))

    def _subscribe(self, key, time_step):
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

    async def _subscribe_async(self, key, time_step):
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

    def subscribe_async(self, key, time_step=None):
        """Executes the read function on a loop, without blocking the main thread"""
        if time_step is None:
            time_step = self.time_step
        if key in self.__subscribed_keys__:
            return
        self.__subscribed_keys__.add(key)

        if self.multi_thread:
            logger.info("Creating subscribe_async thread for key %s", key)
            threading.Thread(target=self._subscribe, args=(key, time_step,), daemon=True).start()
        else:
            logger.info("Adding subscribe_async coroutine for key %s", key)
            self.__run_on_async_thread__(self._subscribe_async(key, time_step))

    async def _write_async(self, key, data):
        self.safe_write(key, data)

    def write_async(self, key: str, data: DataSet):
        """
        Executes the safe_write function without blocking the main thread
        If the connection does not allow multithreading, we use the async thread
        """
        if self.multi_thread:
            threading.Thread(target=self.safe_write, args=(key, data,), daemon=True).start()
        else:
            self.__run_on_async_thread__(self._write_async(key, data))

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
            self._buffer_add_and_flush(key=key, data=data)
        except Exception as e:
            self.on_error(Error(e, client_id=self.client_id, key=key, data=data))

    def _buffer_name(self):
        return self.__class__.__name__

    def _buffer_flush_all(self):
        if not self.store_and_forward:
            return
        
        buffer = self.local_storage.get(self._buffer_name(), {})
        for key in buffer:
            try:
                self.write(key, DataSet(buffer.get(key)))
                buffer[key] = []
                self.local_storage.set(self._buffer_name(), buffer)
            except:
                continue

    def _buffer_add_and_flush(self, key: str, data: DataSet):
        if not self.store_and_forward:
            self.write(key, data)
            return
        
        buffer = self.local_storage.get(self._buffer_name(), {})
        if key not in buffer:
            buffer[key] = []
        buffer[key] = buffer[key] + list(data)
        self.local_storage.set(self._buffer_name(), buffer)
        
        self.write(key, DataSet(buffer.get(key)))
        buffer[key] = []
        self.local_storage.set(self._buffer_name(), buffer)


class AsyncConnection(Connection):

    async def open(self):
        return super().open()
    
    async def close(self):
        return super().close()
    
    async def read(self, key: str, **kwargs) -> Optional[DataSet]:
        return super().read(key, **kwargs)
    
    async def write(self, key: str, data: DataSet):
        return super().write(key, data)

    async def safe_read(self, key: str, **kwargs) -> DataSet | None:
        return super().safe_read(key, **kwargs)
    
    async def safe_write(self, key: str, data: DataSet):
        return super().safe_write(key, data)
    
    async def _open_async(self, time_step):
        pass # TODO

    def _subscribe(self, key, time_step):
        pass # TODO

    async def _subscribe_async(self, key, time_step):
        pass # TODO

    async def _write_async(self, key, data):
        await self.safe_write(key, data)
    
    async def _buffer_flush_all(self):
        pass # TODO

    async def _buffer_add_and_flush(self, key: str, data: DataSet):
        pass # TODO


