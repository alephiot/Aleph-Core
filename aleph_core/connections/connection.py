"""

"""

from aleph_core.utils.local_storage import LocalStorage
from aleph_core.utils.wait_one_step import WaitOneStep
from aleph_core.utils.exceptions import Exceptions, Error
from aleph_core.utils.datetime_functions import now, parse_date_to_timestamp

from abc import ABC
import threading
import asyncio
import logging


logger = logging.getLogger(__name__)


class Connection(ABC):
    key = None
    models = {}
    time_step = 10

    local_storage = LocalStorage()
    report_by_exception = True
    store_and_forward = False
    multi_thread = False

    def __init__(self, client_id=""):
        self.client_id = client_id
        self.__async_loop__ = None
        self.__unsubscribe_flags__ = []

    # ===================================================================================
    # Main functions
    # ===================================================================================
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

    def read(self, key, **kwargs):
        """
        Must return a list (data, a list of records) or a dict (single record)
        """
        return []

    def write(self, key, data):
        """
        Returns None.
        """
        return

    def is_open(self):
        """
        Returns a boolean (True if open, False if not open)
        """
        return True

    # ===================================================================================
    # Callbacks
    # ===================================================================================
    def on_new_data(self, key, data):
        """
        Callback function for when a new message arrives. Data can be a dict or a list of
        dict. This function is used by the read_async, subscribe async and subscribe
        methods.
        """
        return

    def on_read_error(self, error):
        """
        Callback function for when safe_read fails
        """
        return

    def on_write_error(self, error):
        """
        Callback function for when safe_write fails
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

    # ===================================================================================
    # Aux
    # ===================================================================================
    def __on_connect__(self):
        logger.info("Connected")
        self.__store_and_forward_flush_buffer__()
        self.local_storage.set(LocalStorage.LAST_TIME_READ, {})
        self.on_connect()

    def __on_disconnect__(self):
        logger.info("Disconnected")
        self.on_disconnect()

    def __run_on_async_thread__(self, coroutine):
        if self.__async_loop__ is None:
            logging.info("Starting async thread")
            self.__async_loop__ = asyncio.new_event_loop()
            threading.Thread(target=self.__async_loop__.run_forever, daemon=True).start()

        asyncio.run_coroutine_threadsafe(coroutine, self.__async_loop__)

    def __store_and_forward_flush_buffer__(self):
        if not self.store_and_forward:
            return

    def __store_and_forward_add_to_buffer__(self, key, data):
        if not self.store_and_forward:
            return

    def __subscribe_aux__(self, key, time_step=None):
        wait_one_step = WaitOneStep(time_step)

        while True:
            try:
                wait_one_step.wait()
                if not self.__unsubscribe_flags__[key]: break
                data = self.safe_read(key)
                if data is None or len(data) == 0: continue
                self.on_new_data(key, data)
            except Exception:
                self.unsubscribe(key)
                raise

    async def __subscribe_async_aux__(self, key, time_step):
        wait_one_step = WaitOneStep(time_step)
        while True:
            try:
                await wait_one_step.async_wait()
                if self.__unsubscribe_flags__[key]: break
                data = await self._safe_read(key)
                if data is None or len(data) == 0: continue
                self.on_new_data(key, data)
            except Exception:
                self.unsubscribe(key)
                raise

    # ===================================================================================
    # Main async functions
    # ===================================================================================
    async def _open(self):
        wait_one_step = WaitOneStep(self.time_step)

        previous_state = False
        while True:
            current_state = self.is_open()
            if not current_state:
                try:
                    self.open()
                    current_state = True
                except:
                    current_state = False

            # Callbacks
            if current_state and not previous_state: self.__on_connect__()
            if not current_state and previous_state: self.__on_disconnect__()
            previous_state = current_state

            await wait_one_step.async_wait()

    async def _read(self, key, **kwargs):
        return self.read(key, **kwargs)

    async def _write(self, key, data):
        self.write(key, data)

    # ===================================================================================
    # Derived async functions
    # ==================================================================================
    def open_async(self, time_step=None):
        """
        Executes the open function without blocking the main thread
        Calls is_connected on a loop and tries to reconnect if disconnected
        """
        self.__run_on_async_thread__(self._open())

    def write_async(self, key, data):
        """
        Executes the safe_write function without blocking the main thread
        If the connection does not allow multithreading, we use the async thread
        """
        if self.multi_thread:
            threading.Thread(target=self.safe_write, args=(key, data,), daemon=True).start()
        else:
            self.__run_on_async_thread__(self._safe_write(key, data))

    def subscribe_async(self, key, time_step=None):
        """
        Executes the read function on a loop, without blocking the main thread
        """
        if time_step is None: time_step = self.time_step
        if key in self.__unsubscribe_flags__ and not self.__unsubscribe_flags__[key]: return
        self.__unsubscribe_flags__[key] = False

        if self.multi_thread:
            logger.info("Creating subscribe_async thread for key %s", key)
            threading.Thread(target=self.__subscribe_aux__, args=(key, time_step,), daemon=True).start()
        else:
            logger.info("Adding subscribe_async coroutine for key %s", key)
            self.__run_on_async_thread__(self.__subscribe_async_aux__(key, time_step))

    def unsubscribe(self, key):
        self.__unsubscribe_flags__[key] = False

    # ===================================================================================
    # Async Safe read / write
    # ===================================================================================
    async def _safe_read(self, key, **kwargs):
        try:
            if not self.is_open(): self.open()
            args = self.clean_read_args(key, **kwargs)
            data = await self._read(key, **args)
            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            return self.clean_read_data(key, data, **args)

        except Exception as e:
            self.close()
            self.on_read_error(Error(e, client_id=self.client_id, key=key, kw_args=kwargs))
            return None

    async def _safe_write(self, key, data):
        try:
            data = self.clean_write_data(key, data)
        except Exception as e:
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))

        try:
            if not self.is_open(): self.open()
            await self._write(key, data)
            self.__store_and_forward_flush_buffer__()
        except Exception as e:
            self.close()
            self.__store_and_forward_add_to_buffer__(key, data)
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))
            return None

    # ===================================================================================
    # Safe read / write
    # ===================================================================================
    def safe_read(self, key, **kwargs):
        try:
            if not self.is_open(): self.open()
            data = self.read(key, **kwargs)
            if data is None:
                raise Exceptions.InvalidKey("Reading function returned None")

            return self.clean_read_data(key, data, **args)

        except Exception as e:
            self.close()
            self.on_read_error(Error(e, client_id=self.client_id, key=key, kw_args=kwargs))
            return None

    def safe_write(self, key, data):
        try:
            data = self.clean_write_data(key, data)
        except Exception as e:
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))

        try:
            if not self.is_open(): self.open()
            self.write(key, data)
            self.__store_and_forward_flush_buffer__()
        except Exception as e:
            self.close()
            self.__store_and_forward_add_to_buffer__(key, data)
            self.on_write_error(Error(e, client_id=self.client_id, key=key, data=data))
            return None

    # ===================================================================================
    # Cleaning
    # ===================================================================================
    def clean_read_args(self, key, **kwargs):
        last_time_read = self.local_storage.get(LocalStorage.LAST_TIME_READ, {}).get(key, now())

        args = {
            "since": parse_date_to_timestamp(kwargs.get("since", last_time_read)),
            "until": parse_date_to_timestamp(kwargs.get("until", None)),
            "limit": int(kwargs.get("limit", 1000)),
            "offset": int(kwargs.get("offset", 0)),
            "order": kwargs.get("order", None),
            "filter": kwargs.get("filter", None),
            "response_code": kwargs.get("response_code", None),
        }
        return args

    def clean_read_data(self, key, data, **kwargs):
        """Checks models and saves the last_time_read"""
        last_time_read = self.local_storage.get(LocalStorage.LAST_TIME_READ, {})
        last_time_read_for_key = last_time_read.get(key, 0)
        model = self.models.get(key, None)

        if not isinstance(data, list):
            data = [data]

        for record in data:
            try:
                if not isinstance(record, dict):
                    raise Exceptions.InvalidRecord(f"Record must be a dict, got {type(record)}")
                if record.get("t", 0) > last_time_read_for_key:
                    last_time_read_for_key = record["t"]
                if model:
                    model.validate(record)
            except Exception:
                logger.error(f"Error while cleaning record: {record}")

        if last_time_read_for_key > last_time_read.get(key, 0):
            last_time_read[key] = last_time_read_for_key
            self.local_storage.set(LocalStorage.LAST_TIME_READ, last_time_read)
        return data

    def clean_write_data(self, key, data):
        """Checks report by exception and that every record has a t and an id_"""
        if not isinstance(data, list):
            data = [data]

        past_values = self.local_storage.get(LocalStorage.PAST_VALUES, {})
        if key not in past_values:
            past_values[key] = {}

        for record in data:
            try:
                if not isinstance(record, dict):
                    raise Exceptions.InvalidRecord(f"Record must be a dict, got {type(record)}")

                # TODO (report by exception)

            except Exception:
                logger.error(f"Error while cleaning record: {record}")

        return data
