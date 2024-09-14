import asyncio
from threading import Thread
from typing import Coroutine, Callable


class AsyncHelper:
    def __init__(self):
        self.main_thread: Thread = None
        self.main_loop = None

    def run(self, coroutine):
        return asyncio.run(coroutine)

    def run_coroutine_threadsafe(self, coroutine: Coroutine):
        if self.main_loop is None:
            self.main_loop = asyncio.new_event_loop()
            self.main_thread = Thread(target=self.main_loop.run_forever, daemon=True)
            self.main_thread.start()

        asyncio.run_coroutine_threadsafe(coroutine, self.main_loop)

    def run_on_thread(self, function: Callable, *args, **kwargs):
        thread = Thread(target=function, args=args, kwargs=kwargs, daemon=True)
        thread.start()
