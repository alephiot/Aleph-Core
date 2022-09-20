"""
TODO
"""

import time
import asyncio


class WaitOneStep:

    def __init__(self, time_step=1):
        self.first_step = True
        self.time_step = time_step
        self.t = time.time()

        # Use croniter
        if isinstance(time_step, str):
            import croniter
            self.cron = croniter.croniter(time_step, time.time())

    def wait(self):
        if self.first_step:
            self.t = time.time()
            self.first_step = False
            return

        # Croniter
        if isinstance(self.time_step, str):
            c = self.cron.get_next()
            time.sleep(c - time.time())

        # Normal
        else:
            delta = time.time() - self.t
            if delta > self.time_step: return
            time.sleep(self.time_step - delta)
            self.t = time.time()

    async def async_wait(self):
        if self.first_step:
            self.t = time.time()
            self.first_step = False
            return

        # Croniter
        if isinstance(self.time_step, str):
            c = self.cron.get_next()
            time.sleep(c - time.time())

        # Normal
        else:
            delta = time.time() - self.t
            if delta > self.time_step: return
            await asyncio.sleep(self.time_step - delta)
            self.t = time.time()
