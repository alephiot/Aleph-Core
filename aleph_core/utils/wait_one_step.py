import time
import asyncio
import croniter


class WaitOneStep:
    """
    Class to sleep for a given time. Usage:
    w = WaitOneStep(time_step=2)
    w.wait()

    The time_Step can be the sleep time in seconds or a cron job expression
    that sleeps until the next event.
    """

    def __init__(self, time_step=1):
        self.first_step = True
        self.time_step = time_step
        self.t = time.time()

        if isinstance(time_step, str):
            self.cron = croniter.croniter(time_step, time.time())

    def wait(self):
        if self.first_step:
            self.t = time.time()
            self.first_step = False
            return

        if isinstance(self.time_step, str):
            c = self.cron.get_next()
            time.sleep(c - time.time())
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

        if isinstance(self.time_step, str):
            c = self.cron.get_next()
            time.sleep(c - time.time())

        else:
            delta = time.time() - self.t
            if delta > self.time_step: return
            await asyncio.sleep(self.time_step - delta)
            self.t = time.time()
