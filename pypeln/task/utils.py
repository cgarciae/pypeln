import asyncio
from collections import namedtuple
from queue import Empty, Full, Queue
from threading import Lock, Thread

from pypeln import utils as pypeln_utils
import time


LOOP = asyncio.new_event_loop()


def run_on_loop(f_coro):

    if not LOOP.is_running():

        def run():
            LOOP.run_forever()

        thread = Thread(target=run)
        thread.daemon = True
        thread.start()

    LOOP.call_soon_threadsafe(lambda: LOOP.create_task(f_coro()))


def get_namespace():
    return pypeln_utils.Namespace()


class TaskPool(object):
    def __init__(self, workers):
        self.semaphore = asyncio.Semaphore(workers)
        self.tasks = set()
        self.closed = False

    async def put(self, coro):

        if self.closed:
            raise RuntimeError("Trying put items into a closed TaskPool")

        await self.semaphore.acquire()

        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.on_task_done)

    def on_task_done(self, task):
        self.tasks.remove(task)
        self.semaphore.release()

    async def join(self):
        await asyncio.gather(*self.tasks)
        self.closed = True

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()


class IterableQueue(asyncio.Queue):
    def __init__(self, maxsize, total_done, pipeline_namespace, loop, **kwargs):
        super().__init__(maxsize=maxsize, loop=loop, **kwargs)

        self.remaining = total_done
        self.pipeline_namespace = pipeline_namespace

    async def __aiter__(self):

        while not self.is_done():
            x = await self.get()

            if self.pipeline_namespace.error:
                return

            if not pypeln_utils.is_continue(x):
                yield x

    def __iter__(self):
        while not self.is_done():

            if self.pipeline_namespace.error:
                return

            if self.empty():
                time.sleep(pypeln_utils.TIMEOUT)
            else:
                x = self.get_nowait()

                if not pypeln_utils.is_continue(x):
                    yield x

    async def get(self):

        x = await super().get()

        if pypeln_utils.is_done(x):
            self.remaining -= 1
            return pypeln_utils.CONTINUE

        return x

    def get_nowait(self):

        x = super().get_nowait()

        if pypeln_utils.is_done(x):
            self.remaining -= 1
            return pypeln_utils.CONTINUE

        return x

    def is_done(self):
        return self.remaining == 0  # and self.empty()

    async def done(self):
        await self.put(pypeln_utils.DONE)

    def done_nowait(self):
        self.put_nowait(pypeln_utils.DONE)


class MultiQueue(list):
    async def put(self, x):
        for queue in self:
            await queue.put(x)

    async def done(self):
        for queue in self:
            await queue.put(pypeln_utils.DONE)

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.done()


class StageStatus(object):
    def __init__(self):
        pass

    @property
    def done(self):
        return True

    @property
    def active_workers(self):
        return 0

    def __str__(self):
        return "StageStatus(done = {done}, active_workers = {active_workers})".format(
            done=self.done, active_workers=self.active_workers,
        )
