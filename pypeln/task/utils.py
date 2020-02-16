import asyncio
from collections import namedtuple
from queue import Empty, Full, Queue
from threading import Lock

from pypeln import utils as pypeln_utils
import time


def get_namespace():
    return pypeln_utils.Namespace()


class TaskPool(object):
    def __init__(self, workers, loop):
        self.semaphore = asyncio.Semaphore(workers, loop=loop)
        self.tasks = set()
        self.closed = False
        self.loop = loop

    async def put(self, coro):

        if self.closed:
            raise RuntimeError("Trying put items into a closed TaskPool")

        await self.semaphore.acquire()

        task = asyncio.ensure_future(coro, loop=self.loop)
        self.tasks.add(task)
        task.add_done_callback(self.on_task_done)

    def on_task_done(self, task):
        self.tasks.remove(task)
        self.semaphore.release()

    async def join(self):
        await asyncio.gather(*self.tasks, loop=self.loop)
        self.closed = True

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()


class IterableQueue(asyncio.Queue):
    def __init__(self, maxsize, total_done, pipeline_namespace, **kwargs):
        super().__init__(maxsize=maxsize, **kwargs)

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

        # print(x)

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


class StageStatus:
    """
    Object passed to various `on_done` callbacks. It contains information about the stage in case book keeping is needed.
    """

    def __init__(self, namespace, lock):
        self._namespace = namespace
        self._lock = lock

    @property
    def done(self) -> bool:
        """
        `bool` : `True` if all workers finished. 
        """
        with self._lock:
            return self._namespace.active_workers == 0

    @property
    def active_workers(self):
        """
        `int` : Number of active workers. 
        """
        with self._lock:
            return self._namespace.active_workers

    def __str__(self):
        return f"StageStatus(done = {done}, active_workers = {active_workers})"


WorkerInfo = namedtuple("WorkerInfo", ["index"])


class StageReuseError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
