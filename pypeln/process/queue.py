import multiprocessing as mp
from multiprocessing.queues import Full, Empty

from pypeln import interfaces
from pypeln import utils as pypeln_utils

import typing as tp

# CONTEXT = get_context("fork")
CONTEXT = mp
MANAGER = CONTEXT.Manager()

T = tp.TypeVar("T")


class IterableQueue(interfaces.IterableQueue[T]):
    def __init__(self, maxsize=0):

        self.queue = CONTEXT.Queue(maxsize=maxsize)
        self.lock = CONTEXT.Lock()
        self.queue_namespace = MANAGER.Namespace(remaining=1, exception_trace=None)

    def __iter__(self):

        while not self.is_done():
            x = self.get()

            if self.queue_namespace.error:
                return

            if not pypeln_utils.is_continue(x):
                yield x

    def get(self):

        try:
            x = self.queue.get(timeout=pypeln_utils.TIMEOUT)
        except (Empty, Full):
            return pypeln_utils.CONTINUE

        if not pypeln_utils.is_done(x):
            return x
        else:
            with self.lock:
                self.queue_namespace.remaining -= 1

            return pypeln_utils.CONTINUE

    def is_done(self):
        return self.queue_namespace.remaining == 0 and self.queue.empty()

    def put(self, x):
        self.queue.put(x)

    def done(self):
        self.queue.put(pypeln_utils.DONE)
