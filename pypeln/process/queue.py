import multiprocessing as mp
from multiprocessing.queues import Full, Empty, Queue

from pypeln import interfaces
from pypeln import utils as pypeln_utils

import typing as tp
import traceback
import sys

# CONTEXT = get_context("fork")
CONTEXT = mp
MANAGER = CONTEXT.Manager()

T = tp.TypeVar("T")


class IterableQueue(Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(self, maxsize: int = 0, total_sources: int = 1):

        super().__init__(maxsize=maxsize, ctx=mp.get_context())

        self.lock = CONTEXT.Lock()
        self.queue_namespace = MANAGER.Namespace(
            remaining=total_sources, exception_trace=None, force_stop=False
        )

    def __iter__(self):

        while not self.is_done():

            try:
                x = self.get(timeout=pypeln_utils.TIMEOUT)
            except Empty:
                continue

            if self.queue_namespace.exception_trace is not None:

                exception, trace = self.queue_namespace.exception_trace

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            if isinstance(x, pypeln_utils.Done):
                with self.lock:
                    self.queue_namespace.remaining -= 1

                continue

            yield x

    def is_done(self):
        return self.queue_namespace.force_stop or (
            self.queue_namespace.remaining <= 0 and self.empty()
        )

    def done(self):
        self.put(pypeln_utils.DONE)

    def stop(self):
        with self.lock:
            self.queue_namespace.remaining = 0

    def kill(self):
        self.queue_namespace.force_stop = True

    def start(self):
        ...

    def raise_exception(self, exception: BaseException):
        trace = "".join(traceback.format_exception(*sys.exc_info()))
        self.queue_namespace.exception_trace = (exception, trace)
