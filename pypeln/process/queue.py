import multiprocessing
from multiprocessing.queues import Empty, Queue
import sys
import traceback
import typing as tp

from pypeln import interfaces
from pypeln import utils as pypeln_utils

from . import utils


T = tp.TypeVar("T")


class IterableQueue(Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(self, maxsize: int = 0, total_sources: int = 1):

        super().__init__(maxsize=maxsize, ctx=multiprocessing.get_context())

        self.lock = multiprocessing.Lock()
        self.queue_namespace = utils.Namespace(
            remaining=total_sources, exception_trace=None, force_stop=False
        )

    def __iter__(self) -> tp.Iterator[T]:

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

    def raise_exception(self, exception: BaseException):
        _exception_class, _exception, _traceback = sys.exc_info()
        trace = "".join(
            traceback.format_exception(_exception_class, _exception, _traceback)
        )
        self.queue_namespace.exception_trace = (exception, trace)
