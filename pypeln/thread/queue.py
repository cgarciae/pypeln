import sys
import time
import traceback
import typing as tp
from queue import Queue, Empty, Full
from threading import Lock

from pypeln import utils as pypeln_utils

from . import utils

T = tp.TypeVar("T")


class PipelineException(tp.NamedTuple, BaseException):
    exception: tp.Optional[tp.Type[BaseException]]
    trace: str


class IterableQueue(Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(self, maxsize: int = 0, total_sources: int = 1):
        super().__init__(maxsize=maxsize)

        self.lock = Lock()
        self.namespace = utils.Namespace(
            remaining=total_sources, exception=False, force_stop=False
        )
        self.exception_queue: "Queue[PipelineException]" = Queue()

    def get(self, *arg, **kwargs) -> T:
        return super().get(*arg, **kwargs)

    def put(self, x: T):
        while True:
            try:
                super().put(x, timeout=pypeln_utils.TIMEOUT)
                return
            except Full as e:
                pass

    def __iter__(self) -> tp.Iterator[T]:

        while not self.is_done():

            if self.namespace.exception:
                exception, trace = self.exception_queue.get()

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            try:
                x = self.get(timeout=pypeln_utils.TIMEOUT)
            except Empty:
                continue

            if isinstance(x, pypeln_utils.Done):
                with self.lock:
                    self.namespace.remaining -= 1

                continue

            yield x

    def is_done(self):
        return self.namespace.force_stop or (
            self.namespace.remaining <= 0 and self.empty()
        )

    def done(self):
        self.put(pypeln_utils.DONE)

    def stop(self):
        with self.lock:
            self.namespace.remaining = 0

    def kill(self):
        self.namespace.force_stop = True

    def raise_exception(self, exception: BaseException):
        pipeline_exception = self.get_pipeline_exception(exception)
        self.namespace.exception = True
        self.exception_queue.put(pipeline_exception)

    def get_pipeline_exception(self, exception: BaseException) -> PipelineException:

        if isinstance(exception, PipelineException):
            return exception

        exception_type, _exception, _traceback = sys.exc_info()
        trace = "".join(
            traceback.format_exception(exception_type, _exception, _traceback)
        )

        trace = f"{exception.args}\n\n{trace}"

        return PipelineException(exception_type, trace)


class OutputQueues(tp.List[IterableQueue[T]], tp.Generic[T]):
    def put(self, x: T):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.done()

    def stop(self):
        for queue in self:
            queue.stop()

    def kill(self):
        for queue in self:
            queue.kill()
