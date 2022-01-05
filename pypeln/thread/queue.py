import sys
import traceback
import typing as tp
from queue import Queue, Empty, Full

from pypeln import utils as pypeln_utils

from . import utils

T = tp.TypeVar("T")


PipelineException = utils.pypeln_utils.PipelineException


class IterableQueue(Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(self, maxsize: int = 0, total_sources: int = 1):
        super().__init__(maxsize=maxsize)

        self.namespace = utils.Namespace(
            remaining=total_sources, exception=False, force_stop=False
        )
        self.exception_queue: "Queue[PipelineException]" = Queue()

    def get(self, block: bool = True, timeout: tp.Optional[float] = None) -> T:
        while True:

            if self.namespace.exception:
                exception, trace = self.exception_queue.get()

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            # get is implemented like this for thread but not for process
            # because processes can be stopped easily were as
            # threads have to be active when being terminated, implementing
            # get in this way ensures the thread constantly active.
            if block and timeout is None:
                while True:
                    try:
                        x = super().get(block=True, timeout=pypeln_utils.TIMEOUT)
                        break
                    except Empty:
                        continue
            else:
                x = super().get(block=block, timeout=timeout)

            if isinstance(x, pypeln_utils.Done):
                with self.namespace:
                    self.namespace.remaining -= 1

                continue

            return x

    # put is implemented like this for thread but not for process
    # because processes can be stopped easily were as
    # threads have to be active when being terminated, implementing
    # put in this way ensures the thread constantly active.
    def put(self, x: T, block: bool = True, timeout: tp.Optional[float] = None):
        if block and timeout is None:
            while True:
                try:
                    super().put(x, block=True, timeout=pypeln_utils.TIMEOUT)
                    return
                except Full as e:
                    pass
        else:
            return super().put(x, block=False, timeout=timeout)

    def clear(self):
        try:
            while True:
                self.get_nowait()
        except Empty:
            pass

    def __iter__(self) -> tp.Iterator[T]:

        while not self.is_done():

            try:
                x = self.get(timeout=pypeln_utils.TIMEOUT)
            except Empty:
                continue

            yield x

    def is_done(self):
        force_stop = self.namespace.force_stop
        remaining = self.namespace.remaining

        return force_stop or (remaining <= 0 and self.empty())

    def worker_done(self):
        self.put(pypeln_utils.DONE)
        # with self.namespace:
        #     self.namespace.remaining -= 1

    def stop(self):
        with self.namespace:
            self.namespace.remaining = 0
        self.clear()

    def kill(self):
        with self.namespace:
            self.namespace.remaining = 0
            self.namespace.force_stop = True
        self.clear()

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

    def worker_done(self):
        for queue in self:
            queue.worker_done()

    def stop(self):
        for queue in self:
            queue.stop()

    def kill(self):
        for queue in self:
            queue.kill()
