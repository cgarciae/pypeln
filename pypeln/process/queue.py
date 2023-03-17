import sys
import traceback
import typing as tp

from pypeln import utils as pypeln_utils

from . import utils
from .. import config


T = tp.TypeVar("T")


PipelineException = pypeln_utils.PipelineException


def create_iterable_queue_class(cls):
    class IterableQueue(cls, tp.Generic[T], tp.Iterable[T]):
        def __init__(self, maxsize: int = 0, total_sources: int = 1):
            self.multiprocessing = config.config().get_multiprocessing_impl()
            super().__init__(maxsize=maxsize, ctx=self.multiprocessing[0].get_context())

            self.namespace = utils.Namespace(
                remaining=total_sources, exception=False, force_stop=False
            )
            self.exception_queue: self.multiprocessing[1].Queue[PipelineException] = self.multiprocessing[1].Queue(
                ctx=self.multiprocessing[0].get_context()
            )

        def get(self, block: bool = True, timeout: tp.Optional[float] = None) -> T:
            while True:

                if self.namespace.exception:
                    exception, trace = self.exception_queue.get()

                    try:
                        exception = exception(f"\n\n{trace}")
                    except:
                        exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                    raise exception

                x = super().get(block=block, timeout=timeout)

                if isinstance(x, pypeln_utils.Done):
                    with self.namespace:
                        self.namespace.remaining -= 1

                    continue

                return x

        def clear(self):
            try:
                while True:
                    self.get_nowait()
            except self.multiprocessing[1].Empty:
                pass

        def __iter__(self) -> tp.Iterator[T]:

            while not self.is_done():

                try:
                    x = self.get(timeout=pypeln_utils.TIMEOUT)
                except self.multiprocessing[1].Empty:
                    continue

                yield x


        def is_done(self):

            force_stop = self.namespace.force_stop
            remaining = self.namespace.remaining

            return force_stop or (remaining <= 0 and self.empty())

        def worker_done(self):
            # with self.namespace:
            #     self.namespace.remaining -= 1
            self.put(pypeln_utils.DONE)

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

        def __getstate__(self):
            return super().__getstate__() + (self.namespace, self.exception_queue)

        def __setstate__(self, state):
            super().__setstate__(state[:-2])
            self.namespace, self.exception_queue = state[-2:]
    
    return IterableQueue


def create_output_queue_class(cls):
    class OutputQueues(tp.List[cls[T]], tp.Generic[T]):
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
    
    return OutputQueues

multiprocessing = config.config().get_multiprocessing_impl()[1]
IterableQueue = create_iterable_queue_class(multiprocessing.Queue)
OutputQueues = create_output_queue_class(IterableQueue)