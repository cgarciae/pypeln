import sys
import traceback
import typing as tp
import asyncio


from pypeln import utils as pypeln_utils

from . import utils
import time


T = tp.TypeVar("T")


class PipelineException(tp.NamedTuple, BaseException):
    exception: tp.Optional[tp.Type[BaseException]]
    trace: str


class IterableQueue(asyncio.Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(self, maxsize: int = 0, total_sources: int = 1):
        super().__init__(maxsize=maxsize)

        self.namespace = utils.Namespace(
            remaining=total_sources, exception=False, force_stop=False
        )
        self.exception_queue: asyncio.Queue[PipelineException] = asyncio.Queue(
            maxsize=1
        )

    async def get(self, *arg, **kwargs) -> tp.Awaitable[T]:
        return await super().get(*arg, **kwargs)

    def get_nowait(self, *arg, **kwargs) -> T:
        return super().get_nowait(*arg, **kwargs)

    def __iter__(self) -> tp.Iterator[T]:

        while not self.is_done():

            if self.namespace.exception:
                exception, trace = self.exception_queue.get_nowait()

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            try:
                x = self.get_nowait()
            except asyncio.QueueEmpty:
                time.sleep(pypeln_utils.TIMEOUT)
                continue

            if isinstance(x, pypeln_utils.Continue):
                continue
            if isinstance(x, pypeln_utils.Done):
                self.namespace.remaining -= 1

                continue

            yield x

    async def __aiter__(self) -> tp.AsyncIterator[T]:

        while not self.is_done():

            print(5)
            if self.namespace.exception:
                exception, trace = await self.exception_queue.get()

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            print(6)
            x = await self.get()

            print(x)

            if isinstance(x, pypeln_utils.Continue):
                continue
            elif isinstance(x, pypeln_utils.Done):
                self.namespace.remaining -= 1
                continue

            yield x

    def is_done(self):
        return self.namespace.force_stop or (
            self.namespace.remaining <= 0 and self.empty()
        )

    async def done(self):
        await self.put(pypeln_utils.DONE)

    def done_nowait(self):
        self.put_nowait(pypeln_utils.DONE)

    async def stop(self):
        self.namespace.remaining = 0
        await self.put(pypeln_utils.CONTINUE)

    def stop_nowait(self):
        self.namespace.remaining = 0
        self.put_nowait(pypeln_utils.CONTINUE)

    async def kill(self):
        self.namespace.force_stop = True
        await self.put(pypeln_utils.CONTINUE)

    def kill_nowait(self):
        self.namespace.force_stop = True
        self.put_nowait(pypeln_utils.CONTINUE)

    async def raise_exception(self, exception: BaseException):

        pypeline_exception = self.get_pipeline_exception(exception)

        self.namespace.exception = True
        await self.exception_queue.put(pypeline_exception)
        await self.put(pypeln_utils.CONTINUE)

    def raise_exception_nowait(self, exception: BaseException):
        pypeline_exception = self.get_pipeline_exception(exception)

        self.namespace.exception = True
        self.exception_queue.put_nowait(pypeline_exception)
        self.put_nowait(pypeln_utils.CONTINUE)

    # TODO: implement this logic in thread + process
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
    async def put(self, x: T):
        for queue in self:
            await queue.put(x)

    def put_nowait(self, x: T):
        for queue in self:
            queue.put_nowait(x)

    async def done(self):
        for queue in self:
            await queue.done()

    def done_nowait(self):
        for queue in self:
            queue.done_nowait()

    async def stop(self):
        for queue in self:
            await queue.stop()

    def stop_nowait(self):
        for queue in self:
            queue.stop_nowait()

    async def kill(self):
        for queue in self:
            await queue.kill()

    def kill_nowait(self):
        for queue in self:
            queue.kill_nowait()
