import asyncio
import time
import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from .. import utils
from ..queue import IterableQueue
from ..stage import Stage
from ..worker import ProcessFn, Worker


class FromIterable(tp.NamedTuple):
    iterable: tp.Union[tp.Iterable, tp.AsyncIterable]

    async def __call__(self, worker: Worker, **kwargs):
        iterable: tp.AsyncIterable

        if isinstance(self.iterable, tp.AsyncIterable):
            iterable = self.iterable
        else:
            sync_iterable: tp.Iterable

            if isinstance(self.iterable, pypeln_utils.BaseStage):
                sync_iterable = self.iterable.to_iterable(maxsize=0, return_index=True)
            else:
                sync_iterable = self.iterable

            queue = IterableQueue()
            loop = utils.get_running_loop()

            loop.run_in_executor(
                None,
                lambda: FromIterable.consume_iterable(
                    worker=worker, iterable=sync_iterable, queue=queue, loop=loop
                ),
            )

            iterable = queue

        i = 0
        async for x in iterable:
            if not isinstance(x, pypeln_utils.Element):
                x = pypeln_utils.Element(index=(i,), value=x)

            await worker.stage_params.output_queues.put(x)

            i += 1

    @staticmethod
    def consume_iterable(
        worker: Worker,
        iterable: tp.Iterable,
        queue: IterableQueue,
        loop: asyncio.AbstractEventLoop,
    ):
        try:
            for x in iterable:
                if worker.is_done:
                    return

                while queue.full():
                    if worker.is_done:
                        return

                    time.sleep(pypeln_utils.TIMEOUT)

                asyncio.run_coroutine_threadsafe(queue.put(x), loop)

            asyncio.run_coroutine_threadsafe(queue.done(), loop)

        except BaseException as e:
            e = queue.get_pipeline_exception(e)
            asyncio.run_coroutine_threadsafe(queue.raise_exception(e), loop)


@tp.overload
def from_iterable(
    iterable: tp.Union[tp.Iterable[T], tp.AsyncIterable[T]],
    maxsize: int = 0,
    use_thread: bool = True,
) -> Stage[T]:
    ...


@tp.overload
def from_iterable(
    maxsize: int = 0, use_thread: bool = True
) -> pypeln_utils.Partial[Stage[T]]:
    ...


def from_iterable(
    iterable: tp.Union[
        tp.Iterable[T], tp.AsyncIterable[T], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    use_thread: bool = True,
):
    """
    Creates a stage from an iterable. This function gives you more control of the iterable is consumed.
    Arguments:
        iterable: a source iterable.
        maxsize: this parameter is not used and only kept for API compatibility with the other modules.
        use_thread: If set to `True` (default) it will use a thread instead of a process to consume the iterable. Threads start faster and use thread memory to the iterable is not serialized, however, if the iterable is going to perform slow computations it better to use a process.

    Returns:
        If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if isinstance(iterable, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda iterable: from_iterable(
                iterable, maxsize=maxsize, use_thread=use_thread
            )
        )

    return Stage(
        process_fn=FromIterable(iterable),
        workers=1,
        maxsize=maxsize,
        total_sources=1,
        timeout=0,
        dependencies=[],
        on_start=None,
        on_done=None,
        f_args=[],
    )
