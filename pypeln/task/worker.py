import abc
import asyncio
import asyncio
from copy import copy
from dataclasses import dataclass, field
import inspect
import time
import typing as tp

from concurrent.futures import Future

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue, OutputQueues

WorkerConstructor = tp.Callable[["StageParams", IterableQueue], "Worker"]
Kwargs = tp.Dict[str, tp.Any]
T = tp.TypeVar("T")


class ProcessFn(pypeln_utils.Protocol):
    async def __call__(self, worker: "Worker", **kwargs):
        ...


class StageParams(tp.NamedTuple):
    input_queue: IterableQueue
    output_queues: OutputQueues
    namespace: utils.Namespace

    @classmethod
    def create(
        cls, input_queue: IterableQueue, output_queues: OutputQueues
    ) -> "StageParams":
        return cls(
            namespace=utils.Namespace(active_workers=1),
            input_queue=input_queue,
            output_queues=output_queues,
        )

    def worker_done(self):
        self.namespace.active_workers -= 1


class WorkerInfo(tp.NamedTuple):
    index: int


@dataclass
class Worker(tp.Generic[T]):

    process_fn: ProcessFn
    timeout: float
    stage_params: StageParams
    main_queue: IterableQueue
    on_start: tp.Optional[tp.Callable[..., tp.Union[Kwargs, tp.Awaitable[Kwargs]]]]
    on_done: tp.Optional[tp.Callable[..., tp.Union[Kwargs, tp.Awaitable[Kwargs]]]]
    f_args: tp.List[str]
    tasks: "TaskPool"
    process: tp.Optional[Future] = None
    is_done: bool = False

    async def __call__(self):

        worker_info = WorkerInfo(index=0)

        on_start_args: tp.List[str] = (
            pypeln_utils.function_args(self.on_start) if self.on_start else []
        )
        on_done_args: tp.List[str] = (
            pypeln_utils.function_args(self.on_done) if self.on_done else []
        )

        try:
            if self.on_start is not None:
                on_start_kwargs = dict(worker_info=worker_info)
                kwargs = self.on_start(
                    **{
                        key: value
                        for key, value in on_start_kwargs.items()
                        if key in on_start_args
                    }
                )
                if isinstance(kwargs, tp.Awaitable):
                    kwargs = await kwargs
            else:
                kwargs = {}

            if kwargs is None:
                kwargs = {}

            kwargs.setdefault("worker_info", worker_info)

            async with self.tasks:
                await self.process_fn(
                    self,
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in self.f_args
                    },
                )

            self.stage_params.worker_done()

            if self.on_done is not None:

                kwargs.setdefault(
                    "stage_status",
                    StageStatus(),
                )

                coro = self.on_done(
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in on_done_args
                    }
                )

                if isinstance(coro, tp.Awaitable):
                    await coro

        except asyncio.CancelledError:
            pass
        except BaseException as e:
            await self.main_queue.raise_exception(e)
        finally:
            self.is_done = True
            self.tasks.stop()
            await self.stage_params.output_queues.done()

    def start(self):
        [self.process] = start_workers(self)

    def stop(self):

        if self.process is None:
            return

        def cancel():
            self.process.cancel()
            # self.is_done = True

        self.tasks.stop()
        utils.run_function_in_loop(cancel)


class Applicable(pypeln_utils.Protocol):
    def apply(self, worker: "Worker", elem: tp.Any, **kwargs):
        ...


class ApplyProcess(ProcessFn, Applicable):
    async def __call__(self, worker: Worker, **kwargs):

        async for elem in worker.stage_params.input_queue:

            await worker.tasks.put(
                pypeln_utils.get_callable(self.apply, worker, elem, **kwargs)
            )


class StageStatus(tp.NamedTuple):
    """
    Object passed to various `on_done` callbacks. It contains information about the stage in case book keeping is needed.
    """

    @property
    def done(self) -> bool:
        """
        `bool` : `True` if all workers finished.
        """
        return True

    @property
    def active_workers(self):
        """
        `int` : Number of active workers.
        """
        return 0

    def __str__(self):
        return (
            f"StageStatus(done = {self.done}, active_workers = {self.active_workers})"
        )


@dataclass
class TaskPool:
    semaphore: tp.Optional[asyncio.Semaphore]
    tasks: tp.Set[asyncio.Task]
    timeout: float
    closed: bool

    @classmethod
    def create(cls, workers: int, timeout: float = 0):
        return cls(
            semaphore=asyncio.Semaphore(workers) if workers else None,
            tasks=set(),
            timeout=timeout,
            closed=False,
        )

    async def put(self, coro_f: tp.Callable[[], tp.Awaitable]):

        if self.closed:
            raise RuntimeError("Trying put items into a closed TaskPool")

        if self.semaphore:
            await self.semaphore.acquire()

        task = asyncio.ensure_future(self.get_task(coro_f))

        self.tasks.add(task)

        task.add_done_callback(self.on_task_done)

    async def get_task(self, coro_f: tp.Callable[[], tp.Awaitable]):
        coro = coro_f()

        if self.timeout:
            coro = asyncio.wait_for(coro, timeout=self.timeout)

        try:
            await coro
        except asyncio.TimeoutError:
            pass

    def on_task_done(self, task):
        self.tasks.remove(task)

        if self.semaphore:
            self.semaphore.release()

    def stop(self):

        for task in self.tasks:
            if not task.cancelled() or not task.done():
                utils.run_function_in_loop(task.cancel)

        self.tasks.clear()

    async def join(self):
        self.closed = True
        await asyncio.gather(*self.tasks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.join()

    def __len__(self) -> int:
        return len(self.tasks)


# ----------------------------------------------------------------
# create_daemon_workers
# ----------------------------------------------------------------


def start_workers(
    target: tp.Callable,
    n_workers: int = 1,
    args: tp.Tuple[tp.Any, ...] = tuple(),
    kwargs: tp.Optional[tp.Dict[tp.Any, tp.Any]] = None,
    use_threads: bool = False,
) -> tp.List[Future]:

    if kwargs is None:
        kwargs = {}

    workers: tp.List[Future] = []

    for _ in range(n_workers):
        t = utils.run_coroutine_in_loop(
            pypeln_utils.get_callable(target, *args, **kwargs)
        )
        workers.append(t)

    return workers
