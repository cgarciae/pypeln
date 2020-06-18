import typing as tp
from dataclasses import dataclass
from pypeln import utils as pypeln_utils
from pypeln.utils import T, Kwargs

from .queue import IterableQueue, OutputQueues
from .worker import Worker, StageParams, TaskPool, ProcessFn
from .supervisor import Supervisor
from . import utils


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    # stage_params: StageParams
    # worker_constructor: WorkerConstructor
    # total_sources: int
    # dependencies: tp.List["Stage"]
    # built: bool = False
    # started: bool = False

    process_fn: ProcessFn
    workers: int
    maxsize: int
    total_sources: int
    timeout: float
    dependencies: tp.List["Stage"]
    on_start: tp.Optional[tp.Callable[..., tp.Union[Kwargs, tp.Awaitable[Kwargs]]]]
    on_done: tp.Optional[tp.Callable[..., tp.Union[Kwargs, tp.Awaitable[Kwargs]]]]
    f_args: tp.List[str]
    built: bool = False
    started: bool = False

    # @classmethod
    # def create(
    #     cls,
    #     total_sources: int,
    #     maxsize: int,
    #     worker_constructor: WorkerConstructor,
    #     dependencies: tp.List["Stage"],
    # ):

    #     # TODO: move this logic to build, defer the creation of queues
    #     input_queue: IterableQueue[T] = IterableQueue(
    #         maxsize=maxsize, total_sources=total_sources,
    #     )

    #     for dependency in dependencies:
    #         dependency.stage_params.output_queues.append(input_queue)

    #     return cls(
    #         stage_params=StageParams.create(
    #             input_queue=input_queue, output_queues=OutputQueues(),
    #         ),
    #         total_sources=total_sources,
    #         worker_constructor=worker_constructor,
    #         dependencies=dependencies,
    #     )

    def build(
        self,
        built: tp.Dict["Stage", OutputQueues],
        output_queue: IterableQueue,
        main_queue: IterableQueue,
    ) -> tp.Iterable[Worker]:

        if self in built:
            built[self].append(output_queue)
            return
        else:
            built[self] = OutputQueues([output_queue])

        input_queue = IterableQueue(
            maxsize=self.maxsize, total_sources=self.total_sources
        )

        stage_params = StageParams.create(
            input_queue=input_queue, output_queues=built[self],
        )

        yield Worker(
            process_fn=self.process_fn,
            timeout=self.timeout,
            stage_params=stage_params,
            main_queue=main_queue,
            on_start=self.on_start,
            on_done=self.on_done,
            f_args=self.f_args,
            tasks=TaskPool.create(workers=self.workers, timeout=self.timeout),
        )

        for dependency in self.dependencies:
            yield from dependency.build(built, input_queue, main_queue)

    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[T]:

        # create a running event loop in case it doesn't exist
        utils.get_running_loop()

        main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
            maxsize=maxsize, total_sources=self.workers,
        )

        built = {}

        workers: tp.List[Worker] = list(self.build(built, main_queue, main_queue))
        supervisor = Supervisor(workers=workers, main_queue=main_queue)

        with supervisor:
            for elem in main_queue:
                if return_index:
                    yield elem
                else:
                    yield elem.value

    async def to_async_iterable(
        self, maxsize: int, return_index: bool
    ) -> tp.AsyncIterable[T]:

        # build stages first to verify reuse
        main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
            maxsize=maxsize, total_sources=self.workers,
        )

        built = {}

        workers: tp.List[Worker] = list(self.build(built, main_queue, main_queue))
        supervisor = Supervisor(workers=workers, main_queue=main_queue)

        async with supervisor:
            async for elem in main_queue:
                if return_index:
                    yield elem
                else:
                    yield elem.value

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)

    def __aiter__(self):
        return self.to_async_iterable(maxsize=0, return_index=False)

    async def _await(self):
        return [x async for x in self]

    def __await__(self) -> tp.Generator[tp.Any, None, tp.List[T]]:
        return self._await().__await__()
