import typing as tp
from dataclasses import dataclass
from pypeln import utils as pypeln_utils

from .queue import IterableQueue, OutputQueues
from .worker import Worker, StageParams, WorkerConstructor
from .supervisor import Supervisor


T = tp.TypeVar("T")


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    stage_params: StageParams
    worker_constructor: WorkerConstructor
    total_sources: int
    dependencies: tp.List["Stage"]
    built: bool = False
    started: bool = False

    @classmethod
    def create(
        cls,
        total_sources: int,
        maxsize: int,
        worker_constructor: WorkerConstructor,
        dependencies: tp.List["Stage"],
    ):

        input_queue: IterableQueue[T] = IterableQueue(
            maxsize=maxsize, total_sources=total_sources,
        )

        for dependency in dependencies:
            dependency.stage_params.output_queues.append(input_queue)

        return cls(
            stage_params=StageParams.create(
                input_queue=input_queue, output_queues=OutputQueues(),
            ),
            total_sources=total_sources,
            worker_constructor=worker_constructor,
            dependencies=dependencies,
        )

    def build(self) -> tp.Iterable["Stage"]:

        if self.started:
            raise pypeln_utils.StageReuseError()

        if self.built:
            return

        self.built = True

        yield self

        for dependency in self.dependencies:
            yield from dependency.build()

    def start(self, main_queue: IterableQueue) -> Worker:

        self.started = True

        worker = self.worker_constructor(self.stage_params, main_queue)
        worker.start()

        return worker

    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[T]:

        # build stages first to verify reuse
        stages = list(self.build())

        main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
            maxsize=maxsize, total_sources=1,
        )

        # add main_queue before starting
        self.stage_params.output_queues.append(main_queue)

        workers: tp.List[Worker] = [stage.start(main_queue) for stage in stages]
        supervisor = Supervisor(workers=workers, main_queue=main_queue)

        with supervisor:
            for elem in main_queue:
                if return_index:
                    yield elem
                else:
                    yield elem.value

    async def to_async_iterable(
        self, maxsize: int, return_index: bool
    ) -> tp.Iterable[T]:

        # build stages first to verify reuse
        stages = list(self.build())

        main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
            maxsize=maxsize, total_sources=1,
        )

        # add main_queue before starting
        self.stage_params.output_queues.append(main_queue)

        workers: tp.List[Worker] = [stage.start(main_queue) for stage in stages]
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
