import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import T, Kwargs
from dataclasses import dataclass

from . import utils
from .queue import IterableQueue, OutputQueues
from .worker import Worker, StageParams, ProcessFn
from .supervisor import Supervisor


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    process_fn: ProcessFn
    workers: int
    timeout: float
    dependencies: tp.List["Stage"]
    input_queue: IterableQueue
    output_queues: OutputQueues
    on_start: tp.Optional[tp.Callable[..., Kwargs]]
    on_done: tp.Optional[tp.Callable[..., Kwargs]]
    use_threads: bool
    f_args: tp.List[str]
    built: bool = False
    started: bool = False

    @classmethod
    def create(
        cls,
        process_fn: ProcessFn,
        workers: int,
        maxsize: int,
        timeout: float,
        total_sources: int,
        dependencies: tp.List["Stage"],
        on_start: tp.Optional[tp.Callable[..., Kwargs]],
        on_done: tp.Optional[tp.Callable[..., Kwargs]],
        use_threads: bool,
        f_args: tp.List[str],
    ):
        return cls(
            process_fn=process_fn,
            workers=workers,
            timeout=timeout,
            dependencies=dependencies,
            input_queue=IterableQueue(maxsize=maxsize, total_sources=total_sources),
            output_queues=OutputQueues(),
            on_start=on_start,
            on_done=on_done,
            use_threads=use_threads,
            f_args=f_args,
        )

    def build(self) -> tp.Iterable["Stage"]:

        if self.started:
            raise pypeln_utils.StageReuseError()

        if self.built:
            return

        self.built = True

        for dependency in self.dependencies:
            dependency.output_queues.append(self.input_queue)

        yield self

        for dependency in self.dependencies:
            yield from dependency.build()

    def start(self, main_queue: IterableQueue) -> tp.Iterable[Worker]:

        self.started = True

        stage_params = StageParams.create(
            input_queue=self.input_queue,
            output_queues=self.output_queues,
            total_workers=self.workers,
        )

        for i in range(self.workers):
            worker = Worker(
                process_fn=self.process_fn,
                index=i,
                timeout=self.timeout,
                stage_params=stage_params,
                main_queue=main_queue,
                on_start=self.on_start,
                on_done=self.on_done,
                use_threads=self.use_threads,
                f_args=self.f_args,
            )
            worker.start()

            yield worker

    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[T]:

        # build stages first to verify reuse
        stages = list(self.build())

        main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
            maxsize=maxsize, total_sources=self.workers,
        )

        # add main_queue before starting
        self.output_queues.append(main_queue)

        workers: tp.List[Worker] = list(
            pypeln_utils.concat(stage.start(main_queue) for stage in stages)
        )
        supervisor = Supervisor(workers=workers, main_queue=main_queue)

        with supervisor:
            for elem in main_queue:
                if return_index:
                    yield elem
                else:
                    yield elem.value

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)

