import abc
from dataclasses import dataclass, field
import multiprocessing
import time
import typing as tp
from typing import Protocol

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue, OutputQueues
from . import stage

Kwargs = tp.Dict[str, tp.Any]
T = tp.TypeVar("T")


class StageProtocol(Protocol):
    lock: multiprocessing.synchronize.Lock
    namespace: tp.Any

    def worker_done(self):
        ...


class WorkerInfo(tp.NamedTuple):
    index: int


@dataclass
class Worker(tp.Generic[T]):
    index: int
    input_queue: IterableQueue[T]
    output_queues: OutputQueues
    stage: StageProtocol
    main_queue: IterableQueue
    timeout: float = 0
    namespace: utils.Namespace = field(
        default_factory=lambda: utils.Namespace(done=False, task_start_time=None)
    )
    on_start: tp.Optional[tp.Callable[..., Kwargs]] = None
    on_done: tp.Optional[tp.Callable[..., Kwargs]] = None
    process: tp.Optional[multiprocessing.Process] = None

    @abc.abstractmethod
    def process_fn(self, **kwargs):
        ...

    def __call__(self):
        worker_info = WorkerInfo(index=self.index)

        f_args: tp.List[str] = (
            pypeln_utils.function_args(self.f) if self.on_start else []
        )
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
            else:
                kwargs = {}

            if kwargs is None:
                kwargs = {}

            kwargs.setdefault("worker_info", worker_info)

            self.process_fn(
                **{key: value for key, value in kwargs.items() if key in f_args},
            )

            self.stage.worker_done()

            if self.on_done is not None:

                kwargs.setdefault(
                    "stage_status",
                    StageStatus(namespace=self.stage.namespace, lock=self.stage.lock),
                )

                self.on_done(
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in on_done_args
                    }
                )

        except BaseException as e:
            self.main_queue.raise_exception(e)
        finally:
            self.namespace.done = True
            self.output_queues.done()

    def start(self):
        [self.process] = start_workers(self)

    def stop(self):
        self.namespace.task_start_time = None
        self.process.terminate()

    def done(self):
        self.namespace.done = True

    def did_timeout(self):
        return (
            self.timeout
            and not self.namespace.done
            and self.namespace.task_start_time is not None
            and (time.time() - self.namespace.task_start_time > self.timeout)
        )

    def __del__(self):
        self.process.terminate()

    @dataclass
    class MeasureTaskTime:
        worker: "Worker"

        def __enter__(self):
            self.worker.namespace.task_start_time = time.time()
            1

        def __exit__(self, *args):
            self.worker.namespace.task_start_time = None

    def measure_task_time(self):
        return self.MeasureTaskTime(self)


class WorkerApply(Worker[T], tp.Generic[T]):
    f: tp.Callable

    @abc.abstractmethod
    def apply(self, elem: T, **kwargs):
        ...

    def process_fn(self, **kwargs):
        for elem in self.input_queue:
            with self.measure_task_time():
                self.apply(elem, **kwargs)


class StageStatus:
    """
    Object passed to various `on_done` callbacks. It contains information about the stage in case book keeping is needed.
    """

    def __init__(self, namespace, lock):
        self._namespace = namespace
        self._lock = lock

    @property
    def done(self) -> bool:
        """
        `bool` : `True` if all workers finished. 
        """
        with self._lock:
            return self._namespace.active_workers == 0

    @property
    def active_workers(self):
        """
        `int` : Number of active workers. 
        """
        with self._lock:
            return self._namespace.active_workers

    def __str__(self):
        return (
            f"StageStatus(done = {self.done}, active_workers = {self.active_workers})"
        )


# ----------------------------------------------------------------
# create_daemon_workers
# ----------------------------------------------------------------
def start_workers(
    target: tp.Callable,
    n_workers: int = 1,
    args: tp.Tuple[tp.Any, ...] = tuple(),
    kwargs: tp.Optional[tp.Dict[tp.Any, tp.Any]] = None,
) -> tp.List[multiprocessing.Process]:
    if kwargs is None:
        kwargs = {}

    workers: tp.List[multiprocessing.Process] = []

    for _ in range(n_workers):
        t = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        workers.append(t)

    return workers
