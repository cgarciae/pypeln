import abc
from copy import copy
from dataclasses import dataclass, field
import functools
import multiprocessing
from multiprocessing import synchronize
import threading
import time
import typing as tp

import stopit

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue, OutputQueues

WorkerConstructor = tp.Callable[[int, "StageParams", IterableQueue], "Worker"]
Kwargs = tp.Dict[str, tp.Any]
T = tp.TypeVar("T")


class ProcessFn(pypeln_utils.Protocol):
    def __call__(self, worker: "Worker", **kwargs):
        ...


class StageParams(tp.NamedTuple):
    input_queue: IterableQueue
    output_queues: OutputQueues
    lock: synchronize.Lock
    namespace: utils.Namespace

    @classmethod
    def create(
        cls, input_queue: IterableQueue, output_queues: OutputQueues, total_workers: int
    ) -> "StageParams":
        return cls(
            lock=multiprocessing.Lock(),
            namespace=utils.Namespace(active_workers=total_workers),
            input_queue=input_queue,
            output_queues=output_queues,
        )

    def worker_done(self):
        with self.namespace:
            self.namespace.active_workers -= 1


class WorkerInfo(tp.NamedTuple):
    index: int


@dataclass
class Worker(tp.Generic[T]):
    process_fn: ProcessFn
    index: int
    timeout: float
    stage_params: StageParams
    main_queue: IterableQueue
    on_start: tp.Optional[tp.Callable[..., Kwargs]]
    on_done: tp.Optional[tp.Callable[..., Kwargs]]
    use_threads: bool
    f_args: tp.List[str]
    namespace: utils.Namespace = field(
        default_factory=lambda: utils.Namespace(done=False, task_start_time=None)
    )
    process: tp.Optional[tp.Union[multiprocessing.Process, threading.Thread]] = None

    def __call__(self):

        worker_info = WorkerInfo(index=self.index)

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
                self,
                **{key: value for key, value in kwargs.items() if key in self.f_args},
            )

            self.stage_params.worker_done()

            if self.on_done is not None:

                kwargs.setdefault(
                    "stage_status",
                    StageStatus(
                        namespace=self.stage_params.namespace,
                        lock=self.stage_params.lock,
                    ),
                )

                self.on_done(
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in on_done_args
                    }
                )

            self.stage_params.output_queues.worker_done()

        except pypeln_utils.StopThreadException:
            pass
        except BaseException as e:
            self.main_queue.raise_exception(e)
            time.sleep(0.01)
        finally:
            self.done()

    def start(self):
        [self.process] = start_workers(self, use_threads=self.use_threads)

    def stop(self):
        if self.process is None:
            return

        if not self.process.is_alive():
            return

        if isinstance(self.process, multiprocessing.Process):
            self.process.terminate()
        else:
            stopit.async_raise(
                self.process.ident,
                pypeln_utils.StopThreadException,
            )

        with self.namespace:
            self.namespace.task_start_time = None

    def done(self):
        with self.namespace:
            self.namespace.done = True

    def did_timeout(self):
        with self.namespace:
            task_start_time = self.namespace.task_start_time
            done = self.namespace.done

        return (
            self.timeout
            and not done
            and task_start_time is not None
            and (time.time() - task_start_time > self.timeout)
        )

    @dataclass
    class MeasureTaskTime:
        worker: "Worker"

        def __enter__(self):
            with self.worker.namespace:
                self.worker.namespace.task_start_time = time.time()

        def __exit__(self, *args):
            with self.worker.namespace:
                self.worker.namespace.task_start_time = None

    def measure_task_time(self):
        return self.MeasureTaskTime(self)


class Applicable(pypeln_utils.Protocol):
    def apply(self, worker: "Worker", elem: tp.Any, **kwargs):
        ...


class ApplyProcess(ProcessFn, Applicable):
    def __call__(self, worker: Worker, **kwargs):
        for elem in worker.stage_params.input_queue:
            with worker.measure_task_time():
                self.apply(worker, elem, **kwargs)


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
        with self._namespace:
            return self._namespace.active_workers == 0

    @property
    def active_workers(self):
        """
        `int` : Number of active workers.
        """
        with self._namespace:
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
    use_threads: bool = False,
) -> tp.Union[tp.List[multiprocessing.Process], tp.List[threading.Thread]]:
    if kwargs is None:
        kwargs = {}

    workers = []

    for _ in range(n_workers):
        if use_threads:
            t = threading.Thread(target=target, args=args, kwargs=kwargs)
        else:
            t = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        workers.append(t)

    return workers
