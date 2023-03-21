import threading
import time
import typing as tp
from dataclasses import dataclass, field

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
    namespace: utils.Namespace

    @classmethod
    def create(
        cls, input_queue: IterableQueue, output_queues: OutputQueues, total_workers: int
    ) -> "StageParams":
        return cls(
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
    f_args: tp.List[str]
    namespace: utils.Namespace = field(
        default_factory=lambda: utils.Namespace(done=False, task_start_time=None)
    )
    process: tp.Optional[threading.Thread] = None

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
            try:
                self.main_queue.raise_exception(e)
                time.sleep(0.001)
            except pypeln_utils.StopThreadException:
                pass
        finally:
            self.namespace.done = True

    def start(self):
        [self.process] = start_workers(self)

    def stop(self):
        if self.process is None:
            return

        self.namespace.task_start_time = None

        if not self.process.is_alive():
            return

        stopit.async_raise(
            self.process.ident,
            pypeln_utils.StopThreadException,
        )

    def done(self):
        self.namespace.done = True

    def did_timeout(self):
        return (
            self.timeout
            and not self.namespace.done
            and self.namespace.task_start_time is not None
            and (time.time() - self.namespace.task_start_time > self.timeout)
        )

    @dataclass
    class MeasureTaskTime:
        worker: "Worker"

        def __enter__(self):
            self.worker.namespace.task_start_time = time.time()

        def __exit__(self, *args):
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

    def __init__(self, namespace):
        self._namespace = namespace

    @property
    def done(self) -> bool:
        """
        `bool` : `True` if all workers finished.
        """
        return self._namespace.active_workers == 0

    @property
    def active_workers(self):
        """
        `int` : Number of active workers.
        """
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
    use_threads: bool = True,
) -> tp.List[threading.Thread]:
    if kwargs is None:
        kwargs = {}

    workers = []

    for _ in range(n_workers):
        t = threading.Thread(target=target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        workers.append(t)

    return workers
