"""
The `process` module lets you create pipelines using objects from python's [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need of true parallelism for CPU heavy operations but be aware of its implications.
"""

from threading import Thread
import time
import typing
import typing as tp
import asyncio

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue
from .stage import Stage
from .worker import Kwargs, StageParams, Worker, TaskPool

T = tp.TypeVar("T")
A = tp.TypeVar("A")
B = tp.TypeVar("B")


class ApplyWorkerConstructor(WorkerApply[T]):
    @classmethod
    def get_worker_constructor(
        cls,
        f: tp.Callable,
        timeout: float,
        on_start: tp.Optional[tp.Callable[..., Kwargs]],
        on_done: tp.Optional[tp.Callable[..., Kwargs]],
        max_tasks: int,
    ) -> WorkerConstructor:
        def worker_constructor(
            stage_params: StageParams, main_queue: IterableQueue
        ) -> Worker[T]:
            return cls.create(
                f=f,
                stage_params=stage_params,
                main_queue=main_queue,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
                max_tasks=max_tasks,
            )

        return worker_constructor


@tp.overload
def from_iterable(
    iterable: tp.Union[tp.Iterable[T], tp.AsyncIterable[T]],
    maxsize: int = 0,
    use_thread: bool = True,
) -> Stage[T]:
    ...


# ----------------------------------------------------------------
# map
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# flat_map
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# filter
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# each
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# concat
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# ordered
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# run
# ----------------------------------------------------------------


# ----------------------------------------------------------------
# to_iterable
# ----------------------------------------------------------------
