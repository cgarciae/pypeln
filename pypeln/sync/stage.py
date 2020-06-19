import inspect
import sys
import traceback
from collections import namedtuple
from queue import Queue
from threading import Lock, Thread
import typing as tp

import stopit

from pypeln import utils as pypeln_utils
from pypeln.utils import T, Kwargs
from dataclasses import dataclass

from . import utils


class WorkerInfo(tp.NamedTuple):
    index: int


class ProcessFn(tp.Protocol):
    def __call__(self, worker: "Stage", **kwargs) -> tp.Iterable:
        ...


class ApplyFn(tp.Protocol):
    def __call__(self, worker: "Stage", elem: tp.Any, **kwargs) -> tp.Iterable:
        ...


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    process_fn: ProcessFn
    timeout: float
    dependencies: tp.List["Stage"]
    on_start: tp.Optional[tp.Callable[..., Kwargs]]
    on_done: tp.Optional[tp.Callable[..., Kwargs]]
    f_args: tp.List[str]

    def iter_dependencies(self) -> tp.Iterable[pypeln_utils.Element]:

        iterators = [
            iter(dependency.to_iterable(maxsize=0, return_index=True))
            for dependency in self.dependencies
        ]

        while len(iterators) > 0:
            for iterator in tuple(iterators):
                try:
                    yield next(iterator)
                except StopIteration:
                    iterators.remove(iterator)

    def run(self) -> tp.Iterable:

        worker_info = WorkerInfo(index=0)

        on_start_args: tp.List[str] = (
            pypeln_utils.function_args(self.on_start) if self.on_start else []
        )
        on_done_args: tp.List[str] = (
            pypeln_utils.function_args(self.on_done) if self.on_done else []
        )

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

        yield from self.process_fn(
            self, **{key: value for key, value in kwargs.items() if key in self.f_args},
        )

        if self.on_done is not None:

            kwargs.setdefault(
                "stage_status", StageStatus(),
            )

            self.on_done(
                **{key: value for key, value in kwargs.items() if key in on_done_args}
            )

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)

    def to_iterable(self, maxsize, return_index) -> tp.Iterable:
        for elem in self.run():
            if return_index:
                yield elem
            else:
                yield elem.value


class Applicable(tp.Protocol):
    def apply(self, worker: Stage, elem: tp.Any, **kwargs) -> tp.Iterable:
        ...


class ApplyProcess(ProcessFn, Applicable):
    def __call__(self, worker: Stage, **kwargs):
        for x in worker.iter_dependencies():
            with (
                stopit.ThreadingTimeout(worker.timeout)
                if worker.timeout
                else utils.NoOpContext()
            ):
                yield from self.apply(worker, x, **kwargs)


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
