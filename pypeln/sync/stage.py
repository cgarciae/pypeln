import inspect
import sys
import traceback
from collections import namedtuple
from queue import Queue
from threading import Lock, Thread

import stopit

from pypeln import utils as pypeln_utils

from . import utils


class Stage(pypeln_utils.BaseStage):
    def __init__(self, f, on_start, on_done, dependencies, timeout):

        self.f = f
        self.on_start = on_start
        self.on_done = on_done
        self.timeout = timeout
        self.dependencies = dependencies
        self.f_args = pypeln_utils.function_args(self.f) if self.f else set()
        self.on_start_args = (
            pypeln_utils.function_args(self.on_start) if self.on_start else set()
        )
        self.on_done_args = (
            pypeln_utils.function_args(self.on_done) if self.on_done else set()
        )

    def iter_dependencies(self):

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

    def process(self, **kwargs) -> None:
        for x in self.iter_dependencies():
            with (
                stopit.ThreadingTimeout(self.timeout)
                if self.timeout
                else utils.NoOpContext()
            ):
                yield from self.apply(x, **kwargs)

    def run(self):
        worker_info = pypeln_utils.WorkerInfo(index=0)

        if self.on_start is not None:
            on_start_kwargs = dict(worker_info=worker_info)
            kwargs = self.on_start(
                **{
                    key: value
                    for key, value in on_start_kwargs.items()
                    if key in self.on_start_args
                }
            )
        else:
            kwargs = {}

        if kwargs is None:
            kwargs = {}

        kwargs.setdefault("worker_info", worker_info)

        yield from self.process(
            **{key: value for key, value in kwargs.items() if key in self.f_args}
        )

        if self.on_done is not None:

            kwargs.setdefault(
                "stage_status", utils.StageStatus(),
            )

            self.on_done(
                **{
                    key: value
                    for key, value in kwargs.items()
                    if key in self.on_done_args
                }
            )

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)

    def to_iterable(self, maxsize, return_index):
        for elem in self.run():
            if return_index:
                yield elem
            else:
                yield elem.value
