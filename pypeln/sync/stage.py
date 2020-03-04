import inspect
import sys
import traceback
from collections import namedtuple
from queue import Queue
from threading import Lock
from threading import Thread

from pypeln import utils as pypeln_utils
from . import utils


class Stage:
    def __init__(
        self, f, on_start, on_done, dependencies,
    ):

        self.f = f
        self.on_start = on_start
        self.on_done = on_done
        self.dependencies = dependencies

    def iter_dependencies(self):

        iterators = [iter(dependency) for dependency in self.dependencies]

        while len(iterators) > 0:
            for iterator in tuple(iterators):
                try:
                    yield next(iterator)
                except StopIteration:
                    iterators.remove(iterator)

    def process(self, **kwargs) -> None:
        for x in self.iter_dependencies():
            yield from self.apply(x, **kwargs)

    def run(self):
        if self.on_start is not None:
            on_start_kwargs = {}

            if "worker_info" in inspect.getfullargspec(self.on_start).args:
                on_start_kwargs["worker_info"] = utils.WorkerInfo(index=0)

            kwargs = self.on_start(**on_start_kwargs)
        else:
            kwargs = {}

        if kwargs is None:
            kwargs = {}

        yield from self.process(**kwargs)

        if self.on_done is not None:

            if "stage_status" in inspect.getfullargspec(self.on_done).args:
                kwargs["stage_status"] = utils.StageStatus()

            self.on_done(**kwargs)

    def __iter__(self):
        return self.to_iterable(maxsize=0)

    def to_iterable(self, maxsize):
        return self.run()

    def __or__(self, f):
        return f(self)
