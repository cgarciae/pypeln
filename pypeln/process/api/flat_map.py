import typing as tp
import functools
from dataclasses import dataclass

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from .to_stage import to_stage
from ..stage import Stage
from ..worker import ProcessFn, Worker, ApplyProcess


class FlatMapFn(pypeln_utils.Protocol):
    def __call__(self, A, **kwargs) -> tp.Iterable[B]:
        ...


@dataclass
class FlatMap(ApplyProcess):
    f: FlatMapFn

    def apply(self, worker: Worker, elem: tp.Any, **kwargs):

        if "element_index" in worker.f_args:
            kwargs["element_index"] = elem.index

        for i, y in enumerate(self.f(elem.value, **kwargs)):
            elem_y = pypeln_utils.Element(index=elem.index + (i,), value=y)
            worker.stage_params.output_queues.put(elem_y)


@tp.overload
def flat_map(
    f: FlatMapFn,
    stage: Stage[A],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> Stage[B]:
    ...


@tp.overload
def flat_map(
    f: FlatMapFn,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> pypeln_utils.Partial[Stage[B]]:
    ...


def flat_map(
    f: FlatMapFn,
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> tp.Union[Stage[B], pypeln_utils.Partial[Stage[B]]]:
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.process.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_integer_pair(x):
        time.sleep(random()) # <= some slow computation

        if x == 0:
            yield x
        else:
            yield x
            yield -x

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.process.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    `flat_map` is a more general operation, you can actually implement `pypeln.process.map` and `pypeln.process.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.process.map(f, stage) = pl.process.flat_map(lambda x: [f(x)], stage)
    pl.process.filter(f, stage) = pl.process.flat_map(lambda x: [x] if f(x) else [], stage)
    ```

    Using `flat_map` with a generator function is very useful as e.g. you are able to filter out unwanted elements when there are exceptions, missing data, etc.

    Arguments:
        f: A function with signature `f(x) -> iterable`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A Stage or Iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded.
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    Returns:
        Returns a `Stage` if the `stage` parameters is given, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: flat_map(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage, maxsize=maxsize)

    return Stage(
        process_fn=FlatMap(f),
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        total_sources=stage.workers,
        dependencies=[stage],
        on_start=on_start,
        on_done=on_done,
        use_threads=False,
        f_args=pypeln_utils.function_args(f),
    )
