from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, function_args
from ..stage import Stage, ApplyProcess
import typing as tp
from dataclasses import dataclass
from .to_stage import to_stage


class FlatMapFn(pypeln_utils.Protocol):
    def __call__(self, A, **kwargs) -> tp.Iterable[B]:
        ...


@dataclass
class FlatMap(ApplyProcess):
    f: FlatMapFn

    def apply(self, worker: Stage, elem: tp.Any, **kwargs) -> tp.Iterable:
        if "element_index" in worker.f_args:
            kwargs["element_index"] = elem.index

        for i, y in enumerate(self.f(elem.value, **kwargs)):
            yield pypeln_utils.Element(index=elem.index + (i,), value=y)


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
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.sync.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

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
    stage = pl.sync.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # [0, 1, -1, 2, -2, ..., 9, -9]
    ```

        
    `flat_map` is a more general operation, you can actually implement `pypeln.sync.map` and `pypeln.sync.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.sync.map(f, stage) = pl.sync.flat_map(lambda x: [f(x)], stage)
    pl.sync.filter(f, stage) = pl.sync.flat_map(lambda x: [x] if f(x) else [], stage)
    ```

    Using `flat_map` with a generator function is very useful as e.g. you are able to filter out unwanted elements when there are exceptions, missing data, etc.

    Arguments:
        f: A function with signature `f(x) -> iterable`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A Stage or Iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    !!! warning
        To implement `timeout` we use `stopit.ThreadingTimeout` which has some limitations.

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

    stage_ = to_stage(stage, maxsize=maxsize)

    return Stage(
        process_fn=FlatMap(f),
        timeout=timeout,
        dependencies=[stage_],
        on_start=on_start,
        on_done=on_done,
        f_args=pypeln_utils.function_args(f),
    )
