import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, function_args
from ..stage import Stage, ApplyProcess
import typing as tp
from dataclasses import dataclass
from .to_stage import to_stage


class MapFn(tp.Protocol):
    def __call__(self, A, **kwargs) -> B:
        ...


@dataclass
class Map(ApplyProcess):
    f: MapFn

    def apply(self, worker: Stage, elem: tp.Any, **kwargs) -> tp.Iterable:

        if "element_index" in worker.f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)
        yield elem.set(y)


@tp.overload
def map(
    f: MapFn,
    stage: tp.Union[Stage[A], tp.Iterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> Stage[B]:
    ...


@tp.overload
def map(
    f: MapFn,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> pypeln_utils.Partial[Stage[B]]:
    ...


def map(
    f: MapFn,
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
    Creates a stage that maps a function `f` over the data. Its should behave exactly like python's built-in `map` function.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.sync.map(slow_add1, data, workers=3, maxsize=4)

    data = list(stage) # [1, 2, 3, ..., 10]
    ```

    Arguments:
        f: A function with the signature `f(x) -> y`. `f` can accept special additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
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
            lambda stage: map(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage_ = to_stage(stage)

    return Stage(
        process_fn=Map(f),
        timeout=timeout,
        dependencies=[stage_],
        on_start=on_start,
        on_done=on_done,
        f_args=pypeln_utils.function_args(f),
    )

