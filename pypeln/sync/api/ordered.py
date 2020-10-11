import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import T, A
from .to_stage import to_stage
from ..stage import Stage, ProcessFn
from dataclasses import dataclass


@dataclass
class Ordered(ProcessFn):
    def __call__(self, worker: Stage, **kwargs) -> tp.Iterable:
        return sorted(worker.iter_dependencies(), key=lambda elem: elem.index)


@tp.overload
def ordered(stage: Stage[A]) -> Stage[A]:
    ...


@tp.overload
def ordered() -> pypeln_utils.Partial[Stage[A]]:
    ...


def ordered(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
) -> tp.Union[Stage[A], pypeln_utils.Partial[Stage[A]]]:
    """
    Creates a stage that sorts its elements based on their order of creation on the source iterable(s) of the pipeline.

    ```python
    import pypeln as pl
    import random
    import time

    def slow_squared(x):
        time.sleep(random.random())
        
        return x ** 2

    stage = range(5)
    stage = pl.thread.map(slow_squared, stage, workers = 2)
    stage = pl.sync.ordered(stage)

    print(list(stage)) # [0, 1, 4, 9, 16]
    ```

    Since `sync.map` preserves order, instead we used `thread.map` so this example made sense. 

    !!! note
        `ordered` will work even if the previous stages are from different `pypeln` modules, but it may not work if you introduce an itermediate external iterable stage.
    
    !!! warning
        This stage will not yield util it accumulates all of the elements from the previous stage, use this only if all elements fit in memory.

    Arguments:
        stage: A Stage or Iterable.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda stage: ordered(stage))

    stage_ = to_stage(stage, maxsize=maxsize)

    return Stage(
        process_fn=Ordered(),
        timeout=0,
        dependencies=[stage_],
        on_start=None,
        on_done=None,
        f_args=[],
    )
