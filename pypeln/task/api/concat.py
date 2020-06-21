import typing as tp
import functools
from dataclasses import dataclass

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from .to_stage import to_stage
from ..stage import Stage
from ..worker import ProcessFn, Worker, ApplyProcess


@dataclass
class Concat(ApplyProcess):
    async def apply(self, worker: Worker, elem: tp.Any, **kwargs):
        await worker.stage_params.output_queues.put(elem)


def concat(
    stages: tp.List[tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]]],
    maxsize: int = 0,
) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.process.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]
    ```

    Arguments:
        stages: A list of Stage, Iterable or AsyncIterable.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        A stage object.
    """

    dependencies: tp.List[Stage[A]] = [to_stage(stage) for stage in stages]

    return Stage(
        process_fn=Concat(),
        workers=1,
        maxsize=maxsize,
        timeout=0,
        total_sources=len(dependencies),
        dependencies=dependencies,
        on_start=None,
        on_done=None,
        f_args=[],
    )
