from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, function_args
from ..stage import Stage, ApplyProcess
import typing as tp
from dataclasses import dataclass
from .to_stage import to_stage


@dataclass
class Concat(ApplyProcess):
    def apply(self, worker: Stage, elem: tp.Any, **kwargs) -> tp.Iterable:
        yield elem


def concat(
    stages: tp.List[tp.Union[Stage[A], tp.Iterable[A]]], maxsize: int = 0
) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage in order, that is, it yields an element from the frist stage, then an element from the second stage and so on until it reaches the last stage and starts again. When a stage has no more elements its taken out of the process.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.sync.concat([stage_1, stage_2]) # [1, 4, 2, 5, 3, 6, 7]
    ```

    Arguments:
        stages: A list of Stage or Iterable.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        A stage object.
    """

    dependencies = [to_stage(stage, maxsize=maxsize) for stage in stages]

    return Stage(
        process_fn=Concat(),
        timeout=0,
        dependencies=dependencies,
        on_start=None,
        on_done=None,
        f_args=[],
    )

