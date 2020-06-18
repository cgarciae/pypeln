import typing as tp
import functools
from dataclasses import dataclass

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from .to_iterable import to_iterable
from .to_stage import to_stage
from .concat import concat
from ..stage import Stage
from ..worker import ProcessFn, Worker, ApplyProcess


def run(*stages: tp.Union[Stage[A], tp.Iterable[A]], maxsize: int = 0) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.thread.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.thread.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    """

    if len(stages) > 0:
        stage = concat(list(stages), maxsize=maxsize)

    else:
        stage = stages[0]

    stage = to_iterable(stage, maxsize=maxsize)

    for _ in stage:
        pass

