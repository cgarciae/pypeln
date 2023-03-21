import functools
import typing as tp
from dataclasses import dataclass

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..stage import Stage
from ..worker import ApplyProcess, ProcessFn, Worker
from .concat import concat
from .to_iterable import to_iterable
from .to_stage import to_stage


def run(
    *stages: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]], maxsize: int = 0
) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.process.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.process.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    """

    if len(stages) == 0:
        return
    elif len(stages) == 1:
        stage = to_iterable(stages[0], maxsize=maxsize)
    else:
        stage = concat(list(stages), maxsize=maxsize)

    for _ in stage:
        pass
