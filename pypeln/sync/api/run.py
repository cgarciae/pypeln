import typing as tp

from ..stage import Stage
from .to_iterable import to_iterable


def run(stages: tp.List[Stage], maxsize: int = 0) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.sync.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.sync.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.

    """

    if isinstance(stages, list) and len(stages) == 0:
        raise ValueError("Expected at least 1 stage to run")

    elif isinstance(stages, list):
        stage = concat(stages, maxsize=maxsize)

    else:
        stage = stages

    stage = to_iterable(stage, maxsize=maxsize)

    for _ in stages:
        pass
