import typing as tp

from pypeln.utils import A

from ..stage import Stage
from .concat import concat
from .to_iterable import to_iterable


def run(*stages: tp.Union[Stage[A], tp.Iterable[A]], maxsize: int = 0) -> None:
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

    if len(stages) == 0:
        return
    elif len(stages) == 1:
        stage = to_iterable(stages[0], maxsize=maxsize)
    else:
        stage = concat(list(stages), maxsize=maxsize)

    for _ in stage:
        pass
