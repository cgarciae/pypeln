import typing as tp

from pypeln.utils import A

from ..stage import Stage
from pypeln import utils as pypeln_utils


@tp.overload
def to_iterable(
    stage: tp.Union[Stage[A], tp.Iterable[A]],
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Iterable[A]:
    ...


@tp.overload
def to_iterable(
    maxsize: int = 0, return_index: bool = False,
) -> pypeln_utils.Partial[tp.Iterable[A]]:
    ...


def to_iterable(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Union[tp.Iterable[A], pypeln_utils.Partial[tp.Iterable[A]]]:
    """
    Creates an iterable from a stage.

    Arguments:
        stage: A stage object.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda stage: to_iterable(stage, maxsize=maxsize))

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize, return_index=return_index)
    else:
        iterable = stage

    return iterable
