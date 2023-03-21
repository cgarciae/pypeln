import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A

from ..stage import Stage
from .from_iterable import from_iterable


@tp.overload
def to_iterable(
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Iterable[A]:
    ...


@tp.overload
def to_iterable(
    maxsize: int = 0,
    return_index: bool = False,
) -> pypeln_utils.Partial[tp.Iterable[A]]:
    ...


def to_iterable(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Union[tp.Iterable[A], pypeln_utils.Partial[tp.Iterable[A]]]:
    """
    Creates an iterable from a stage. Use this function to when you want to have more control over how the output stage is consumed, especifically, setting the `maxsize` argument can help you avoid OOM error if the consumer is slow.

    Arguments:
        stage: A Stage, Iterable, or AsyncIterable.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        return_index: When set to `True` the resulting iterable will yield the `Elemen(index: Tuple[int, ...], value: Any)` which contains both the resulting value and the index parameter which holds information about the order of creation of the elements at the source.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda stage: to_iterable(stage, maxsize=maxsize))

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize, return_index=return_index)
    elif isinstance(stage, tp.Iterable[A]):
        return stage
    else:
        iterable = from_iterable(stage).to_iterable(
            maxsize=maxsize, return_index=return_index
        )

    return iterable


@tp.overload
def to_async_iterable(
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.AsyncIterable[A]:
    ...


@tp.overload
def to_async_iterable(
    maxsize: int = 0,
    return_index: bool = False,
) -> pypeln_utils.Partial[tp.AsyncIterable[A]]:
    ...


def to_async_iterable(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Union[tp.AsyncIterable[A], pypeln_utils.Partial[tp.AsyncIterable[A]]]:
    """
    Creates an iterable from a stage.

    Arguments:
        stage: A Stage, Iterable, or AsyncIterable.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: to_async_iterable(stage, maxsize=maxsize)
        )

    if isinstance(stage, Stage):
        iterable = stage.to_async_iterable(maxsize=maxsize, return_index=return_index)

    elif isinstance(stage, tp.AsyncIterable[A]):
        return stage
    else:
        iterable = from_iterable(stage, maxsize=maxsize).to_async_iterable(
            maxsize=maxsize, return_index=return_index
        )

    return iterable
