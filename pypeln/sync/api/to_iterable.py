from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, function_args
from ..stage import Stage, ApplyProcess
import typing as tp
from dataclasses import dataclass
from .to_stage import to_stage


@tp.overload
def to_iterable(
    stage: tp.Union[Stage[A], tp.Iterable[A]],
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
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Union[tp.Iterable[A], pypeln_utils.Partial[tp.Iterable[A]]]:
    """
    Creates an iterable from a stage. Use this function to when you want to have more control over how the output stage is consumed, especifically, setting the `maxsize` argument can help you avoid OOM error if the consumer is slow.

    Arguments:
        stage: A Stage or Iterable.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        return_index: When set to `True` the resulting iterable will yield the `Elemen(index: Tuple[int, ...], value: Any)` which contains both the resulting value and the index parameter which holds information about the order of creation of the elements at the source.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: to_iterable(stage, maxsize=maxsize, return_index=return_index)
        )

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize, return_index=return_index)
    else:
        iterable = stage

    return iterable
