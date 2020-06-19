import typing as tp

from pypeln.utils import A

from ..stage import Stage
from .from_iterable import from_iterable


def to_stage(obj: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]]) -> Stage[A]:

    if isinstance(obj, Stage):
        return obj

    elif isinstance(obj, tp.Iterable) or isinstance(obj, tp.AsyncIterable):
        return from_iterable(obj)

    else:
        raise ValueError(f"Object {obj} is not a Stage or iterable")
