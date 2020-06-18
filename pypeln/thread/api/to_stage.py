import typing as tp

from pypeln.utils import A

from ..stage import Stage
from .from_iterable import from_iterable


def to_stage(obj: tp.Union[Stage[A], tp.Iterable[A]]) -> Stage[A]:

    if isinstance(obj, Stage):
        return obj
    else:
        return from_iterable(obj)

