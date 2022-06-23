import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import T

from ..stage import Stage


class FromIterable(tp.NamedTuple):
    iterable: tp.Iterable
    maxsize: int

    def __call__(self, worker: Stage, **kwargs) -> tp.Iterable:
        if isinstance(self.iterable, pypeln_utils.BaseStage):
            yield from self.iterable.to_iterable(
                maxsize=self.maxsize, return_index=True
            )
        else:
            for i, x in enumerate(self.iterable):
                if isinstance(x, pypeln_utils.Element):
                    yield x
                else:
                    yield pypeln_utils.Element(index=(i,), value=x)


@tp.overload
def from_iterable(
    iterable: tp.Iterable[T], use_thread: bool = True, maxsize: int = 0
) -> Stage[T]:
    ...


@tp.overload
def from_iterable(
    use_thread: bool = True, maxsize: int = 0
) -> pypeln_utils.Partial[Stage[T]]:
    ...


def from_iterable(
    iterable: tp.Union[tp.Iterable[T], pypeln_utils.Undefined] = pypeln_utils.UNDEFINED,
    use_thread: bool = True,
    maxsize: int = 0,
) -> tp.Union[Stage[T], pypeln_utils.Partial[Stage[T]]]:
    """
    Creates a stage from an iterable.

    Arguments:
        iterable: A source Iterable.
        use_thread: This parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        Returns a `Stage` if the `iterable` parameters is given, else it returns a `Partial`.
    """

    if isinstance(iterable, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda iterable: from_iterable(iterable))

    return Stage(
        process_fn=FromIterable(iterable, maxsize=maxsize),
        timeout=0,
        dependencies=[],
        on_start=None,
        on_done=None,
        f_args=[],
    )
