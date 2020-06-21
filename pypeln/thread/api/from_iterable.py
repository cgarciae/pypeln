import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..stage import Stage
from ..worker import ProcessFn, Worker


class FromIterable(tp.NamedTuple):
    iterable: tp.Iterable

    def __call__(self, worker: Worker, **kwargs):

        if isinstance(self.iterable, pypeln_utils.BaseStage):

            for x in self.iterable.to_iterable(maxsize=0, return_index=True):
                worker.stage_params.output_queues.put(x)
        else:
            for i, x in enumerate(self.iterable):
                if isinstance(x, pypeln_utils.Element):
                    worker.stage_params.output_queues.put(x)
                else:
                    worker.stage_params.output_queues.put(
                        pypeln_utils.Element(index=(i,), value=x)
                    )


@tp.overload
def from_iterable(iterable: tp.Iterable[T], use_thread: bool = True) -> Stage[T]:
    ...


@tp.overload
def from_iterable(use_thread: bool = True) -> pypeln_utils.Partial[Stage[T]]:
    ...


def from_iterable(
    iterable: tp.Union[tp.Iterable[T], pypeln_utils.Undefined] = pypeln_utils.UNDEFINED,
    use_thread: bool = True,
) -> tp.Union[Stage[T], pypeln_utils.Partial[Stage[T]]]:
    """
    Creates a stage from an iterable. This function gives you more control of the iterable is consumed.
    Arguments:
        iterable: A source Iterable.
        use_thread: This parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        Returns a `Stage` if the `iterable` parameters is given, else it returns a `Partial`.
    """

    if isinstance(iterable, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda iterable: from_iterable(iterable, use_thread=use_thread)
        )

    return Stage(
        process_fn=FromIterable(iterable),
        workers=1,
        maxsize=0,
        timeout=0,
        total_sources=1,
        dependencies=[],
        on_start=None,
        on_done=None,
        f_args=[],
    )
