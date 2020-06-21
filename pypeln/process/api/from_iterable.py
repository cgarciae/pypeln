import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..stage import Stage
from ..worker import ProcessFn, Worker
from copy import copy


@tp.runtime_checkable
class GeneratorFn(tp.Protocol):
    def __call__(self) -> tp.Union[tp.Iterable]:
        ...


class FromIterable(tp.NamedTuple):
    iterable: tp.Iterable

    def __call__(self, worker: Worker, **kwargs):

        iterable = self.iterable

        if isinstance(iterable, pypeln_utils.BaseStage):

            for x in iterable.to_iterable(maxsize=0, return_index=True):
                worker.stage_params.output_queues.put(x)
        else:
            for i, x in enumerate(iterable):
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
):
    """
    Creates a stage from an iterable.

    Arguments:
        iterable: A source Iterable.
        use_thread: If set to `True` (default) it will use a thread instead of a process to consume the iterable. Threads start faster and use thread memory to the iterable is not serialized, however, if the iterable is going to perform slow computations it better to use a process.

    Returns:
        If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
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
        use_threads=use_thread,
        f_args=[],
    )
