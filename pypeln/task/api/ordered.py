import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..stage import Stage
from ..worker import ProcessFn, Worker
from .to_stage import to_stage


class Ordered(tp.NamedTuple):
    async def __call__(self, worker: Worker, **kwargs):

        elems = []

        async for elem in worker.stage_params.input_queue:

            if len(elems) == 0:
                elems.append(elem)
            else:
                for i in reversed(range(len(elems))):
                    if elem.index >= elems[i].index:
                        elems.insert(i + 1, elem)
                        break

                    if i == 0:
                        elems.insert(0, elem)

        for _ in range(len(elems)):
            await worker.stage_params.output_queues.put(elems.pop(0))


@tp.overload
def ordered(
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
) -> Stage[A]:
    ...


@tp.overload
def ordered() -> pypeln_utils.Partial[Stage[A]]:
    ...


def ordered(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
) -> tp.Union[Stage[A], pypeln_utils.Partial[Stage[A]]]:
    """
    Creates a stage that sorts its elements based on their order of creation on the source iterable(s) of the pipeline.

    ```python
    import pypeln as pl
    import random
    import time

    def slow_squared(x):
        time.sleep(random.random())
        
        return x ** 2

    stage = range(5)
    stage = pl.process.map(slow_squared, stage, workers = 2)
    stage = pl.process.ordered(stage)

    print(list(stage)) # [0, 1, 4, 9, 16]
    ```

    !!! note
        `ordered` will work even if the previous stages are from different `pypeln` modules, but it may not work if you introduce an itermediate external iterable stage.
    
    !!! warning
        This stage will not yield util it accumulates all of the elements from the previous stage, use this only if all elements fit in memory.

    Arguments:
        stage: A Stage, Iterable, or AsyncIterable.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda stage: ordered(stage))

    stage = to_stage(stage)

    return Stage(
        process_fn=Ordered(),
        workers=1,
        maxsize=0,
        timeout=0,
        total_sources=1,
        dependencies=[stage],
        on_start=None,
        on_done=None,
        f_args=[],
    )
