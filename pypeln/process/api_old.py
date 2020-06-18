"""
The `process` module lets you create pipelines using objects from python's [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need of true parallelism for CPU heavy operations but be aware of its implications.
"""

import typing
from threading import Thread

from pypeln import utils as pypeln_utils
from pypeln.utils import T, A, B

from . import utils
from .worker import Worker, WorkerApply, StageParams, Kwargs
from .queue import IterableQueue
from .stage import Stage, WorkerConstructor
import typing as tp


# ----------------------------------------------------------------
# flat_map
# ----------------------------------------------------------------



# ----------------------------------------------------------------
# filter
# ----------------------------------------------------------------



# ----------------------------------------------------------------
# each
# ----------------------------------------------------------------


class Each(ApplyWorkerConstructor[T]):
    def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):
        if "element_index" in f_args:
            kwargs["element_index"] = elem.index

        self.f(elem.value, **kwargs)


@tp.overload
def each(
    f: typing.Callable[..., B],
    stage: tp.Union[Stage[A], tp.Iterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
    run: bool = False,
) -> tp.Optional[Stage[B]]:
    ...


@tp.overload
def each(
    f: typing.Callable[..., B],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
    run: bool = False,
) -> pypeln_utils.Partial[tp.Optional[Stage[B]]]:
    ...


def each(
    f: typing.Callable,
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
    run: bool = False,
) -> tp.Union[tp.Optional[Stage[B]], pypeln_utils.Partial[tp.Optional[Stage[B]]]]:
    """
    Creates a stage that runs the function `f` for each element in the data but the stage itself yields no elements. Its useful for sink stages that perform certain actions such as writting to disk, saving to a database, etc, and dont produce any results. For example:

    ```python
    import pypeln as pl

    def process_image(image_path):
        image = load_image(image_path)
        image = transform_image(image)
        save_image(image_path, image)

    files_paths = get_file_paths()
    stage = pl.process.each(process_image, file_paths, workers=4)
    pl.process.run(stage)

    ```

    or alternatively

    ```python
    files_paths = get_file_paths()
    pl.process.each(process_image, file_paths, workers=4, run=True)
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `f(x) -> None`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        run: Whether or not to execute the stage immediately.

    Returns:
        If the `stage` parameters is not given then this function returns a `Partial`, else if `run=False` (default) it return a new stage, if `run=True` then it runs the stage and returns `None`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: each(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    stage = Stage.create(
        workers=workers,
        total_sources=stage.workers,
        maxsize=maxsize,
        worker_constructor=Each.get_worker_constructor(
            f=f, timeout=timeout, on_start=on_start, on_done=on_done
        ),
        dependencies=[stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass


# ----------------------------------------------------------------
# concat
# ----------------------------------------------------------------


class Concat(ApplyWorkerConstructor[T]):
    def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):
        self.stage_params.output_queues.put(elem)


def concat(
    stages: typing.List[tp.Union[Stage[A], tp.Iterable[A]]], maxsize: int = 0
) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.process.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]
    ```

    Arguments:
        stages: a list of stages or iterables.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        A stage object.
    """

    stages = [to_stage(stage) for stage in stages]

    return Stage.create(
        workers=1,
        total_sources=sum(stage.workers for stage in stages),
        maxsize=maxsize,
        worker_constructor=Concat.get_worker_constructor(
            f=pypeln_utils.no_op, timeout=0, on_start=None, on_done=None
        ),
        dependencies=stages,
    )


# ----------------------------------------------------------------
# ordered
# ----------------------------------------------------------------


class Ordered(Worker[T]):
    def __init__(
        self, index: int, stage_params: StageParams, main_queue: IterableQueue,
    ):
        super().__init__(
            f=pypeln_utils.no_op,
            index=index,
            stage_params=stage_params,
            main_queue=main_queue,
        )

    @classmethod
    def get_worker_constructor(cls) -> WorkerConstructor:
        def from_iterable(
            index: int, stage_params: StageParams, main_queue: IterableQueue
        ) -> Ordered:
            return cls(index=index, stage_params=stage_params, main_queue=main_queue)

        return from_iterable

    def process_fn(self, f_args: tp.List[str], **kwargs):

        elems = []

        for elem in self.stage_params.input_queue:
            if self.main_queue.namespace.exception:
                return

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
            self.stage_params.output_queues.put(elems.pop(0))


@tp.overload
def ordered(stage: Stage[A], maxsize: int = 0) -> Stage[A]:
    ...


@tp.overload
def ordered(maxsize: int = 0) -> pypeln_utils.Partial[Stage[A]]:
    ...


def ordered(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
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
        stage: A stage object.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(lambda stage: ordered(stage, maxsize=maxsize))

    stage = to_stage(stage)

    return Stage.create(
        workers=1,
        total_sources=stage.workers,
        maxsize=maxsize,
        worker_constructor=Ordered.get_worker_constructor(),
        dependencies=[stage],
    )


# ----------------------------------------------------------------
# run
# ----------------------------------------------------------------


def run(*stages: tp.Union[Stage[A], tp.Iterable[A]], maxsize: int = 0) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.process.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.process.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    """

    if len(stages) > 0:
        stage = concat(list(stages), maxsize=maxsize)

    else:
        stage = stages[0]

    stage = to_iterable(stage, maxsize=maxsize)

    for _ in stage:
        pass


# ----------------------------------------------------------------
# to_iterable
# ----------------------------------------------------------------


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
