"""
The `sync` module follows same API as the rest of the modules but runs the code synchronously using normal python generators. This module is intended to be used for debugging purposes as synchronous code tends to be easier to debug than concurrent code in Python (e.g. vscode's debugger doesn't work well (if at all) with the multiprocessing and threading modules).

Common arguments such as `workers` and `maxsize` are accepted by this module's 
functions for API compatibility purposes but are ignored.
"""
import typing
from threading import Thread

from pypeln import utils as pypeln_utils

from . import utils
from .stage import Stage

#############################################################
# from_iterable
#############################################################


class FromIterable(Stage):
    def __init__(self, iterable, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.iterable = iterable

    def process(self):
        yield from self.iterable


def from_iterable(
    iterable: typing.Iterable = pypeln_utils.UNDEFINED,
    maxsize: int = None,
    worker_constructor: typing.Type = None,
) -> Stage:
    """
    Creates a stage from an iterable.

    Arguments:
        iterable: a source iterable.
        maxsize: this parameter is not used and only kept for API compatibility with the other modules.
        worker_constructor: this parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(iterable):
        return pypeln_utils.Partial(
            lambda iterable: from_iterable(
                iterable, maxsize=None, worker_constructor=worker_constructor
            )
        )

    return FromIterable(
        iterable=iterable, f=None, on_start=None, on_done=None, dependencies=[],
    )


#############################################################
# to_stage
#############################################################


def to_stage(obj):

    if isinstance(obj, Stage):
        return obj

    elif hasattr(obj, "__iter__"):
        return from_iterable(obj)

    else:
        raise ValueError(f"Object {obj} is not a Stage or iterable")


#############################################################
# map
#############################################################


class Map(Stage):
    def apply(self, x, **kwargs):
        y = self.f(x, **kwargs)
        yield y


def map(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = None,
    maxsize: int = None,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that maps a function `f` over the data. Its should behave exactly like python's built-in `map` function.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.sync.map(slow_add1, data, workers=3, maxsize=4)

    data = list(stage) # [1, 2, 3, ..., 10]
    ```

    Arguments:
        f: A function with signature `f(x, **kwargs) -> y`, where `kwargs` is the return of `on_start` if present.
        stage: A stage or iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        on_start: A function with signature `on_start(worker_info?) -> kwargs`, where `kwargs` can be a `dict` of keyword arguments that will be passed to `f` and `on_done`. If you define a `worker_info` argument an object with information about the worker will be passed. This function is executed once per worker at the beggining.
        on_done: A function with signature `on_done(stage_status?, **kwargs)`, where `kwargs` is the return of `on_start` if present. If you define a `stage_status` argument an object with information about the stage will be passed. This function is executed once per worker when the worker finishes.

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(
            lambda stage: map(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return Map(f=f, on_start=on_start, on_done=on_done, dependencies=[stage],)


#############################################################
# flat_map
#############################################################


class FlatMap(Stage):
    def apply(self, x, **kwargs):
        yield from self.f(x, **kwargs)


def flat_map(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.sync.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_integer_pair(x):
        time.sleep(random()) # <= some slow computation

        if x == 0:
            yield x
        else:
            yield x
            yield -x

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.sync.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # [0, 1, -1, 2, -2, ..., 9, -9]
    ```

        
    `flat_map` is a more general operation, you can actually implement `pypeln.sync.map` and `pypeln.sync.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.sync.map(f, stage) = pl.sync.flat_map(lambda x: [f(x)], stage)
    pl.sync.filter(f, stage) = pl.sync.flat_map(lambda x: [x] if f(x) else [], stage)
    ```

    Using `flat_map` with a generator function is very useful as e.g. you are able to filter out unwanted elements when there are exceptions, missing data, etc.

    Arguments:
        f: A function with signature `f(x, **kwargs) -> Iterable`, where `kwargs` is the return of `on_start` if present.
        stage: A stage or iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        on_start: A function with signature `on_start(worker_info?) -> kwargs`, where `kwargs` can be a `dict` of keyword arguments that will be passed to `f` and `on_done`. If you define a `worker_info` argument an object with information about the worker will be passed. This function is executed once per worker at the beggining.
        on_done: A function with signature `on_done(stage_status?, **kwargs)`, where `kwargs` is the return of `on_start` if present. If you define a `stage_status` argument an object with information about the stage will be passed. This function is executed once per worker when the worker finishes.

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(
            lambda stage: flat_map(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return FlatMap(f=f, on_start=on_start, on_done=on_done, dependencies=[stage],)


#############################################################
# filter
#############################################################


class Filter(Stage):
    def apply(self, x, **kwargs):
        if self.f(x, **kwargs):
            yield x


def filter(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that filter the data given a predicate function `f`. exactly like python's built-in `filter` function.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_gt3(x):
        time.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.sync.filter(slow_gt3, data, workers=3, maxsize=4)

    data = list(stage) # [3, 4, 5, ..., 9]
    ```

    Arguments:
        f: A function with signature `f(x, **kwargs) -> bool`, where `kwargs` is the return of `on_start` if present.
        stage: A stage or iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        on_start: A function with signature `on_start(worker_info?) -> kwargs`, where `kwargs` can be a `dict` of keyword arguments that will be passed to `f` and `on_done`. If you define a `worker_info` argument an object with information about the worker will be passed. This function is executed once per worker at the beggining.
        on_done: A function with signature `on_done(stage_status?, **kwargs)`, where `kwargs` is the return of `on_start` if present. If you define a `stage_status` argument an object with information about the stage will be passed. This function is executed once per worker when the worker finishes.

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(
            lambda stage: filter(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return Filter(f=f, on_start=on_start, on_done=on_done, dependencies=[stage],)


#############################################################
# each
#############################################################


class Each(Stage):
    def apply(self, x, **kwargs):
        self.f(x, **kwargs)


def each(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
    run: bool = False,
) -> Stage:
    """
    Creates a stage that runs the function `f` for each element in the data but the stage itself yields no elements. Its useful for sink stages that perform certain actions such as writting to disk, saving to a database, etc, and dont produce any results. For example:

    ```python
    import pypeln as pl

    def process_image(image_path):
        image = load_image(image_path)
        image = transform_image(image)
        save_image(image_path, image)

    files_paths = get_file_paths()
    stage = pl.sync.each(process_image, file_paths, workers=4)
    pl.sync.run(stage)

    ```

    or alternatively

    ```python
    files_paths = get_file_paths()
    pl.sync.each(process_image, file_paths, workers=4, run=True)
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `f(x, **kwargs) -> None`, where `kwargs` is the return of `on_start` if present.
        stage: A stage or iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        on_start: A function with signature `on_start(worker_info?) -> kwargs`, where `kwargs` can be a `dict` of keyword arguments that will be passed to `f` and `on_done`. If you define a `worker_info` argument an object with information about the worker will be passed. This function is executed once per worker at the beggining.
        on_done: A function with signature `on_done(stage_status?, **kwargs)`, where `kwargs` is the return of `on_start` if present. If you define a `stage_status` argument an object with information about the stage will be passed. This function is executed once per worker when the worker finishes.
        run: Whether or not to execute the stage immediately.

    Returns:
        If the `stage` parameters is not given then this function returns a `Partial`, else if `run=False` (default) it return a new stage, if `run=True` then it runs the stage and returns `None`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(
            lambda stage: each(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    stage = Each(f=f, on_start=on_start, on_done=on_done, dependencies=[stage],)

    if not run:
        return stage

    for _ in stage:
        pass


#############################################################
# concat
#############################################################


class Concat(Stage):
    def apply(self, x):
        yield x


def concat(stages: typing.List[Stage], maxsize: int = 0) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage in order, that is, it yields an element from the frist stage, then an element from the second stage and so on until it reaches the last stage and starts again. When a stage has no more elements its taken out of the process.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.sync.concat([stage_1, stage_2]) # [1, 4, 2, 5, 3, 6, 7]
    ```

    Arguments:
        stages: a list of stages or iterables.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        A stage object.
    """

    stages = [to_stage(stage) for stage in stages]

    return Concat(f=None, on_start=None, on_done=None, dependencies=stages,)


#############################################################
# run
#############################################################


def run(stages: typing.List[Stage], maxsize: int = 0) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.sync.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.sync.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.

    """

    if isinstance(stages, list) and len(stages) == 0:
        raise ValueError("Expected at least 1 stage to run")

    elif isinstance(stages, list):
        stage = concat(stages, maxsize=maxsize)

    else:
        stage = stages

    stage = to_iterable(stage, maxsize=maxsize)

    for _ in stages:
        pass


#############################################################
# to_iterable
#############################################################


def to_iterable(
    stage: Stage = pypeln_utils.UNDEFINED, maxsize: int = 0
) -> typing.Iterable:
    """
    Creates an iterable from a stage.

    Arguments:
        stage: A stage object.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(lambda stage: to_iterable(stage, maxsize=maxsize))

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize)
    else:
        iterable = stage

    return iterable
