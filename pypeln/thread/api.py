"""
The `thread` module lets you create pipelines using objects from python's [threading](https://docs.python.org/3/library/threading.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need to perform some synchronous IO operations and DONT need to perform heavy CPU operations.
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

    def process(self, worker_namespace):

        if isinstance(self.iterable, pypeln_utils.BaseStage):
            for x in self.iterable.to_iterable(maxsize=0, return_index=True):
                if self.pipeline_namespace.error:
                    return

                self.output_queues.put(x)
        else:
            for i, x in enumerate(self.iterable):
                if self.pipeline_namespace.error:
                    return

                if isinstance(x, pypeln_utils.Element):
                    self.output_queues.put(x)
                else:
                    self.output_queues.put(pypeln_utils.Element(index=(i,), value=x))


def from_iterable(
    iterable: typing.Iterable = pypeln_utils.UNDEFINED,
    maxsize: int = None,
    worker_constructor: typing.Type = Thread,
) -> Stage:
    """
    Creates a stage from an iterable. This function gives you more control of how a stage is created through the `worker_constructor` parameter which can be either:
    
    * `threading.Thread`: (default) is efficient for iterables that already have the data in memory like lists or numpy arrays because threads can share memory so no serialization is needed. 
    * `multiprocessing.Process`: is efficient for iterables who's data is not in memory like arbitrary generators and benefit from escaping the GIL. This is inefficient for iterables which have data in memory because they have to be serialized when sent to the background process.
    

    Arguments:
        iterable: a source iterable.
        maxsize: this parameter is not used and only kept for API compatibility with the other modules.
        worker_constructor: defines the worker type for the producer stage.

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
        iterable=iterable,
        f=None,
        worker_constructor=worker_constructor,
        workers=1,
        maxsize=0,
        timeout=0,
        on_start=None,
        on_done=None,
        dependencies=[],
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
    def apply(self, elem, **kwargs):

        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)
        self.output_queues.put(elem.set(y))


def map(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that maps a function `f` over the data. Its intended to behave like python's built-in `map` function but with the added concurrency.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.thread.map(slow_add1, data, workers=3, maxsize=4)

    data = list(stage) # e.g. [2, 1, 5, 6, 3, 4, 7, 8, 9, 10]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 

    Arguments:
        f: A function with the signature `f(x) -> y`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    !!! warning
        To implement `timeout` we use `stopit.async_raise` which has some limitations for stoping threads.

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
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return Map(
        f=f,
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# flat_map
#############################################################


class FlatMap(Stage):
    def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        for i, y in enumerate(self.f(elem.value, **kwargs)):
            elem_y = pypeln_utils.Element(index=elem.index + (i,), value=y)
            self.output_queues.put(elem_y)


def flat_map(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.thread.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

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
    stage = pl.thread.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 
        
    `flat_map` is a more general operation, you can actually implement `pypeln.thread.map` and `pypeln.thread.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.thread.map(f, stage) = pl.thread.flat_map(lambda x: [f(x)], stage)
    pl.thread.filter(f, stage) = pl.thread.flat_map(lambda x: [x] if f(x) else [], stage)
    ```

    Using `flat_map` with a generator function is very useful as e.g. you are able to filter out unwanted elements when there are exceptions, missing data, etc.

    Arguments:
        f: A function with signature `f(x) -> iterable`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    !!! warning
        To implement `timeout` we use `stopit.async_raise` which has some limitations for stoping threads.

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
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return FlatMap(
        f=f,
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# filter
#############################################################


class Filter(Stage):
    def apply(self, elem, **kwargs):

        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        if self.f(elem.value, **kwargs):
            self.output_queues.put(elem)


def filter(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: typing.Callable = None,
    on_done: typing.Callable = None,
) -> Stage:
    """
    Creates a stage that filter the data given a predicate function `f`. It is intended to behave like python's built-in `filter` function but with the added concurrency.

    ```python
    import pypeln as pl
    import time
    from random import random

    def slow_gt3(x):
        time.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.thread.filter(slow_gt3, data, workers=3, maxsize=4)

    data = list(stage) # e.g. [5, 6, 3, 4, 7, 8, 9]
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `f(x) -> bool`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    !!! warning
        To implement `timeout` we use `stopit.async_raise` which has some limitations for stoping threads.

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
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    return Filter(
        f=f,
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# each
#############################################################


class Each(Stage):
    def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        self.f(elem.value, **kwargs)


def each(
    f: typing.Callable,
    stage: Stage = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
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
    stage = pl.thread.each(process_image, file_paths, workers=4)
    pl.thread.run(stage)

    ```

    or alternatively

    ```python
    files_paths = get_file_paths()
    pl.thread.each(process_image, file_paths, workers=4, run=True)
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

    !!! warning
        To implement `timeout` we use `stopit.async_raise` which has some limitations for stoping threads.

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
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = to_stage(stage)

    stage = Each(
        f=f,
        workers=workers,
        maxsize=maxsize,
        timeout=timeout,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass


#############################################################
# concat
#############################################################


class Concat(Stage):
    def apply(self, x):
        self.output_queues.put(x)


def concat(stages: typing.List[Stage], maxsize: int = 0) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.thread.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]
    ```

    Arguments:
        stages: a list of stages or iterables.
        maxsize: the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        A stage object.
    """

    stages = [to_stage(stage) for stage in stages]

    return Concat(
        f=None,
        workers=1,
        maxsize=maxsize,
        timeout=0,
        on_start=None,
        on_done=None,
        dependencies=stages,
    )


#############################################################
# ordered
#############################################################


class Ordered(Stage):
    def process(self, worker_namespace, **kwargs) -> None:

        elems = []

        for elem in self.input_queue:
            if self.pipeline_namespace.error:
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
            self.output_queues.put(elems.pop(0))


def ordered(stage: Stage = pypeln_utils.UNDEFINED, maxsize: int = 0) -> Stage:
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
    stage = pl.thread.map(slow_squared, stage, workers = 2)
    stage = pl.thread.ordered(stage)

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

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(lambda stage: ordered(stage, maxsize=maxsize))

    stage = to_stage(stage)

    return Ordered(
        f=None,
        workers=1,
        maxsize=maxsize,
        timeout=0,
        on_start=None,
        on_done=None,
        dependencies=[stage],
    )


#############################################################
# run
#############################################################


def run(stages: typing.List[Stage], maxsize: int = 0) -> None:
    """
    Iterates over one or more stages until their iterators run out of elements.

    ```python
    import pypeln as pl

    data = get_data()
    stage = pl.thread.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.thread.run(stage)
    ```

    Arguments:
        stages: A stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `concat` before iterating.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

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
    stage: Stage = pypeln_utils.UNDEFINED, maxsize: int = 0, return_index: bool = False
) -> typing.Iterable:
    """
    Creates an iterable from a stage.

    Arguments:
        stage: A stage object.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if pypeln_utils.is_undefined(stage):
        return pypeln_utils.Partial(lambda stage: to_iterable(stage, maxsize=maxsize))

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize, return_index=return_index)
    else:
        iterable = stage

    return iterable
