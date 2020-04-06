"""
The `task` module lets you create pipelines using objects from python's [asyncio](https://docs.python.org/3/library/asyncio.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need to perform efficient asynchronous IO operations and DONT need to perform heavy CPU operations.
"""

import asyncio
import sys
import time
import traceback
import typing
from threading import Thread

from pypeln import utils as pypeln_utils

from . import utils
from .stage import Stage

#############################################################
# to_stage
#############################################################


class FromIterable(Stage):
    def __init__(self, iterable, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.iterable = iterable

    async def process(self):
        i = 0
        async for x in self.to_async_iterable():
            if self.pipeline_namespace.error:
                return

            if isinstance(x, pypeln_utils.Element):
                await self.output_queues.put(x)
            else:
                await self.output_queues.put(pypeln_utils.Element(index=(i,), value=x))
                i += 1

    async def to_async_iterable(self):
        if not hasattr(self.iterable, "__iter__") or hasattr(
            self.iterable, "__aiter__"
        ):
            raise ValueError(
                f"Object {self.iterable} most be either iterable or async iterable."
            )

        if hasattr(self.iterable, "__aiter__"):
            async for x in self.iterable:
                yield x

        elif type(self.iterable) in (list, dict, tuple, set):
            for i, x in enumerate(self.iterable):
                yield x

                if i % 1000 == 0:
                    await asyncio.sleep(0)

        else:
            queue = utils.IterableQueue(
                maxsize=self.maxsize,
                total_done=1,
                pipeline_namespace=self.pipeline_namespace,
                loop=self.loop,
            )

            task = self.loop.run_in_executor(None, lambda: self.consume_iterable(queue))
            async for x in queue:
                yield x

            await task

    def consume_iterable(self, queue):
        iterable = (
            self.iterable.to_iterable(maxsize=pypeln_utils.MAXSIZE, return_index=True)
            if isinstance(self.iterable, pypeln_utils.BaseStage)
            else self.iterable
        )

        try:
            for x in iterable:
                while True:
                    if not queue.full():
                        self.loop.call_soon_threadsafe(queue.put_nowait, x)
                        break
                    else:
                        time.sleep(pypeln_utils.TIMEOUT)

            while True:
                if not queue.full():
                    self.loop.call_soon_threadsafe(queue.done_nowait)
                    break
                else:
                    time.sleep(pypeln_utils.TIMEOUT)

        except BaseException as e:
            try:
                for stage in self.pipeline_stages:
                    self.loop.call_soon_threadsafe(stage.input_queue.done_nowait)

                self.loop.call_soon_threadsafe(
                    self.pipeline_error_queue.put_nowait,
                    (type(e), e, "".join(traceback.format_exception(*sys.exc_info()))),
                )
                self.pipeline_namespace.error = True
                self.loop.call_soon_threadsafe(queue.done_nowait)
            except BaseException as e:
                print(e)


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
            lambda iterable: from_iterable(iterable, maxsize=None,)
        )

    return FromIterable(
        iterable=iterable,
        f=None,
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

    elif hasattr(obj, "__iter__") or hasattr(obj, "__aiter__"):
        return from_iterable(obj)

    else:
        raise ValueError(f"Object {obj} is not a Stage or iterable")


#############################################################
# map
#############################################################


class Map(Stage):
    async def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)

        if hasattr(y, "__await__"):
            y = await y

        await self.output_queues.put(elem.set(y))


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
    import asyncio
    from random import random

    async def slow_add1(x):
        await asyncio.sleep(random()) # <= some slow computation
        return x + 1

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.task.map(slow_add1, data, workers=3, maxsize=4)

    data = list(stage) # e.g. [2, 1, 5, 6, 3, 4, 7, 8, 9, 10]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 

    Arguments:
        f: A function with the signature `async? f(x) -> y`. `f` can accept special additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

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
    async def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        iterable = self.f(elem.value, **kwargs)

        if hasattr(iterable, "__aiter__"):
            i = 0
            async for y in iterable:
                await self.output_queues.put(
                    pypeln_utils.Element(index=elem.index + (i,), value=y)
                )
                i += 1
        else:
            if hasattr(iterable, "__await__"):
                iterable = await iterable

            for i, y in enumerate(iterable):

                if hasattr(y, "__await__"):
                    y = await y

                await self.output_queues.put(
                    pypeln_utils.Element(index=elem.index + (i,), value=y)
                )


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
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.task.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

    ```python
    import pypeln as pl
    import asyncio
    from random import random

    async def slow_integer_pair(x):
        await asyncio.sleep(random()) # <= some slow computation

        if x == 0:
            yield x
        else:
            yield x
            yield -x

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.task.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 
        
    `flat_map` is a more general operation, you can actually implement `pypeln.task.map` and `pypeln.task.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.task.map(f, stage) = pl.task.flat_map(lambda x: [f(x)], stage)
    pl.task.filter(f, stage) = pl.task.flat_map(lambda x: [x] if f(x) else [], stage)
    ```

    Using `flat_map` with a generator function is very useful as e.g. you are able to filter out unwanted elements when there are exceptions, missing data, etc.

    Arguments:
        f: A function with signature `async? f(x) -> iterable`, it can also be an [async generator](https://www.python.org/dev/peps/pep-0525/#id8). `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

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
    async def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await self.output_queues.put(elem)


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
    import asyncio
    from random import random

    async def slow_gt3(x):
        await asyncio.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pl.task.filter(slow_gt3, data, workers=3, maxsize=4)

    data = list(stage) # e.g. [5, 6, 3, 4, 7, 8, 9]
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `async? f(x) -> bool`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

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
    async def apply(self, elem, **kwargs):
        if "element_index" in self.f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)

        if hasattr(y, "__await__"):
            y = await y


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

    async def process_image(image_path):
        image = await load_image(image_path)
        image = await transform_image(image)
        await save_image(image_path, image)

    files_paths = get_file_paths()
    stage = pl.task.each(process_image, file_paths, workers=4)
    pl.task.run(stage)

    ```

    or alternatively

    ```python
    files_paths = get_file_paths()
    pl.task.each(process_image, file_paths, workers=4, run=True)
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `async? f(x) -> None`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
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
    async def apply(self, x):
        await self.output_queues.put(x)


def concat(stages: typing.List[Stage], maxsize: int = 0) -> Stage:
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

    ```python
    import pypeln as pl

    stage_1 = [1, 2, 3]
    stage_2 = [4, 5, 6, 7]

    stage_3 = pl.task.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]
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
    async def process(self, **kwargs) -> None:

        elems = []

        async for elem in self.input_queue:
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
            await self.output_queues.put(elems.pop(0))


def ordered(stage: Stage = pypeln_utils.UNDEFINED, maxsize: int = 0) -> Stage:
    """
    Creates a stage that sorts its elements based on their order of creation on the source iterable(s) of the pipeline.

    ```python
    import pypeln as pl
    import random
    import asyncio

    async def slow_squared(x):
        await asyncio.sleep(random.random())
        
        return x ** 2

    stage = range(5)
    stage = pl.task.map(slow_squared, stage, workers = 2)
    stage = pl.task.ordered(stage)

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
    stage = pl.task.each(slow_fn, data, workers=6)

    # execute pipeline
    pl.task.run(stage)
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
        return pypeln_utils.Partial(
            lambda stage: to_iterable(stage, maxsize=maxsize, return_index=return_index)
        )

    if isinstance(stage, Stage):
        iterable = stage.to_iterable(maxsize=maxsize, return_index=return_index)
    else:
        iterable = stage

    return iterable
