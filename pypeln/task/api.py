from threading import Thread

from pypeln import utils as pypeln_utils

from . import utils
from .stage import Stage
import asyncio
import time
import traceback
import sys


#############################################################
# to_stage
#############################################################


class FromIterable(Stage):
    def __init__(self, iterable, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.iterable = iterable

    async def process(self):
        async for x in self.to_async_iterable():
            if self.pipeline_namespace.error:
                return

            await self.output_queues.put(x)

    async def to_async_iterable(self):
        if hasattr(self.iterable, "__aiter__"):
            async for x in self.iterable:
                yield x
        elif not hasattr(self.iterable, "__iter__"):
            raise ValueError(
                f"Object {self.iterable} most be either iterable or async iterable."
            )

        if False and type(self.iterable) in (list, dict, tuple, set):
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
        try:
            for x in self.iterable:
                while True:
                    if not queue.full():
                        self.loop.call_soon_threadsafe(queue.put_nowait, x)
                        break
                    else:
                        time.sleep(utils.TIMEOUT)

            while True:
                if not queue.full():
                    self.loop.call_soon_threadsafe(queue.done_nowait)
                    break
                else:
                    time.sleep(utils.TIMEOUT)

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
    iterable=pypeln_utils.UNDEFINED, maxsize=None, worker_constructor=Thread
):
    """
    Creates a stage from an iterable. This function gives you more control of how a stage is created through the `worker_constructor` parameter which can be either:
    
    * `threading.Thread`: (default) is efficient for iterables that already have the data in memory like lists or numpy arrays because threads can share memory so no serialization is needed. 
    * `multiprocessing.Process`: is efficient for iterables who's data is not in memory like arbitrary generators and benefit from escaping the GIL. This is inefficient for iterables which have data in memory because they have to be serialized when sent to the background process.

    All functions that accept stages or iterables use this function when an iterable is passed to convert it into a stage using the default arguments.

    # **Args**
    * **`iterable`** : a source iterable.
    * **`maxsize = None`** : this parameter is not used and only kept for API compatibility with the other modules.
    * **`worker_constructor = threading.Thread`** : defines the worker type for the producer stage.

    # **Returns**
    * If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
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
    async def apply(self, x, **kwargs):
        y = self.f(x, **kwargs)

        if hasattr(y, "__await__"):
            y = await y

        await self.output_queues.put(y)


def map(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):
    """
    Creates a stage that maps a function `f` over the data. Its intended to behave like python's built-in `map` function but with the added concurrency.

        import pypeln as pl
        import time
        from random import random

        def slow_add1(x):
            time.sleep(random()) # <= some slow computation
            return x + 1

        data = range(10) # [0, 1, 2, ..., 9]
        stage = pl.process.map(slow_add1, data, workers = 3, maxsize = 4)

        data = list(stage) # e.g. [2, 1, 5, 6, 3, 4, 7, 8, 9, 10]

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> y`, where `args` is the return of `on_start` if present, else the signature is just `f(x) -> y`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
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

    return Map(
        f=f,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# flat_map
#############################################################


class FlatMap(Stage):
    async def apply(self, x, **kwargs):
        for y in self.f(x, **kwargs):

            if hasattr(y, "__await__"):
                y = await y

            await self.output_queues.put(y)


def flat_map(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.process.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

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
        stage = pl.process.flat_map(slow_integer_pair, data, workers = 3, maxsize = 4)

        list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]

    Note that because of concurrency order is not guaranteed. Also, `flat_map` is more general than both `pypeln.process.map` and `pypeln.process.filter`, as such these expressions are equivalent:

        import pypeln as pl

        pl.process.map(f, stage) = pl.process.flat_map(lambda x: [f(x)], stage)
        pl.process.filter(f, stage) = pl.process.flat_map(lambda x: [x] if f(x) else [], stage)

    Using `flat_map` with a generator function is very useful as we are able to filter out unwanted elements when e.g. there are exceptions, missing data, etc.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> [y]`, where `args` is the return of `on_start` if present, else the signature is just `f(x) -> [y]`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
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

    return FlatMap(
        f=f,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# filter
#############################################################


class Filter(Stage):
    async def apply(self, x, **kwargs):
        y = self.f(x, **kwargs)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await self.output_queues.put(x)


def filter(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):
    """
    Creates a stage that filter the data given a predicate function `f`. It is intended to behave like python's built-in `filter` function but with the added concurrency.

        import pypeln as pl
        import time
        from random import random

        def slow_gt3(x):
            time.sleep(random()) # <= some slow computation
            return x > 3

        data = range(10) # [0, 1, 2, ..., 9]
        stage = pl.process.filter(slow_gt3, data, workers = 3, maxsize = 4)

        data = list(stage) # e.g. [5, 6, 3, 4, 7, 8, 9]

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> bool`, where `args` is the return of `on_start` if present, else the signature is just `f(x)`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
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

    return Filter(
        f=f,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        dependencies=[stage],
    )


#############################################################
# each
#############################################################


class Each(Stage):
    async def apply(self, x, **kwargs):
        y = self.f(x, **kwargs)

        if hasattr(y, "__await__"):
            y = await y


def each(
    f,
    stage=pypeln_utils.UNDEFINED,
    workers=1,
    maxsize=0,
    on_start=None,
    on_done=None,
    run=False,
):
    """
    Creates a stage that runs the function `f` for each element in the data but the stage itself yields no elements. Its useful for sink stages that perform certain actions such as writting to disk, saving to a database, etc, and dont produce any results. For example:

        import pypeln as pl

        def process_image(image_path):
            image = load_image(image_path)
            image = transform_image(image)
            save_image(image_path, image)

        files_paths = get_file_paths()
        stage = pl.process.each(process_image, file_paths, workers = 4)
        pl.process.run(stage)

    or alternatively

        files_paths = get_file_paths()
        pl.process.each(process_image, file_paths, workers = 4, run = True)

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> None`, where `args` is the return of `on_start` if present, else the signature is just `f(x)`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.
    * **`run = False`** : specify whether to run the stage immediately.

    # **Returns**
    * If the `stage` parameters is not given then this function returns a `Partial`, else if `return = False` (default) it return a new stage, if `run = True` then it runs the stage and returns `None`.
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

    stage = Each(
        f=f,
        workers=workers,
        maxsize=maxsize,
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


def concat(stages, maxsize=0):
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

        import pypeln as pl

        stage_1 = [1, 2, 3]
        stage_2 = [4, 5, 6, 7]

        stage_3 = pl.process.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]

    # **Args**
    * **`stages`** : a list of stages or iterables.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    # **Returns**
    * A stage object.
    """

    stages = [to_stage(stage) for stage in stages]

    return Concat(
        f=None,
        workers=1,
        maxsize=maxsize,
        on_start=None,
        on_done=None,
        dependencies=stages,
    )

