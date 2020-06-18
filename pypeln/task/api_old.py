"""
The `process` module lets you create pipelines using objects from python's [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need of true parallelism for CPU heavy operations but be aware of its implications.
"""

from threading import Thread
import time
import typing
import typing as tp
import asyncio

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue
from .stage import Stage
from .worker import Kwargs, StageParams, Worker, TaskPool

T = tp.TypeVar("T")
A = tp.TypeVar("A")
B = tp.TypeVar("B")


class ApplyWorkerConstructor(WorkerApply[T]):
    @classmethod
    def get_worker_constructor(
        cls,
        f: tp.Callable,
        timeout: float,
        on_start: tp.Optional[tp.Callable[..., Kwargs]],
        on_done: tp.Optional[tp.Callable[..., Kwargs]],
        max_tasks: int,
    ) -> WorkerConstructor:
        def worker_constructor(
            stage_params: StageParams, main_queue: IterableQueue
        ) -> Worker[T]:
            return cls.create(
                f=f,
                stage_params=stage_params,
                main_queue=main_queue,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
                max_tasks=max_tasks,
            )

        return worker_constructor


# ----------------------------------------------------------------
# from_iterable
# ----------------------------------------------------------------


class FromIterable(Worker[T]):
    def __init__(
        self,
        iterable: tp.Union[tp.Iterable[T], tp.AsyncIterable[T]],
        stage_params: StageParams,
        main_queue: IterableQueue,
        maxsize: int,
    ):
        super().__init__(
            f=pypeln_utils.no_op,
            stage_params=stage_params,
            main_queue=main_queue,
            tasks=TaskPool.create(workers=1, timeout=0),
        )

        self.iterable = iterable
        self.maxsize = maxsize

    @classmethod
    def get_worker_constructor(
        cls, iterable: tp.Union[tp.Iterable[T], tp.AsyncIterable[T]], maxsize: int
    ) -> WorkerConstructor:
        def worker_constructor(
            stage_params: StageParams, main_queue: IterableQueue
        ) -> FromIterable:
            return cls(
                iterable=iterable,
                stage_params=stage_params,
                main_queue=main_queue,
                maxsize=maxsize,
            )

        return worker_constructor

    async def process_fn(self, f_args: tp.List[str], **kwargs):
        iterable: tp.AsyncIterable

        if isinstance(self.iterable, tp.AsyncIterable):
            iterable = self.iterable
        else:
            sync_iterable: tp.Iterable

            if isinstance(self.iterable, pypeln_utils.BaseStage):
                sync_iterable = self.iterable.to_iterable(maxsize=0, return_index=True)
            else:
                sync_iterable = self.iterable

            queue = IterableQueue()
            loop = utils.get_running_loop()

            loop.run_in_executor(
                None, lambda: self.consume_iterable(sync_iterable, queue, loop)
            )

            iterable = queue

        i = 0
        async for x in iterable:
            if not isinstance(x, pypeln_utils.Element):
                x = pypeln_utils.Element(index=(i,), value=x)

            await self.stage_params.output_queues.put(x)

            i += 1

    def consume_iterable(
        self,
        iterable: tp.Iterable,
        queue: IterableQueue,
        loop: asyncio.AbstractEventLoop,
    ):

        try:
            for x in iterable:
                if self.is_done:
                    return

                while queue.full():
                    if self.is_done:
                        return

                    time.sleep(pypeln_utils.TIMEOUT)

                asyncio.run_coroutine_threadsafe(queue.put(x), loop)

            asyncio.run_coroutine_threadsafe(queue.done(), loop)

        except BaseException as e:
            e = queue.get_pipeline_exception(e)
            asyncio.run_coroutine_threadsafe(queue.raise_exception(e), loop)


@tp.overload
def from_iterable(
    iterable: tp.Union[tp.Iterable[T], tp.AsyncIterable[T]],
    maxsize: int = 0,
    use_thread: bool = True,
) -> Stage[T]:
    ...


@tp.overload
def from_iterable(
    maxsize: int = 0, worker_constructor: tp.Type = Thread, use_thread: bool = True
) -> pypeln_utils.Partial[Stage[T]]:
    ...


def from_iterable(
    iterable: tp.Union[
        tp.Iterable[T], tp.AsyncIterable[T], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    use_thread: bool = True,
):
    """
    Creates a stage from an iterable. This function gives you more control of the iterable is consumed.
    Arguments:
        iterable: a source iterable.
        maxsize: this parameter is not used and only kept for API compatibility with the other modules.
        use_thread: If set to `True` (default) it will use a thread instead of a process to consume the iterable. Threads start faster and use thread memory to the iterable is not serialized, however, if the iterable is going to perform slow computations it better to use a process.

    Returns:
        If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if isinstance(iterable, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda iterable: from_iterable(
                iterable, maxsize=maxsize, use_thread=use_thread
            )
        )

    return Stage.create(
        total_sources=1,
        maxsize=maxsize,
        worker_constructor=FromIterable.get_worker_constructor(
            iterable=iterable, maxsize=maxsize
        ),
        dependencies=[],
    )


# ----------------------------------------------------------------
# to_stage
# ----------------------------------------------------------------


def to_stage(obj: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]]) -> Stage[A]:

    if isinstance(obj, Stage):
        return obj

    elif isinstance(obj, tp.Iterable) or isinstance(obj, tp.AsyncIterable):
        return from_iterable(obj)

    else:
        raise ValueError(f"Object {obj} is not a Stage or iterable")


# ----------------------------------------------------------------
# map
# ----------------------------------------------------------------


class Map(ApplyWorkerConstructor[T]):
    async def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):

        if "element_index" in f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)

        if isinstance(y, tp.Awaitable):
            y = await y

        await self.stage_params.output_queues.put(elem.set(y))


@tp.overload
def map(
    f: tp.Callable[..., B],
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> Stage[B]:
    ...


@tp.overload
def map(
    f: tp.Callable[..., B],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> pypeln_utils.Partial[Stage[B]]:
    ...


def map(
    f: tp.Callable,
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> tp.Union[Stage[B], pypeln_utils.Partial[Stage[B]]]:
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
    stage = pl.process.map(slow_add1, data, workers=3, maxsize=4)

    data = list(stage) # e.g. [2, 1, 5, 6, 3, 4, 7, 8, 9, 10]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 

    Arguments:
        f: A function with the signature `f(x) -> y`. `f` can accept special additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A stage or iterable.
        workers: The number of workers the stage should contain.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
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

    return Stage.create(
        maxsize=maxsize,
        total_sources=1,
        worker_constructor=Map.get_worker_constructor(
            f=f, timeout=timeout, on_start=on_start, on_done=on_done, max_tasks=workers
        ),
        dependencies=[stage],
    )


# ----------------------------------------------------------------
# flat_map
# ----------------------------------------------------------------


class FlatMap(ApplyWorkerConstructor[T]):
    async def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):

        if "element_index" in f_args:
            kwargs["element_index"] = elem.index

        ys: tp.Union[
            tp.Iterable[pypeln_utils.Element], tp.AsyncIterable[pypeln_utils.Element]
        ] = self.f(elem.value, **kwargs)

        if isinstance(ys, tp.AsyncIterable):
            i = 0
            async for y in ys:
                elem_y = pypeln_utils.Element(index=elem.index + (i,), value=y)
                await self.stage_params.output_queues.put(elem_y)
                i += 1
        else:
            for i, y in enumerate(ys):
                elem_y = pypeln_utils.Element(index=elem.index + (i,), value=y)
                await self.stage_params.output_queues.put(elem_y)


@tp.overload
def flat_map(
    f: tp.Callable[..., tp.Union[tp.Iterable[B], tp.AsyncIterable[B]]],
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> Stage[B]:
    ...


@tp.overload
def flat_map(
    f: tp.Callable[..., tp.Union[tp.Iterable[B], tp.AsyncIterable[B]]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> pypeln_utils.Partial[Stage[B]]:
    ...


def flat_map(
    f: tp.Callable[..., tp.Union[tp.Iterable[B], tp.AsyncIterable[B]]],
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> tp.Union[Stage[B], pypeln_utils.Partial[Stage[B]]]:
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.process.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

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
    stage = pl.process.flat_map(slow_integer_pair, data, workers=3, maxsize=4)

    list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]
    ```

    !!! note
        Because of concurrency order is not guaranteed. 
        
    `flat_map` is a more general operation, you can actually implement `pypeln.process.map` and `pypeln.process.filter` with it, for example:

    ```python
    import pypeln as pl

    pl.process.map(f, stage) = pl.process.flat_map(lambda x: [f(x)], stage)
    pl.process.filter(f, stage) = pl.process.flat_map(lambda x: [x] if f(x) else [], stage)
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

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
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

    return Stage.create(
        total_sources=1,
        maxsize=maxsize,
        worker_constructor=FlatMap.get_worker_constructor(
            f=f, timeout=timeout, on_start=on_start, on_done=on_done, max_tasks=workers
        ),
        dependencies=[stage],
    )


# ----------------------------------------------------------------
# filter
# ----------------------------------------------------------------


class Filter(ApplyWorkerConstructor[T]):
    async def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):

        if "element_index" in f_args:
            kwargs["element_index"] = elem.index

        y = self.f(elem.value, **kwargs)

        if isinstance(y, tp.Awaitable):
            y = await y

        if y:
            await self.stage_params.output_queues.put(elem)


@tp.overload
def filter(
    f: tp.Callable[..., tp.Union[bool, tp.Awaitable[bool]]],
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> Stage[B]:
    ...


@tp.overload
def filter(
    f: tp.Callable[..., tp.Union[bool, tp.Awaitable[bool]]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> pypeln_utils.Partial[Stage[B]]:
    ...


def filter(
    f: tp.Callable[..., tp.Union[bool, tp.Awaitable[bool]]],
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
) -> tp.Union[Stage[A], pypeln_utils.Partial[Stage[A]]]:
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
    stage = pl.process.filter(slow_gt3, data, workers=3, maxsize=4)

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

    Returns:
        If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
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

    return Stage.create(
        total_sources=1,
        maxsize=maxsize,
        worker_constructor=Filter.get_worker_constructor(
            f=f, timeout=timeout, on_start=on_start, on_done=on_done, max_tasks=workers
        ),
        dependencies=[stage],
    )


# ----------------------------------------------------------------
# each
# ----------------------------------------------------------------


class Each(ApplyWorkerConstructor[T]):
    async def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):
        if "element_index" in f_args:
            kwargs["element_index"] = elem.index

        output = self.f(elem.value, **kwargs)

        if isinstance(output, tp.Awaitable):
            await output


@tp.overload
def each(
    f: tp.Callable[..., B],
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
    run: bool = False,
) -> tp.Optional[Stage[B]]:
    ...


@tp.overload
def each(
    f: tp.Callable[..., None],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
    run: bool = False,
) -> pypeln_utils.Partial[tp.Optional[Stage[B]]]:
    ...


def each(
    f: tp.Callable,
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
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
        run: Whether or not to execute the stage immediately. If each is running inside another coroutine / task then avoid using `run=True` since it will block the event loop, use `await pl.task.each(...)` instead.

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
        total_sources=1,
        maxsize=maxsize,
        worker_constructor=Each.get_worker_constructor(
            f=f, timeout=timeout, on_start=on_start, on_done=on_done, max_tasks=workers
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
    async def apply(self, elem: pypeln_utils.Element, f_args: tp.List[str], **kwargs):
        await self.stage_params.output_queues.put(elem)


def concat(
    stages: tp.List[tp.Union[Stage[A], tp.Iterable[A]]], maxsize: int = 0
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

    dependencies: tp.List[Stage[A]] = [to_stage(stage) for stage in stages]

    return Stage.create(
        total_sources=len(dependencies),
        maxsize=maxsize,
        worker_constructor=Concat.get_worker_constructor(
            f=pypeln_utils.no_op, timeout=0, on_start=None, on_done=None, max_tasks=1
        ),
        dependencies=dependencies,
    )


# ----------------------------------------------------------------
# ordered
# ----------------------------------------------------------------


class Ordered(Worker[T]):
    def __init__(self, stage_params: StageParams, main_queue: IterableQueue):
        super().__init__(
            f=pypeln_utils.no_op,
            stage_params=stage_params,
            main_queue=main_queue,
            tasks=TaskPool.create(workers=1, timeout=0),
        )

    @classmethod
    def get_worker_constructor(cls) -> WorkerConstructor:
        def worker_constructor(
            stage_params: StageParams, main_queue: IterableQueue
        ) -> Ordered:
            return cls(stage_params=stage_params, main_queue=main_queue)

        return worker_constructor

    async def process_fn(self, f_args: tp.List[str], **kwargs):

        elems = []

        async for elem in self.stage_params.input_queue:

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
            await self.stage_params.output_queues.put(elems.pop(0))


@tp.overload
def ordered(
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]], maxsize: int = 0
) -> Stage[A]:
    ...


@tp.overload
def ordered(maxsize: int = 0) -> pypeln_utils.Partial[Stage[A]]:
    ...


def ordered(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
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
        total_sources=1,
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
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
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
    elif isinstance(stage, tp.Iterable[A]):
        return stage
    else:
        iterable = from_iterable(stage, maxsize=maxsize).to_iterable(
            maxsize=maxsize, return_index=return_index
        )

    return iterable


@tp.overload
def to_async_iterable(
    stage: tp.Union[Stage[A], tp.Iterable[A], tp.AsyncIterable[A]],
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.AsyncIterable[A]:
    ...


@tp.overload
def to_async_iterable(
    maxsize: int = 0, return_index: bool = False,
) -> pypeln_utils.Partial[tp.AsyncIterable[A]]:
    ...


def to_async_iterable(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], tp.AsyncIterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    return_index: bool = False,
) -> tp.Union[tp.AsyncIterable[A], pypeln_utils.Partial[tp.AsyncIterable[A]]]:
    """
    Creates an iterable from a stage.

    Arguments:
        stage: A stage object.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: to_async_iterable(stage, maxsize=maxsize)
        )

    if isinstance(stage, Stage):
        iterable = stage.to_async_iterable(maxsize=maxsize, return_index=return_index)

    elif isinstance(stage, tp.AsyncIterable[A]):
        return stage
    else:
        iterable = from_iterable(stage, maxsize=maxsize).to_async_iterable(
            maxsize=maxsize, return_index=return_index
        )

    return iterable
