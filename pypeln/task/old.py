from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
import time
import functools


def _WORKER(target, args, kwargs=None):
    kwargs = kwargs if kwargs is not None else dict()
    return target(*args, **kwargs)


def get_namespace():
    return utils.Namespace()


class _Stage(utils.BaseStage):
    def __init__(
        self,
        worker_constructor,
        workers,
        maxsize,
        on_start,
        on_done,
        target,
        args,
        dependencies,
    ):
        self.worker_constructor = worker_constructor
        self.workers = workers
        self.maxsize = maxsize
        self.on_start = on_start
        self.on_done = on_done
        self.target = target
        self.args = args
        self.dependencies = dependencies

    def __await__(self):

        maxsize = 0
        pipeline_namespace = get_namespace()
        pipeline_namespace.error = None
        loop = asyncio.get_event_loop()

        task, _input_queue = _to_task(self, maxsize, pipeline_namespace, loop)

        return task.__await__()

    def __iter__(self):
        # print("__iter__", self)
        return to_iterable(self)

    async def __aiter__(self):

        maxsize = 0
        pipeline_namespace = get_namespace()
        pipeline_namespace.error = None
        loop = asyncio.get_event_loop()

        task, input_queue = _to_task(self, maxsize, pipeline_namespace, loop)

        task = asyncio.ensure_future(task, loop=loop)

        async for x in input_queue:
            yield x

        await task

    def __repr__(self):
        return "_Stage(worker_constructor = {worker_constructor}, workers={workers}, maxsize={maxsize}, target = {target}, args = {args}, dependencies = {dependencies})".format(
            worker_constructor=self.worker_constructor,
            workers=self.workers,
            maxsize=self.maxsize,
            target=self.target,
            args=self.args,
            dependencies=len(self.dependencies),
        )


class _StageParams(
    namedtuple(
        "_StageParams",
        [
            "workers",
            "input_queue",
            "output_queues",
            "on_start",
            "on_done",
            "pipeline_namespace",
            "loop",
            "maxsize",
        ],
    )
):
    pass


class StageStatus(object):
    def __init__(self):
        pass

    @property
    def done(self):
        return True

    @property
    def active_workers(self):
        return 0

    def __str__(self):
        return "StageStatus(done = {done}, active_workers = {active_workers})".format(
            done=self.done, active_workers=self.active_workers,
        )


class _InputQueue(asyncio.Queue):
    def __init__(self, maxsize, total_done, pipeline_namespace, **kwargs):
        super(_InputQueue, self).__init__(maxsize=maxsize, **kwargs)

        self.remaining = total_done
        self.pipeline_namespace = pipeline_namespace

    async def __aiter__(self):

        while not self.is_done():
            x = await self.get()

            if self.pipeline_namespace.error is not None:
                return

            if not utils.is_continue(x):
                yield x

    async def get(self):

        x = await super(_InputQueue, self).get()

        # print(x)

        if utils.is_done(x):
            self.remaining -= 1
            return utils.CONTINUE

        return x

    def get_nowait(self):

        x = super(_InputQueue, self).get_nowait()

        if utils.is_done(x):
            self.remaining -= 1
            return utils.CONTINUE

        return x

    def is_done(self):
        return self.remaining == 0  # and self.empty()

    async def done(self):
        await self.put(utils.DONE)


class _OutputQueues(list):
    async def put(self, x):
        for queue in self:
            await queue.put(x)

    async def done(self):
        for queue in self:
            await queue.put(utils.DONE)

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.done()


def _handle_exceptions(params):
    def handle_exceptions(f):
        @functools.wraps(f)
        async def wrapper(*args, **kwargs):
            try:
                await f(*args, **kwargs)

            except BaseException as e:
                params.pipeline_namespace.error = e

        return wrapper

    return handle_exceptions


async def _run_task(f_task, params):

    args = params.on_start() if params.on_start is not None else None

    if hasattr(args, "__await__"):
        args = await args

    if args is None:
        args = ()

    elif not isinstance(args, tuple):
        args = (args,)

    if params.input_queue:

        async with TaskPool(params.workers, params.loop) as tasks:

            async for x in params.input_queue:

                task = f_task(x, args)
                await tasks.put(task)

    else:
        await f_task(args)

    await params.output_queues.done()

    if params.on_done is not None:

        stage_status = StageStatus()

        done_resp = params.on_done(stage_status, *args)

        if hasattr(done_resp, "__await__"):
            await done_resp


########################
# map
########################


def _map(f, params):
    @_handle_exceptions(params)
    async def f_task(x, args):

        y = f(x, *args)

        if hasattr(y, "__await__"):
            y = await y

        await params.output_queues.put(y)

    return _run_task(f_task, params)


def map(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: map(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor=_WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_map,
        args=(f,),
        dependencies=[stage],
    )


########################
# flat_map
########################
def _flat_map(f, params):
    @_handle_exceptions(params)
    async def f_task(x, args):

        ys = f(x, *args)

        ys = _to_async_iterable(ys, params)

        async for y in ys:
            await params.output_queues.put(y)

    return _run_task(f_task, params)


def flat_map(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: flat_map(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor=_WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_flat_map,
        args=(f,),
        dependencies=[stage],
    )


########################
# filter
########################


def _filter(f, params):
    @_handle_exceptions(params)
    async def f_task(x, args):

        y = f(x, *args)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await params.output_queues.put(x)

    return _run_task(f_task, params)


def filter(
    f, stage=pypeln_utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: filter(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor=_WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_filter,
        args=(f,),
        dependencies=[stage],
    )


########################
# each
########################


def _each(f, params):
    @_handle_exceptions(params)
    async def f_task(x, args):

        y = f(x, *args)

        if hasattr(y, "__await__"):
            y = await y

    return _run_task(f_task, params)


def each(
    f,
    stage=pypeln_utils.UNDEFINED,
    workers=1,
    maxsize=0,
    on_start=None,
    on_done=None,
    run=False,
):

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: each(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
                run=run,
            )
        )

    stage = _to_stage(stage)

    stage = _Stage(
        worker_constructor=_WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_filter,
        args=(f,),
        dependencies=[stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass


########################
# concat
########################


def _concat(params):
    async def f_task(x, args):
        await params.output_queues.put(x)

    return _run_task(f_task, params)


def concat(stages, maxsize=0):

    stages = [_to_stage(s) for s in stages]

    return _Stage(
        worker_constructor=_WORKER,
        workers=1,
        maxsize=maxsize,
        on_start=None,
        on_done=None,
        target=_concat,
        args=tuple(),
        dependencies=stages,
    )


################
# _to_task
################


def _to_task(stage, maxsize, pipeline_namespace, loop):

    total_done = 1
    input_queue = _InputQueue(maxsize, total_done, pipeline_namespace)

    stage_input_queue, stage_output_queues = _build_queues(
        stage=stage,
        stage_input_queue=dict(),
        stage_output_queues=dict(),
        visited=set(),
        pipeline_namespace=pipeline_namespace,
    )

    stage_output_queues[stage] = _OutputQueues([input_queue])

    tasks = []

    for _stage in stage_output_queues:

        stage_params = _StageParams(
            workers=_stage.workers,
            input_queue=stage_input_queue.get(_stage, None),
            output_queues=stage_output_queues[_stage],
            on_start=_stage.on_start,
            on_done=_stage.on_done,
            pipeline_namespace=pipeline_namespace,
            loop=loop,
            maxsize=_stage.maxsize,
        )

        task = _stage.worker_constructor(
            target=_stage.target, args=_stage.args + (stage_params,),
        )

        tasks.append(task)

    task = asyncio.gather(*tasks, loop=loop)

    return task, input_queue


################
# _to_stage
################


def _to_stage(obj):

    if isinstance(obj, _Stage):
        return obj

    elif hasattr(obj, "__iter__") or hasattr(obj, "__aiter__"):
        return from_iterable(obj)

    else:
        raise ValueError("Object {obj} is not iterable".format(obj=obj))


########################
# from_iterable
########################


def _from_iterable(iterable, params):

    # print("_from_iterable", iterable)

    if not hasattr(iterable, "__aiter__") and not hasattr(iterable, "__iter__"):
        raise ValueError()

    @_handle_exceptions(params)
    async def f_task(args):

        # print("_from_iterable_task", iterable)

        # if hasattr(iterable, "__aiter__"):
        #     async for x in iterable:
        #         await params.output_queues.put(x)

        # elif hasattr(iterable, "__iter__"):
        #     for x in iterable:
        #         await params.output_queues.put(x)

        iterable_ = _to_async_iterable(iterable, params)

        async for x in iterable_:
            await params.output_queues.put(x)

    return _run_task(f_task, params)


def from_iterable(iterable=pypeln_utils.UNDEFINED, maxsize=0, worker_constructor=None):

    if utils.is_undefined(iterable):
        return utils.Partial(
            lambda iterable: from_iterable(
                iterable, maxsize=maxsize, worker_constructor=worker_constructor
            )
        )

    # print("from_iterable", iterable)

    return _Stage(
        worker_constructor=_WORKER,
        workers=1,
        maxsize=maxsize,
        on_start=None,
        on_done=None,
        target=_from_iterable,
        args=(iterable,),
        dependencies=[],
    )


########################
# to_iterable
########################


def _build_queues(
    stage, stage_input_queue, stage_output_queues, visited, pipeline_namespace
):

    if stage in visited:
        return stage_input_queue, stage_output_queues
    else:
        visited.add(stage)

    if len(stage.dependencies) > 0:
        total_done = len(stage.dependencies)
        input_queue = _InputQueue(stage.maxsize, total_done, pipeline_namespace)
        stage_input_queue[stage] = input_queue

        for stage in stage.dependencies:
            if stage not in stage_output_queues:
                stage_output_queues[stage] = _OutputQueues([input_queue])
            else:
                stage_output_queues[stage].append(input_queue)

            stage_input_queue, stage_output_queues = _build_queues(
                stage,
                stage_input_queue,
                stage_output_queues,
                visited,
                pipeline_namespace,
            )

    return stage_input_queue, stage_output_queues


def _handle_async_exception(loop, ctx):
    # print(ctx)

    if "exception" in ctx:
        raise ctx["exception"]


def _to_iterable_fn(loop, task):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(task)


def _to_iterable(stage, maxsize):

    # print("_to_iterable", stage)

    pipeline_namespace = get_namespace()
    pipeline_namespace.error = None

    try:
        old_loop = asyncio.get_event_loop()
    except RuntimeError:
        old_loop = None

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task, input_queue = _to_task(stage, maxsize, pipeline_namespace, loop)

    # exception handler
    old_exception_handler = loop.get_exception_handler()

    def exception_handler(loop, ctx):
        if old_exception_handler:
            old_exception_handler(loop, ctx)
        _handle_async_exception(loop, ctx)

    loop.set_exception_handler(exception_handler)

    thread = threading.Thread(target=_to_iterable_fn, args=(loop, task))
    thread.daemon = True
    thread.start()

    try:
        while not input_queue.is_done():

            if pipeline_namespace.error is not None:
                raise pipeline_namespace.error

            if input_queue.empty():
                time.sleep(utils.TIMEOUT)
            else:
                x = input_queue.get_nowait()

                if not utils.is_continue(x):
                    yield x

        thread.join()

    finally:
        # loop.stop()
        if old_loop:
            asyncio.set_event_loop(old_loop)
        loop.set_exception_handler(old_exception_handler)


def to_iterable(stage=pypeln_utils.UNDEFINED, maxsize=0):

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: to_iterable(stage, maxsize=maxsize))

    # print("to_iterable", stage.target)

    return _to_iterable(stage, maxsize)


################
################
# UTILS
################
################

############################
# TaskPool
############################


class TaskPool(object):
    def __init__(self, workers, loop):
        self._semaphore = asyncio.Semaphore(workers, loop=loop)
        self._tasks = set()
        self._closed = False
        self._loop = loop

    async def put(self, coro):

        if self._closed:
            raise RuntimeError("Trying put items into a closed TaskPool")

        await self._semaphore.acquire()

        task = asyncio.ensure_future(coro, loop=self._loop)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task):
        self._tasks.remove(task)
        self._semaphore.release()

    async def join(self):
        await asyncio.gather(*self._tasks, loop=self._loop)
        self._closed = True

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()


############################
# _to_async_iterable
############################
async def _trivial_async_iterable(iterable):

    # print("_trivial_async_iterable", iterable)

    for i, x in enumerate(iterable):
        # print(x)
        yield x

        if i % 1000 == 0:
            await asyncio.sleep(0)


def _consume_iterable(params, iterable, queue):

    try:
        for x in iterable:
            while True:
                if not queue.full():
                    params.loop.call_soon_threadsafe(queue.put_nowait, x)
                    break
                else:
                    time.sleep(utils.TIMEOUT)

        while True:
            if not queue.full():
                params.loop.call_soon_threadsafe(queue.put_nowait, utils.DONE)
                break
            else:
                time.sleep(utils.TIMEOUT)

    except BaseException as e:
        params.pipeline_namespace.error = e


async def _async_iterable(iterable, params):

    queue = _InputQueue(
        maxsize=params.maxsize,
        total_done=1,
        pipeline_namespace=params.pipeline_namespace,
    )

    task = params.loop.run_in_executor(
        None, lambda: _consume_iterable(params, iterable, queue)
    )

    async for x in queue:
        yield x

    await task


def _to_async_iterable(iterable, params):

    if hasattr(iterable, "__aiter__"):
        return iterable
    elif not hasattr(iterable, "__iter__"):
        raise ValueError(
            "Object {iterable} most be either iterable or async iterable.".format(
                iterable=iterable
            )
        )

    if type(iterable) in (list, dict, tuple, set):
        return _trivial_async_iterable(iterable)

    else:
        return _async_iterable(iterable, params)


############################
# _to_sync_iterable
############################


def _run_coroutine(loop, async_iterable, queue):

    loop.run_until_complete(_consume_async_iterable(async_iterable, queue))


async def _consume_async_iterable(async_iterable, queue):

    async for x in async_iterable:
        await queue.put(x)

    await queue.done()


def _to_sync_iterable(async_iterable, params):
    def sync_iterable():

        queue = _InputQueue(params.maxsize, 1, params.pipeline_namespace)

        t = threading.Thread(
            target=_run_coroutine, args=(params.loop, async_iterable, queue)
        )
        t.daemon = True
        t.start()

        while True:
            if not queue.empty():
                x = queue.get_nowait()

                if utils.is_done(x):
                    break
                else:
                    yield x
            else:
                time.sleep(utils.TIMEOUT)

        t.join()

    return sync_iterable()


if __name__ == "__main__":
    import random

    async def slow_square(x):
        await asyncio.sleep(random.uniform(0, 1))
        return x ** 2

    stage = range(10)

    stage = flat_map(lambda x: [x, x + 1, x + 2], stage)

    stage = map(slow_square, stage, workers=4)

    stage = filter(lambda x: x > 9, stage)

    each(print, stage)

