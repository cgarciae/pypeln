from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
from .task_pool import TaskPool
import time
from functools import reduce

def WORKER(target, args, kwargs):
    return target(*args, **kwargs)

class Stage(utils.BaseStage):

    def __init__(self, worker_constructor, workers, maxsize, target, args, dependencies):
        self.worker_constructor = worker_constructor
        self.workers = workers
        self.maxsize = maxsize
        self.target = target
        self.args = args
        self.dependencies = dependencies

    def __await__(self):
        task, _input_queue = _to_task(self, maxsize = 0)
        return task.__await__()
    
    def __iter__(self):
        return to_iterable(self)

    def __repr__(self):
        return "Stage(worker_constructor = {worker_constructor}, workers = {workers}, maxsize = {maxsize}, target = {target}, args = {args}, dependencies = {dependencies})".format(
            worker_constructor = self.worker_constructor,
            workers = self.workers,
            maxsize = self.maxsize,
            target = self.target,
            args = self.args,
            dependencies = len(self.dependencies),
        )

class InputQueue(asyncio.Queue):

    def __init__(self, maxsize = 0, total_done = 1, **kwargs):
        super(InputQueue, self).__init__(maxsize = maxsize, **kwargs)

        self.remaining = total_done

    async def __aiter__(self):

        while not self.is_done():
            x = await self.get()

            if not utils.is_continue(x):
                yield x

    async def get(self):
        
        x = await super(InputQueue, self).get()

        if utils.is_done(x):
            self.remaining -= 1    
            return utils.CONTINUE

        return x

    def get_nowait(self):
        
        x = super(InputQueue, self).get_nowait()

        if utils.is_done(x):
            self.remaining -= 1    
            return utils.CONTINUE

        return x
            

    def is_done(self):
        return self.remaining == 0 # and self.empty()


class OutputQueues(list):

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


async def _run_tasks(f_task, workers, input_queue, output_queues):

    async with TaskPool(workers = workers) as tasks:
        async for x in input_queue:
            
            task = f_task(x)
            await tasks.put(task)

    # wait all tasks to finish
    await output_queues.done()

########################
# map
########################

async def _map(f, workers, input_queue, output_queues):

    async def f_task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        await output_queues.put(y)

    await _run_tasks(f_task, workers, input_queue, output_queues)


@utils.maybe_partial(2)
def map(f, stage = None, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        target = _map,
        args = (f,),
        dependencies = [stage],
    )



########################
# flat_map
########################

async def _flat_map(f, workers, input_queue, output_queues):
    

    async def f_task(x):

        ys = f(x)

        if hasattr(ys, "__aiter__"):
            async for y in ys:
                await output_queues.put(y)
            
        elif hasattr(ys, "__iter__"):
            for y in ys:
                await output_queues.put(y)

    await _run_tasks(f_task, workers, input_queue, output_queues)


@utils.maybe_partial(2)
def flat_map(f, stage = None, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        target = _flat_map,
        args = (f,),
        dependencies = [stage],
    )

########################
# filter
########################

async def _filter(f, workers, input_queue, output_queues):
    
    async def f_task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await output_queues.put(x)

    await _run_tasks(f_task, workers, input_queue, output_queues)


@utils.maybe_partial(2)
def filter(f, stage = None, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        target = _filter,
        args = (f,),
        dependencies = [stage],
    )


########################
# each
########################


async def _each(f, workers, input_queue, output_queues):
    
    async def f_task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

    await _run_tasks(f_task, workers, input_queue, output_queues)


@utils.maybe_partial(2)
def each(f, stage = None, workers = 1, maxsize = 0, run = True):

    stage = _to_stage(stage)

    stage = Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        target = _each,
        args = (f,),
        dependencies = [stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass

########################
# concat
########################

async def _concat(workers, input_queue, output_queues):

    async for x in input_queue:
        await output_queues.put(x)
        
    await output_queues.done()
    


def concat(stages, maxsize = 0):

    stages = [ _to_stage(s) for s in stages ]

    return Stage(
        worker_constructor = WORKER,
        workers = 1,
        maxsize = maxsize,
        target = _concat,
        args = tuple(),
        dependencies = stages,
    )


################
# _to_task
################

def _to_task(stage, maxsize):

    total_done = 1
    input_queue = InputQueue(maxsize, total_done)

    stage_input_queue, stage_output_queues = _build_queues(
        stage = stage,
        stage_input_queue = dict(),
        stage_output_queues = dict(),
        visited = set(),
    )

    stage_output_queues[stage] = OutputQueues([ input_queue ])

    tasks = [
        _stage.worker_constructor(
            target = _stage.target,
            args = _stage.args,
            kwargs = dict(
                workers = _stage.workers,
                output_queues = stage_output_queues[_stage],
                input_queue = stage_input_queue.get(_stage, None),
            ),
        )
        for _stage in stage_output_queues
    ]

    task = asyncio.gather(*tasks)

    return task, input_queue

################
# _to_stage
################

def _to_stage(obj):

    if isinstance(obj, Stage):
        return obj

    elif hasattr(obj, "__iter__"):
        return from_iterable(obj)

    else:
        raise ValueError("Object {obj} is not iterable".format(obj = obj))
    
########################
# from_iterable
########################

async def _from_iterable(iterable, workers, input_queue, output_queues):

    for x in iterable:
        await output_queues.put(x)
        
    await output_queues.done()

@utils.maybe_partial(1)
def from_iterable(iterable = None, worker_constructor = None):
    
    return Stage(
        worker_constructor = WORKER,
        workers = 1,
        maxsize = None,
        target = _from_iterable,
        args = (iterable,),
        dependencies = [],
    )



########################
# to_iterable
########################


def _build_queues(stage, stage_input_queue, stage_output_queues, visited):

    if stage in visited:
        return stage_input_queue, stage_output_queues
    else:
        visited.add(stage)

    if len(stage.dependencies) > 0:
        total_done = len(stage.dependencies)
        input_queue = InputQueue(stage.maxsize, total_done)
        stage_input_queue[stage] = input_queue

        for stage in stage.dependencies:
            if stage not in stage_output_queues:
                stage_output_queues[stage] = OutputQueues([input_queue])
            else:
                stage_output_queues[stage].append(input_queue)

            stage_input_queue, stage_output_queues = _build_queues(
                stage,
                stage_input_queue,
                stage_output_queues,
                visited
            )

    return stage_input_queue, stage_output_queues

def _handle_async_exception(loop, ctx, error_namespace, task):
    print("CTX", ctx)
    if error_namespace.exception is None:
        if "exception" in ctx:
            error_namespace.exception = ctx["exception"]
        if "message" in ctx:
            error_namespace.exception = Exception(ctx["message"])
        else:
            error_namespace.exception = Exception(str(ctx))
        
        task.cancel()

def _to_iterable_fn(loop, task):
    
    loop.run_until_complete(task)


@utils.maybe_partial(1)
def to_iterable(stage: Stage = None, maxsize = 0):

    error_namespace = utils.Namespace()
    error_namespace.exception = None

    # old_loop = asyncio.get_event_loop()
    loop = asyncio.get_event_loop()
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    

    task, input_queue = _to_task(stage, maxsize = maxsize)

    # exception handler
    old_exception_handler = loop.get_exception_handler()

    def exception_handler(loop, ctx):
        if old_exception_handler:
            old_exception_handler(loop, ctx)
        _handle_async_exception(loop, ctx, error_namespace, task)
    
    loop.set_exception_handler(exception_handler)

    # start thread
    thread = threading.Thread(target=_to_iterable_fn, args=(loop, task))
    thread.daemon = True
    thread.start()

    try:
        while not input_queue.is_done():

            while input_queue.empty():
                time.sleep(utils.TIMEOUT)

            x = input_queue.get_nowait()

            if error_namespace.exception is not None:
                raise error_namespace.exception

            if not utils.is_continue(x):
                yield x
        
        thread.join()
        
    finally:
        # asyncio.set_event_loop(old_loop)
        loop.set_exception_handler(old_exception_handler)
        pass




if __name__ == '__main__':
    import random

    async def slow_square(x):
        await asyncio.sleep(random.uniform(0, 1))
        return x**2

    stage = range(10)

    stage = flat_map(lambda x: [x, x + 1, x + 2], stage)

    stage = map(slow_square, stage, workers = 4)

    stage = filter(lambda x: x > 9, stage)

    each(print, stage)


    
