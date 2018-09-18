from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
from .utils_async import TaskPool
import time
from functools import reduce

def WORKER(target, args, kwargs):
    return target(*args, **kwargs)

class Stage(object):

    def __init__(self, workers, maxsize, target, args, dependencies):
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
        return "Stage(workers = {workers}, maxsize = {maxsize}, target = {target}, args = {args}, dependencies = {dependencies})".format(
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
        return self.remaining == 0 #and self.queue.empty()


class OutputQueues(list):

    async def put(self, x):
        for queue in self:
            await queue.put(x)

    async def done(self):
        for queue in self:
            await queue.put(utils.DONE)


async def _runner_task(f_task, workers, input_queue, output_queues):

    async with TaskPool(workers = workers) as tasks:

        while not input_queue.is_done():

            x = await input_queue.get()

            if not utils.is_continue(x):
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

    await _runner_task(f_task, workers, input_queue, output_queues)


def map(f, stage, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
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

    await _runner_task(f_task, workers, input_queue, output_queues)


def flat_map(f, stage, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
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

    await _runner_task(f_task, workers, input_queue, output_queues)


def filter(f, stage, workers = 1, maxsize = 0):

    stage = _to_stage(stage)

    return Stage(
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

    await _runner_task(f_task, workers, input_queue, output_queues)


def each(f, stage, workers = 1, maxsize = 0, run = True):

    stage = _to_stage(stage)

    stage = Stage(
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

    while not input_queue.is_done():

        x = await input_queue.get()

        if not utils.is_continue(x):
            await output_queues.put(x)
        
    # wait all tasks to finish
    await output_queues.done()
    


def concat(stages, maxsize = 0):

    stages = [ _to_stage(s) for s in stages ]

    return Stage(
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
        WORKER(
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
        return _from_iterable(obj)

    else:
        raise ValueError("Object {obj} is not iterable".format(obj = obj))
    
########################
# _from_iterable
########################

async def _from_iterable_fn(iterable, workers, input_queue, output_queues):

    for x in iterable:
        await output_queues.put(x)
        
    await output_queues.done()

def _from_iterable(iterable):
    
    return Stage(
        workers = 1,
        maxsize = None,
        target = _from_iterable_fn,
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

def _handle_async_exception(loop, ctx):
    loop.stop()
    raise Exception(f"Exception in async task: {ctx}")

def _to_iterable_fn(loop, task):
    
    loop.run_until_complete(task)



def to_iterable(stage: Stage, maxsize = 0):

    old_loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_handle_async_exception)

    task, input_queue = _to_task(stage, maxsize = maxsize)

    try:
        thread = threading.Thread(target=_to_iterable_fn, args=(loop, task))
        thread.daemon = True
        thread.start()


        while not input_queue.is_done():

            while input_queue.empty():
                time.sleep(utils.TIMEOUT)

            x = input_queue.get_nowait()

            if not utils.is_continue(x):
                yield x
        
        thread.join()

    finally:
        asyncio.set_event_loop(old_loop)




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


    
