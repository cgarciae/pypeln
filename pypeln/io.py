from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
from .utils_async import TaskPool
import time
from functools import reduce


class Stream(namedtuple("Stream", ["workers", "tasks", "output_queues"])): 

    def __await__(self):
        return asyncio.gather(*self.tasks).__await__()
    
    def __iter__(self):
        return to_iterable(self)

    def __repr__(self):
        return "Stream(workers = {workers}, tasks = {tasks}, output_queues = {output_queues})".format(
            workers = self.workers,
            tasks = len(self.tasks),
            output_queues = len(self.output_queues),
        )

    def create_queue(self, maxsize, remaining = 1):
        queue = InputQueue(maxsize = maxsize, remaining = 1)
        self.output_queues.append(queue)
        return queue



class InputQueue(asyncio.Queue):

    def __init__(self, maxsize = 0, remaining = 1, **kwargs):
        super(InputQueue, self).__init__(maxsize = maxsize, **kwargs)

        self.remaining = remaining

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


async def _runner_task(f_task, input_queue, output_queues, workers):
    
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

def _map(f, output_queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        await output_queues.put(y)
    
    return _task


def map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    output_queues = OutputQueues()
    input_queue = stream.create_queue(maxsize)
    
    f_task = _map(f, output_queues)
    task = _runner_task(f_task, input_queue, output_queues, workers)

    tasks = stream.tasks | {task}

    return Stream(workers, tasks, output_queues)



########################
# flat_map
########################

def _flat_map(f, output_queues):
    async def _task(x):

        ys = f(x)

        if hasattr(ys, "__aiter__"):
            async for y in ys:
                await output_queues.put(y)
            
        elif hasattr(ys, "__iter__"):
            for y in ys:
                await output_queues.put(y)
    
    return _task


def flat_map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    output_queues = OutputQueues()
    input_queue = stream.create_queue(maxsize)
    
    f_task = _flat_map(f, output_queues)
    task = _runner_task(f_task, input_queue, output_queues, workers)

    tasks = stream.tasks | {task}

    return Stream(workers, tasks, output_queues)


########################
# filter
########################

def _filter(f, output_queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await output_queues.put(x)
    
    return _task


def filter(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    output_queues = OutputQueues()
    input_queue = stream.create_queue(maxsize)
    
    f_task = _filter(f, output_queues)
    task = _runner_task(f_task, input_queue, output_queues, workers)

    tasks = stream.tasks | {task}

    return Stream(workers, tasks, output_queues)


########################
# each
########################

def _each(f, output_queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y
    
    return _task


def each(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    output_queues = OutputQueues()
    input_queue = stream.create_queue(maxsize)
    
    f_task = _each(f, output_queues)
    task = _runner_task(f_task, input_queue, output_queues, workers)

    tasks = stream.tasks | {task}

    for _ in Stream(workers, tasks, output_queues):
        pass


########################
# concat
########################

async def _concat(input_queue, output_queues):

    while not input_queue.is_done():

        x = await input_queue.get()

        if not utils.is_continue(x):
            await output_queues.put(x)
        
    # wait all tasks to finish
    await output_queues.done()
    


def concat(streams, workers = 1, maxsize = 0):

    streams = [ _to_stream(s) for s in streams ]
    output_queues = OutputQueues()

    input_queue = InputQueue(maxsize = maxsize, remaining = len(streams))

    for stream in streams:
        stream.output_queues.append(input_queue)
    
    task = _concat(input_queue, output_queues)

    tasks = reduce(
        lambda tasks, stream: tasks | stream.tasks, 
        streams, 
        {task},
    )

    return Stream(1, tasks, output_queues)



################
# _to_stream
################
def _to_stream(obj):
    if isinstance(obj, Stream):
        return obj
    elif hasattr(obj, "__iter__"):
        return _from_iterable(obj)
    else:
        raise ValueError("Object {obj} is not iterable".format(obj = obj))
    
########################
# _from_iterable
########################

async def _from_iterable_fn(iterable, output_queues):

    for x in iterable:
        await output_queues.put(x)
        
    await output_queues.done()

def _from_iterable(iterable):
    
    output_queues = OutputQueues()
    task = _from_iterable_fn(iterable, output_queues)

    return Stream(1, {task}, output_queues)

########################
# to_iterable
########################

def _handle_async_exception(loop, ctx):
    loop.stop()
    raise Exception(f"Exception in async task: {ctx}")

def _to_iterable_fn(loop, stream):
    loop.run_until_complete(stream)

def to_iterable(stream: Stream, maxsize = 0):

    input_queue = stream.create_queue(maxsize)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(_handle_async_exception)

    thread = threading.Thread(target=_to_iterable_fn, args=(loop, stream))
    thread.daemon = True
    thread.start()


    while not input_queue.is_done():

        while input_queue.empty():
            time.sleep(utils.TIMEOUT)

        x = input_queue.get_nowait()

        if not utils.is_continue(x):
            yield x
    
    thread.join()




if __name__ == '__main__':
    import random

    async def slow_square(x):
        await asyncio.sleep(random.uniform(0, 1))
        return x**2

    stream = range(10)

    stream = flat_map(lambda x: [x, x + 1, x + 2], stream)

    stream = map(slow_square, stream, workers = 4)

    stream = filter(lambda x: x > 9, stream)

    each(print, stream)


    
