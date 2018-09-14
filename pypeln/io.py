from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
from .utils_async import TaskPool
import time

class Stream(namedtuple("Stream", ["coroutine", "queues"])):

    def __await__(self):
        return self.coroutine.__await__()

    def __iter__(self):
        return _to_iterable(self)


async def _task_runner(f_task, coro_in, qin, queues_out, workers):
    
    coroin_task = asyncio.ensure_future(coro_in)

    async with TaskPool(workers = workers) as tasks:

        x = await qin.get()

        while x is not utils.DONE:

            fcoro = f_task(x)
            await tasks.put(fcoro)

            x = await qin.get()

    # wait all tasks to finish
    for qout in queues_out:
        await qout.put(utils.DONE)

    await coroin_task

########################
# map
########################

def _map(f, queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        for queue in queues:
            await queue.put(y)
    
    return _task


def map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    queues_out = []
    qin = asyncio.Queue(maxsize = queue_maxsize)
    stream.queues.append(qin)
    
    f_task = _map(f, queues = queues_out)
    coro_in = stream.coroutine
    coro_out = _task_runner(f_task, coro_in, qin, queues_out, workers)

    return Stream(coro_out, queues_out)



########################
# flat_map
########################

def _flat_map_wrapper(f, queue):
    async def _task(x):

        ys = f(x)

        if hasattr(ys, "__aiter__"):
            async for y in ys:
                await queue.put(y)
        elif hasattr(ys, "__iter__"):
            for y in ys:
                await queue.put(y)
        else:
            raise ValueError(f"{ys} is not an (async) iterable")
    
    return _task

def _flat_map(f, queues):
    async def _task(x):

        ys = f(x)

        if hasattr(ys, "__aiter__"):
            async for y in ys:
                for queue in queues:
                    await queue.put(y)
        elif hasattr(ys, "__iter__"):
            for y in ys:
                for queue in queues:
                    await queue.put(y)
        else:
            raise ValueError(f"{ys} is not an (async) iterable")

        
    
    return _task


def flat_map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    queues_out = []
    qin = asyncio.Queue(maxsize = queue_maxsize)
    stream.queues.append(qin)
    
    f_task = _flat_map(f, queues = queues_out)
    coro_in = stream.coroutine
    coro_out = _task_runner(f_task, coro_in, qin, queues_out, workers)

    return Stream(coro_out, queues_out)


########################
# filter
########################

def _filter(f, queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            for queue in queues:
                await queue.put(x)
    
    return _task


def filter(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    queues_out = []
    qin = asyncio.Queue(maxsize = queue_maxsize)
    stream.queues.append(qin)
    
    f_task = _filter(f, queues = queues_out)
    coro_in = stream.coroutine
    coro_out = _task_runner(f_task, coro_in, qin, queues_out, workers)

    return Stream(coro_out, queues_out)


########################
# each
########################

def _each(f, queues):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y
    
    return _task


def each(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    queues_out = []
    qin = asyncio.Queue(maxsize = queue_maxsize)
    stream.queues.append(qin)
    
    f_task = _each(f, queues = queues_out)
    coro_in = stream.coroutine
    coro_out = _task_runner(f_task, coro_in, qin, queues_out, workers)

    for _ in Stream(coro_out, queues_out):
        pass


########################
# concat
########################

async def _concat(coro_in, qin, remaining, queues_out):

    coroin_task = asyncio.ensure_future(coro_in)

    while remaining > 0:

        x = await qin.get()

        if x is not utils.DONE:
            for queue in queues_out:
                await queue.put(x)
    
        else:
            remaining -= 1
        
    # wait all tasks to finish
    for qout in queues_out:
        await qout.put(utils.DONE)

    await coroin_task
    


def concat(streams, workers = 1, queue_maxsize = 0):

    streams = [ _to_stream(s) for s in streams ]
    remaining = len(streams)
    queues_out = []

    qin = asyncio.Queue(maxsize = queue_maxsize)

    for stream in streams:
        stream.queues.append(qin)
    

    coro_in = asyncio.gather(*[ s.coroutine for s in streams ])
    coro_out = _concat(coro_in, qin, remaining, queues_out)

    return Stream(coro_out, queues_out)



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

async def _from_iterable_fn(iterable, queues_out):
        
    for x in iterable:
        for qout in queues_out:
            await qout.put(x)
        
    for qout in queues_out:
        await qout.put(utils.DONE)

def _from_iterable(iterable):
    
    queues_out = []
    coro_out = _from_iterable_fn(iterable, queues_out)

    return Stream(coro_out, queues_out)

########################
# _to_iterable
########################

def _handle_async_exception(loop, ctx):
    loop.stop()
    raise Exception(f"Exception in async task: {ctx}")

def _to_iterable_fn(loop, stream):
    
    loop.run_until_complete(stream)

def _to_iterable(stream: Stream, queue_maxsize = 0):

    qin = asyncio.Queue(maxsize = queue_maxsize)

    stream.queues.append(qin)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(_handle_async_exception)

    thread = threading.Thread(target=_to_iterable_fn, args=(loop, stream))
    thread.daemon = True
    thread.start()


    while True:

        while qin.empty():
            time.sleep(utils.TIMEOUT)

        x = qin.get_nowait()

        if not utils.is_done(x):
            yield x
        else:
            break
    
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


    
