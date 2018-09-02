from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import threading
import sys
from . import utils
import time

class Stream(namedtuple("Stream", ["coroutine", "queue"])):

    def __await__(self):
        return self.coroutine.__await__()

    def __iter__(self):
        return to_iterable(self)


class TaskPool(object):

    def __init__(self, workers = 1):
        self._limit = workers
        self._tasks = []

    def filter_tasks(self):
        self._tasks = [ task for task in self._tasks if not task.done() ]

    async def join(self):
        self.filter_tasks()

        if len(self._tasks) > 0:
            await asyncio.wait(self._tasks)

        self.filter_tasks()
        

    async def put(self, coro):
        self.filter_tasks()

        # wait until space is available
        while self._limit > 0 and len(self._tasks) >= self._limit:
            await asyncio.sleep(0)

            self.filter_tasks()

        
        task = asyncio.ensure_future(coro)
        self._tasks.append(task)


    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()

################
# to_stream
################

def to_stream(obj):
    if isinstance(obj, Stream):
        return obj
    else:
        return from_iterable(obj)
    
########################
# from_iterable
########################

async def _from_iterable(iterable, qout):
        
    for x in iterable:
        await qout.put(x)
        
    await qout.put(utils.DONE)

def from_iterable(iterable, queue_maxsize = 0):
    
    qout = asyncio.Queue(maxsize=queue_maxsize)
    coro_out = _from_iterable(iterable, qout)

    return Stream(coro_out, qout)

########################
# map
########################

def _map_wrapper(f, queue):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        await queue.put(y)
    
    return _task

async def _map(f_task, coro_in, qin, qout, workers):
    
    coroin_task = asyncio.ensure_future(coro_in)

    async with TaskPool(workers = workers) as tasks:

        x = await qin.get()
        while x is not utils.DONE:

            fcoro = f_task(x)
            await tasks.put(fcoro)

            x = await qin.get()

    # end async with: wait all tasks to finish
    await qout.put(utils.DONE)
    await coroin_task

def map(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = asyncio.Queue(maxsize = queue_maxsize)
    f_task = _map_wrapper(f, queue = qout)
    coro_in = stream.coroutine
    coro_out = _map(f_task, coro_in, qin, qout, workers)

    return Stream(coro_out, qout)



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

async def _flat_map(f_task, coro_in, qin, qout, workers):
    
    coroin_task = asyncio.ensure_future(coro_in)

    async with TaskPool(workers = workers) as tasks:

        x = await qin.get()
        while x is not utils.DONE:

            fcoro = f_task(x)
            await tasks.put(fcoro)

            x = await qin.get()

    # end async with: wait all tasks to finish
    await qout.put(utils.DONE)
    await coroin_task

def flat_map(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = asyncio.Queue(maxsize = queue_maxsize)
    f_task = _flat_map_wrapper(f, queue = qout)
    coro_in = stream.coroutine
    coro_out = _flat_map(f_task, coro_in, qin, qout, workers)

    return Stream(coro_out, qout)


########################
# filter
########################

def _filter_wrapper(f, queue):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        if y:
            await queue.put(x)
    
    return _task

async def _filter(f_task, coro_in, qin, qout, workers):
    
    coroin_task = asyncio.ensure_future(coro_in)

    async with TaskPool(workers = workers) as tasks:

        x = await qin.get()
        while x is not utils.DONE:

            fcoro = f_task(x)
            await tasks.put(fcoro)

            x = await qin.get()

    # end async with: wait all tasks to finish
    await qout.put(utils.DONE)
    await coroin_task

def filter(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = asyncio.Queue(maxsize = queue_maxsize)
    f_task = _filter_wrapper(f, queue = qout)
    coro_in = stream.coroutine
    coro_out = _filter(f_task, coro_in, qin, qout, workers)

    return Stream(coro_out, qout)


########################
# each
########################

def _each_wrapper(f, queue):
    async def _task(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

    return _task

async def _each(f_task, coro_in, qin, qout, workers):
    
    coroin_task = asyncio.ensure_future(coro_in)

    async with TaskPool(workers = workers) as tasks:

        x = await qin.get()
        while x is not utils.DONE:

            fcoro = f_task(x)
            await tasks.put(fcoro)

            x = await qin.get()

    # end async with: wait all tasks to finish
    await qout.put(utils.DONE)
    await coroin_task

def each(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = asyncio.Queue(maxsize = queue_maxsize)
    f_task = _each_wrapper(f, queue = qout)
    coro_in = stream.coroutine
    coro_out = _each(f_task, coro_in, qin, qout, workers)

    return Stream(coro_out, qout)


########################
# to_iterable
########################

def _handle_async_exception(loop, ctx):
    loop.stop()
    raise Exception(f"Exception in async task: {ctx}")

def _to_iterable(loop, stream):
    
    loop.run_until_complete(stream)

def to_iterable(stream: Stream):

    qin = stream.queue

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(_handle_async_exception)

    thread = threading.Thread(target=_to_iterable, args=(loop, stream))
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


    
