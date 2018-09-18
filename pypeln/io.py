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

class Stream(object):

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
        return "Stream(workers = {workers}, maxsize = {maxsize}, target = {target}, args = {args}, dependencies = {dependencies})".format(
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


def map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _map,
        args = (f,),
        dependencies = [stream],
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


def flat_map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _flat_map,
        args = (f,),
        dependencies = [stream],
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


def filter(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _filter,
        args = (f,),
        dependencies = [stream],
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


def each(f, stream, workers = 1, maxsize = 0, run = True):

    stream = _to_stream(stream)

    stream = Stream(
        workers = workers,
        maxsize = maxsize,
        target = _each,
        args = (f,),
        dependencies = [stream],
    )

    if not run:
        return stream

    for _ in stream:
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
    


def concat(streams, maxsize = 0):

    streams = [ _to_stream(s) for s in streams ]

    return Stream(
        workers = 1,
        maxsize = maxsize,
        target = _concat,
        args = tuple(),
        dependencies = streams,
    )


################
# _to_task
################

def _to_task(stream, maxsize):

    total_done = 1
    input_queue = InputQueue(maxsize, total_done)

    stream_input_queue, stream_output_queues = _build_queues(
        stream = stream,
        stream_input_queue = dict(),
        stream_output_queues = dict(),
        visited = set(),
    )

    stream_output_queues[stream] = OutputQueues([ input_queue ])

    tasks = [
        WORKER(
            target = _stream.target,
            args = _stream.args,
            kwargs = dict(
                workers = _stream.workers,
                output_queues = stream_output_queues[_stream],
                input_queue = stream_input_queue.get(_stream, None),
            ),
        )
        for _stream in stream_output_queues
    ]

    task = asyncio.gather(*tasks)

    return task, input_queue

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

async def _from_iterable_fn(iterable, workers, input_queue, output_queues):

    for x in iterable:
        await output_queues.put(x)
        
    await output_queues.done()

def _from_iterable(iterable):
    
    return Stream(
        workers = 1,
        maxsize = None,
        target = _from_iterable_fn,
        args = (iterable,),
        dependencies = [],
    )



########################
# to_iterable
########################


def _build_queues(stream, stream_input_queue, stream_output_queues, visited):

    if stream in visited:
        return stream_input_queue, stream_output_queues
    else:
        visited.add(stream)

    if len(stream.dependencies) > 0:
        total_done = len(stream.dependencies)
        input_queue = InputQueue(stream.maxsize, total_done)
        stream_input_queue[stream] = input_queue

        for stream in stream.dependencies:
            if stream not in stream_output_queues:
                stream_output_queues[stream] = OutputQueues([input_queue])
            else:
                stream_output_queues[stream].append(input_queue)

            stream_input_queue, stream_output_queues = _build_queues(
                stream,
                stream_input_queue,
                stream_output_queues,
                visited
            )

    return stream_input_queue, stream_output_queues

def _handle_async_exception(loop, ctx):
    loop.stop()
    raise Exception(f"Exception in async task: {ctx}")

def _to_iterable_fn(loop, task):
    
    loop.run_until_complete(task)



def to_iterable(stream: Stream, maxsize = 0):

    old_loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(_handle_async_exception)

    task, input_queue = _to_task(stream, maxsize = maxsize)

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

    stream = range(10)

    stream = flat_map(lambda x: [x, x + 1, x + 2], stream)

    stream = map(slow_square, stream, workers = 4)

    stream = filter(lambda x: x > 9, stream)

    each(print, stream)


    
