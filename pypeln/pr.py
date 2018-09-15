from __future__ import absolute_import, print_function

from functools import reduce
from collections import namedtuple
from . import utils

#############
# imports pr
#############

from multiprocessing import Process as WORKER
from multiprocessing import Manager, Lock, Queue
from multiprocessing.queues import Full, Empty
from multiprocessing.context import DefaultContext

from collections import namedtuple
from . import utils

MANAGER = Manager()

def _get_namespace():
    return MANAGER.Namespace()

#############
# imports th
#############

# from threading import Thread as WORKER
# from.utils import Namespace
# from six.moves.queue import Queue, Empty, Full
# from threading import Lock

# def _get_namespace():
#     return Namespace()


####################
# classes
####################

class Stream(namedtuple("Stream", ["workers", "tasks", "output_queues"])): 
    
    def __iter__(self):
        return to_iterable(self)

    def __repr__(self):
        return "Stream(workers = {workers}, tasks = {tasks}, output_queues = {output_queues})".format(
            workers = self.workers,
            tasks = len(self.tasks),
            output_queues = len(self.output_queues),
        )

    def create_queue(self, maxsize):
        queue = InputQueue(maxsize, self.workers)
        self.output_queues.append(queue)
        return queue

class _Task(object):
    
    def __init__(self, f, args, kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs

class InputQueue(object):

    def __init__(self, maxsize, remaining, **kwargs):
        
        self.queue = Queue(maxsize = maxsize, **kwargs)
        self.lock = Lock()
        self.namespace = _get_namespace()
        self.namespace.remaining = remaining

    def get(self):
        
        try:
            x = self.queue.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
            return utils.CONTINUE
        
        if not utils.is_done(x):
            return x
        else:
            with self.lock:
                self.namespace.remaining -= 1
            
            return utils.CONTINUE

    def is_done(self):
        return self.namespace.remaining == 0 and self.queue.empty()

    def put(self, x):
        self.queue.put(x)


class OutputQueues(list):

    def put(self, x):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.put(utils.DONE)

###########
# map
###########

def _map(f, input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            y = f(x)
            output_queues.put(y)


    output_queues.done()



def map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    input_queue = stream.create_queue(maxsize)
    output_queues = OutputQueues()

    tasks = set([
        _Task(
            f = _map,
            args = (f, input_queue, output_queues),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks | stream.tasks

    return Stream(workers, tasks, output_queues)

###########
# flat_map
###########

def _flat_map(f, input_queue, output_queues):

    while not input_queue.is_done():

        x = input_queue.get()

        if not utils.is_continue(x):
            for y in f(x):
                output_queues.put(y)

    output_queues.done()



def flat_map(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    input_queue = stream.create_queue(maxsize)
    output_queues = OutputQueues()

    tasks = set([
        _Task(
            f = _flat_map,
            args = (f, input_queue, output_queues),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks | stream.tasks

    return Stream(workers, tasks, output_queues)


###########
# filter
###########

def _filter(f, input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            if f(x):
                output_queues.put(x)


    output_queues.done()



def filter(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    input_queue = stream.create_queue(maxsize)
    output_queues = OutputQueues()

    tasks = set([
        _Task(
            f = _filter,
            args = (f, input_queue, output_queues),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks | stream.tasks

    return Stream(workers, tasks, output_queues)


###########
# each
###########

def _each(f, input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            f(x)


    output_queues.done()



def each(f, stream, workers = 1, maxsize = 0):

    stream = _to_stream(stream)

    input_queue = stream.create_queue(maxsize)
    output_queues = OutputQueues()

    tasks = set([
        _Task(
            f = _each,
            args = (f, input_queue, output_queues),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks | stream.tasks

    for _ in Stream(workers, tasks, output_queues):
        pass


###########
# concat
###########
#NOTE: NOT FINISED

def _concat(input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            output_queues.put(x)


    output_queues.done()



def concat(streams, maxsize = 0):

    streams = [ _to_stream(s) for s in streams ]
    workers = sum([ stream.workers for stream in streams ])

    input_queue = InputQueue(maxsize, workers)
    output_queues = OutputQueues()

    for stream in streams:
        stream.output_queues.append(input_queue)

    tasks = {
        _Task(
            f = _concat,
            args = (input_queue, output_queues),
            kwargs = dict(),
        )
    }

    stream_tasks = (s.tasks for s in streams)
    tasks = reduce(set.__or__, stream_tasks, tasks)

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

################
# _from_iterable
################

def _from_iterable_fn(iterable, output_queues):

    for x in iterable:
        output_queues.put(x)
    
    output_queues.done()

def _from_iterable(iterable):
    
    output_queues = OutputQueues()

    task = _Task(
        f = _from_iterable_fn,
        args = (iterable, output_queues),
        kwargs = dict(),
    )

    return Stream(1, {task}, output_queues)

##############
# to_iterable
##############

def to_iterable(stream, maxsize = 0):

    input_queue = stream.create_queue(maxsize)

    processes = [
        WORKER(target = task.f, args = task.args, kwargs = task.kwargs)
        for task in stream.tasks
    ]

    for p in processes:
        p.daemon = True
        p.start()

    while not input_queue.is_done():

        x = input_queue.get()

        if utils.is_continue(x):
            continue
        else:
            yield x

    
    for p in processes:
        p.join()

if __name__ == '__main__':
    import time
    import random

    def slow_square(x):
        time.sleep(random.uniform(0, 1))
        return x**2

    stream = range(10)

    stream = flat_map(lambda x: [x, x + 1, x + 2], stream)

    stream = map(slow_square, stream, workers=4)

    stream = filter(lambda x: x > 9, stream)

    print(stream)
    

    
