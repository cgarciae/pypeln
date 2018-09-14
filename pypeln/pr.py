from __future__ import absolute_import, print_function

from functools import reduce
from collections import namedtuple
from . import utils

#############
# imports pr
#############

from multiprocessing import Process as WORKER
from multiprocessing import Manager, Queue, Lock
from multiprocessing.queues import Full, Empty

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

class Stream(namedtuple("Stream", ["workers", "tasks", "queues"])): 
    
    def __iter__(self):
        return _to_iterable(self)

    def __repr__(self):
        return "Stream(workers = {workers}, tasks = {tasks}, queues = {queues})".format(
            workers = self.workers,
            tasks = len(self.tasks),
            queues = len(self.queues),
        )

    def create_queue(self, maxsize):
        queue = Queue(maxsize = maxsize)
        self.queues.append(queue)
        return InputQueue(queue, self.workers)

class _Task(object):
    
    def __init__(self, f, args, kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs


class InputQueue(object):

    def __init__(self, queue, remaining):
        self.queue = queue
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


class OutputQueues(object):

    def __init__(self, queues = None):
        self.queues = queues or []

    def put(self, x):
        for queue in self.queues:
            queue.put(x)

    def done(self):
        for queue in self.queues:
            queue.put(utils.DONE)

    def append(self, x):
        self.queues.append(x)

    def __iter__(self):
        return self.queues

    def __str__(self):
        return str(self.queues)

###########
# map
###########

def _map(f, qin, queues_out):

    while not qin.is_done():
        
        x = qin.get()

        if not utils.is_continue(x):
            y = f(x)
            queues_out.put(y)


    queues_out.done()



def map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = stream.create_queue(queue_maxsize)
    queues_out = OutputQueues()

    tasks = set([
        _Task(
            f = _map,
            args = (f, qin, queues_out),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)

###########
# flat_map
###########

def _flat_map(f, qin, queues_out):

    while not qin.is_done():

        x = qin.get()

        if not utils.is_continue(x):
            for y in f(x):
                queues_out.put(y)

    queues_out.done()



def flat_map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = stream.create_queue(queue_maxsize)
    queues_out = OutputQueues()

    tasks = set([
        _Task(
            f = _flat_map,
            args = (f, qin, queues_out),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)


###########
# filter
###########

def _filter(f, qin, queues_out):

    while not qin.is_done():
        
        x = qin.get()

        if not utils.is_continue(x):
            if f(x):
                queues_out.put(x)


    queues_out.done()



def filter(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = stream.create_queue(queue_maxsize)
    queues_out = OutputQueues()

    tasks = set([
        _Task(
            f = _filter,
            args = (f, qin, queues_out),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)


###########
# each
###########

def _each(f, qin, queues_out):

    while not qin.is_done():
        
        x = qin.get()

        if not utils.is_continue(x):
            f(x)


    queues_out.done()



def each(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = stream.create_queue(queue_maxsize)
    queues_out = OutputQueues()

    tasks = set([
        _Task(
            f = _each,
            args = (f, qin, queues_out),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    for _ in Stream(workers, tasks, queues_out):
        pass


###########
# concat
###########
#NOTE: NOT FINISED

def _concat(qin, queues_out):

    while not qin.is_done():
        
        x = qin.get()

        if not utils.is_continue(x):
            queues_out.put(x)


    queues_out.done()



def concat(streams, queue_maxsize = 0):

    streams = [ _to_stream(s) for s in streams ]
    
    qin = Queue(maxsize = queue_maxsize)
    queues_out = OutputQueues()
    workers = sum([ stream.workers for stream in streams ])

    for stream in streams:
        stream.queues.append(qin)

    qin = InputQueue(qin, workers)

    tasks = {
        _Task(
            f = _concat,
            args = (qin, queues_out),
            kwargs = dict(),
        )
    }

    tasks = reduce(lambda tasks, stream: tasks.union(stream.tasks), streams, tasks)
    

    return Stream(1, tasks, queues_out)

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

def _from_iterable_fn(iterable, queues_out):

    for x in iterable:
        queues_out.put(x)
    
    queues_out.done()

def _from_iterable(iterable):
    
    queues_out = OutputQueues()

    task = _Task(
        f = _from_iterable_fn,
        args = (iterable, queues_out),
        kwargs = dict(),
    )

    return Stream(1, {task}, queues_out)

##############
# _to_iterable
##############

def _to_iterable(stream, queue_maxsize = 0):

    qin = stream.create_queue(queue_maxsize)

    processes = [
        WORKER(target = task.f, args = task.args, kwargs = task.kwargs)
        for task in stream.tasks
    ]

    for p in processes:
        p.daemon = True
        p.start()

    while not qin.is_done():

        x = qin.get()

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
    

    
