from __future__ import absolute_import, print_function

import multiprocessing as mp
from collections import namedtuple
from . import utils



class Stream(namedtuple("Stream", ["workers", "tasks", "queue"])): 
    
    def __iter__(self):
        return to_iterable(self)


class Task(namedtuple("TaskInfo", ["f", "args", "kwargs"])):
    pass

################
# to_stream
################ 
def to_stream(obj):
    if isinstance(obj, Stream):
        return obj
    else:
        return from_iterable(obj)

################
# from_iterable
################

def _from_iterable(iterable, qout):

    for x in iterable:
        qout.put(x)
        
    qout.put(utils.DONE)

def from_iterable(iterable, queue_maxsize = 0):
    
    qout = mp.Queue(maxsize = queue_maxsize)
    task = Task(
        f = _from_iterable,
        args = (iterable, qout),
        kwargs = dict(),
    )

    return Stream(1, [task], qout)


###########
# map
###########

def _map(f, qin, qout, namespace):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except mp.queues.Empty:
            continue

        if not utils.is_done(x):
            y = f(x)
            qout.put(y)
        
        else:
            namespace.remaining -= 1

    qout.put(utils.DONE)



def map(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = mp.Queue(maxsize = queue_maxsize)
    namespace = mp.Manager().Namespace()

    namespace.remaining = stream.workers

    tasks = [
        Task(
            f = _map,
            args = (f, qin, qout, namespace),
            kwargs = dict(),
        )
        for _ in range(workers)
    ]

    tasks += stream.tasks

    return Stream(workers, tasks, qout)

###########
# flat_map
###########

def _flat_map(f, qin, qout, namespace):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except mp.queues.Empty:
            continue

        if not utils.is_done(x):
            for y in f(x):
                qout.put(y)
        
        else:
            namespace.remaining -= 1

    qout.put(utils.DONE)



def flat_map(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = mp.Queue(maxsize = queue_maxsize)
    namespace = mp.Manager().Namespace()

    namespace.remaining = stream.workers

    tasks = [
        Task(
            f = _flat_map,
            args = (f, qin, qout, namespace),
            kwargs = dict(),
        )
        for _ in range(workers)
    ]

    tasks += stream.tasks

    return Stream(workers, tasks, qout)


###########
# filter
###########

def _filter(f, qin, qout, namespace):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except mp.queues.Empty:
            continue

        if not utils.is_done(x):
            if f(x):
                qout.put(x)
        
        else:
            namespace.remaining -= 1

    qout.put(utils.DONE)



def filter(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = mp.Queue(maxsize = queue_maxsize)
    namespace = mp.Manager().Namespace()

    namespace.remaining = stream.workers

    tasks = [
        Task(
            f = _filter,
            args = (f, qin, qout, namespace),
            kwargs = dict(),
        )
        for _ in range(workers)
    ]

    tasks += stream.tasks

    return Stream(workers, tasks, qout)


###########
# each
###########
def _each(f, qin, qout, namespace):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except mp.queues.Empty:
            continue

        if not utils.is_done(x):
            f(x)
        
        else:
            namespace.remaining -= 1

    qout.put(utils.DONE)



def each(f, stream, workers = 1, queue_maxsize = 0):

    stream = to_stream(stream)

    qin = stream.queue
    qout = mp.Queue(maxsize = queue_maxsize)
    namespace = mp.Manager().Namespace()

    namespace.remaining = stream.workers

    tasks = [
        Task(
            f = _each,
            args = (f, qin, qout, namespace),
            kwargs = dict(),
        )
        for _ in range(workers)
    ]

    tasks += stream.tasks

    for _ in Stream(workers, tasks, qout):
        pass


##############
# to_iterable
##############

def to_iterable(stream):

    processes = [
        mp.Process(target = task.f, args = task.args, kwargs = task.kwargs)
        for task in stream.tasks
    ]

    for p in processes:
        p.daemon = True
        p.start()

    remaining = stream.workers
    qin = stream.queue

    while not (remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except mp.queues.Empty:
            continue

        if not utils.is_done(x):
            yield x
        else:
            remaining -= 1

    
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

    each(print, stream)

    
