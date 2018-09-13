from __future__ import absolute_import, print_function

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

# from collections import namedtuple
# from . import utils

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


class _Task(object):
    
    def __init__(self, f, args, kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs



###########
# map
###########

def _map(f, qin, queues_out, namespace, lock):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
            continue

        if not utils.is_done(x):
            y = f(x)
            for qout in queues_out:
                qout.put(y)
        
        else:
            with lock:
                namespace.remaining -= 1

    for qout in queues_out:
        qout.put(utils.DONE)



def map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = Queue(maxsize = queue_maxsize)
    queues_out = []
    namespace = _get_namespace()
    lock = Lock()

    stream.queues.append(qin)

    namespace.remaining = stream.workers

    tasks = set([
        _Task(
            f = _map,
            args = (f, qin, queues_out, namespace, lock),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)

###########
# flat_map
###########

def _flat_map(f, qin, queues_out, namespace, lock):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
            continue

        if not utils.is_done(x):
            for y in f(x):
                for qout in queues_out:
                    qout.put(y)
        
        else:
            with lock:
                namespace.remaining -= 1

    for qout in queues_out:
        qout.put(utils.DONE)



def flat_map(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = Queue(maxsize = queue_maxsize)
    queues_out = []
    namespace = _get_namespace()
    lock = Lock()

    stream.queues.append(qin)

    namespace.remaining = stream.workers

    tasks = set([
        _Task(
            f = _flat_map,
            args = (f, qin, queues_out, namespace, lock),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)


###########
# filter
###########

def _filter(f, qin, queues_out, namespace, lock):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
            continue

        if not utils.is_done(x):
            if f(x):
                for qout in queues_out:
                    qout.put(x)
        
        else:
            with lock:
                namespace.remaining -= 1

    for qout in queues_out:
        qout.put(utils.DONE)



def filter(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = Queue(maxsize = queue_maxsize)
    queues_out = []
    namespace = _get_namespace()
    lock = Lock()

    stream.queues.append(qin)

    namespace.remaining = stream.workers

    tasks = set([
        _Task(
            f = _filter,
            args = (f, qin, queues_out, namespace, lock),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)


###########
# each
###########

def _each(f, qin, queues_out, namespace, lock):

    while not (namespace.remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
            continue

        if not utils.is_done(x):
            f(x)
        
        else:
            with lock:
                namespace.remaining -= 1

    for qout in queues_out:
        qout.put(utils.DONE)



def each(f, stream, workers = 1, queue_maxsize = 0):

    stream = _to_stream(stream)

    qin = Queue(maxsize = queue_maxsize)
    queues_out = []
    namespace = _get_namespace()
    lock = Lock()

    stream.queues.append(qin)

    namespace.remaining = stream.workers

    tasks = set([
        _Task(
            f = _each,
            args = (f, qin, queues_out, namespace, lock),
            kwargs = dict(),
        )
        for _ in range(workers)
    ])

    tasks = tasks.union(stream.tasks)

    return Stream(workers, tasks, queues_out)



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
        for qout in queues_out:
            qout.put(x)
    
    for qout in queues_out:
        qout.put(utils.DONE)

def _from_iterable(iterable):
    
    queues_out = []

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

    remaining = stream.workers
    qin = Queue(maxsize = queue_maxsize)

    stream.queues.append(qin)

    processes = [
        WORKER(target = task.f, args = task.args, kwargs = task.kwargs)
        for task in stream.tasks
    ]

    for p in processes:
        p.daemon = True
        p.start()

    while not (remaining == 0 and qin.empty()):

        try:
            x = qin.get(timeout = utils.TIMEOUT)
        except (Empty, Full):
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

    print(stream)

    
