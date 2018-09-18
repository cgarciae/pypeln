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

class Stream(object):

    def __init__(self, workers, maxsize, target, args, dependencies):
        self.workers = workers
        self.maxsize = maxsize
        self.target = target
        self.args = args
        self.dependencies = dependencies
    
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


class InputQueue(object):

    def __init__(self, maxsize, total_done, **kwargs):
        
        self.queue = Queue(maxsize = maxsize, **kwargs)
        self.lock = Lock()
        self.namespace = _get_namespace()
        self.namespace.remaining = total_done

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

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _map,
        args = (f,),
        dependencies = [stream],
    )

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

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _flat_map,
        args = (f,),
        dependencies = [stream],
    )


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

    return Stream(
        workers = workers,
        maxsize = maxsize,
        target = _filter,
        args = (f,),
        dependencies = [stream],
    )


###########
# each
###########

def _each(f, input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            f(x)


    output_queues.done()


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


###########
# concat
###########

def _concat(input_queue, output_queues):

    while not input_queue.is_done():
        
        x = input_queue.get()

        if not utils.is_continue(x):
            output_queues.put(x)


    output_queues.done()


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
# run
################

def run(streams, maxsize = 0):
    
    if isinstance(streams, list) and len(streams) == 0:
        raise ValueError("Expected atleast stream to run")

    elif isinstance(streams, list):
        stream = concat(streams, maxsize = maxsize)
    
    else:
        stream = streams

    stream = to_iterable(stream, maxsize = maxsize)
    
    for _ in streams:
        pass

    

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

def _from_iterable_fn(iterable, input_queue, output_queues):

    for x in iterable:
        output_queues.put(x)
    
    output_queues.done()

def _from_iterable(iterable):

    return Stream(
        workers = 1,
        maxsize = None,
        target = _from_iterable_fn,
        args = (iterable,),
        dependencies = [],
    )

##############
# to_iterable
##############

def _build_queues(stream, stream_input_queue, stream_output_queues, visited):

    if stream in visited:
        return stream_input_queue, stream_output_queues
    else:
        visited.add(stream)

    if len(stream.dependencies) > 0:
        total_done = sum([ stream.workers for stream in stream.dependencies ])
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

def _create_worker(f, args, output_queues, input_queue):

    kwargs = dict(
        output_queues = output_queues)

    if input_queue is not None:
        kwargs.update(input_queue = input_queue)

    return WORKER(target = f, args = args, kwargs = kwargs)

def to_iterable(stream, maxsize = 0):

    input_queue = InputQueue(maxsize, stream.workers)

    stream_input_queue, stream_output_queues = _build_queues(
        stream = stream,
        stream_input_queue = dict(),
        stream_output_queues = dict(),
        visited = set(),
    )

    stream_output_queues[stream] = OutputQueues([ input_queue ])

    processes = [
        WORKER(
            target = _stream.target,
            args = _stream.args,
            kwargs = dict(
                output_queues = stream_output_queues[_stream],
                input_queue = stream_input_queue.get(_stream, None),
            ),
        )
        for _stream in stream_output_queues
        for _ in range(_stream.workers)
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
    

    
