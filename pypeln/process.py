from __future__ import absolute_import, print_function

import functools
from collections import namedtuple
from . import utils

#############
# imports pr
#############

from multiprocessing import Process as WORKER
from multiprocessing import Manager, Lock, Queue
from multiprocessing.queues import Full, Empty
from threading import Thread

from collections import namedtuple
from . import utils

_MANAGER = Manager()

def _get_namespace():
    return _MANAGER.Namespace()

#############
# imports th
#############

# from threading import Thread as WORKER
# from threading import Thread
# from.utils import Namespace
# from six.moves.queue import Queue, Empty, Full
# from threading import Lock

# def _get_namespace():
#     return Namespace()


####################
# classes
####################

class _Stage(utils.BaseStage):

    def __init__(self, worker_constructor, workers, maxsize, on_start, on_done, target, args, dependencies):
        self.worker_constructor = worker_constructor
        self.workers = workers
        self.maxsize = maxsize
        self.on_start = on_start
        self.on_done = on_done
        self.target = target
        self.args = args
        self.dependencies = dependencies
    
    def __iter__(self):
        return to_iterable(self)

    def __repr__(self):
        return "_Stage(worker_constructor = {worker_constructor}, workers = {workers}, maxsize = {maxsize}, target = {target}, args = {args}, dependencies = {dependencies})".format(
            worker_constructor = self.worker_constructor,
            workers = self.workers,
            maxsize = self.maxsize,
            target = self.target,
            args = self.args,
            dependencies = len(self.dependencies),
        )

class _StageParams(namedtuple("_StageParams",
    ["input_queue", "output_queues", "on_start", "on_done", "stage_namespace", "stage_lock"])):
    pass

class _InputQueue(object):

    def __init__(self, maxsize, total_done, **kwargs):
        
        self.queue = Queue(maxsize = maxsize, **kwargs)
        self.lock = Lock()
        self.namespace = _get_namespace()
        self.namespace.remaining = total_done

    def __iter__(self):

        while not self.is_done():
            x = self.get()

            if not utils.is_continue(x):
                yield x

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


class _OutputQueues(list):

    def put(self, x):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.put(utils.DONE)


def _with_runtime(f_task):

    @functools.wraps(f_task)
    def wrapper(*wrapper_args):
        params = wrapper_args[-1]

        args = params.on_start() if params.on_start is not None else None

        if args is None:
            args = ()

        elif not isinstance(args, tuple):
            args = (args,)
        
        if params.input_queue:
            for x in params.input_queue:
                task_args = wrapper_args + (x, args)
                f_task(*task_args)
        else:
            task_args = wrapper_args + (args,)
            f_task(*task_args)

        params.output_queues.done()

        if params.on_done is not None:
            with params.stage_lock:
                params.stage_namespace.active_workers -= 1

            stage_status = utils.StageStatus(
                namespace = params.stage_namespace,
                lock = params.stage_lock,
            )

            params.on_done(stage_status, *args)
    
    return wrapper

###########
# map
###########

@_with_runtime
def _map(f, params, x, args):
    y = f(x, *args)
    params.output_queues.put(y)


def map(f, stage = utils.UNDEFINED, workers = 1, maxsize = 0, on_start = None, on_done = None):
    """
    """

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: map(f, stage, workers=workers, maxsize=maxsize, on_start=on_start, on_done=on_done))

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        on_start = on_start,
        on_done = on_done,
        target = _map,
        args = (f,),
        dependencies = [stage],
    )

###########
# flat_map
###########

@_with_runtime
def _flat_map(f, params, x, args):
    for y in f(x, *args):
        params.output_queues.put(y)


def flat_map(f, stage = utils.UNDEFINED, workers = 1, maxsize = 0, on_start = None, on_done = None):
    """
    """

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: flat_map(f, stage, workers=workers, maxsize=maxsize, on_start=on_start, on_done=on_done))

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        on_start = on_start,
        on_done = on_done,
        target = _flat_map,
        args = (f,),
        dependencies = [stage],
    )


###########
# filter
###########

@_with_runtime
def _filter(f, params, x, args):
    if f(x, *args):
        params.output_queues.put(x)


def filter(f, stage = utils.UNDEFINED, workers = 1, maxsize = 0, on_start = None, on_done = None):
    """
    """

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: filter(f, stage, workers=workers, maxsize=maxsize, on_start=on_start, on_done=on_done))

    stage = _to_stage(stage)
    
    return _Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        on_start = on_start,
        on_done = on_done,
        target = _filter,
        args = (f,),
        dependencies = [stage],
    )


###########
# each
###########

@_with_runtime
def _each(f, params, x, args):
    f(x, *args)


def each(f, stage = utils.UNDEFINED, workers = 1, maxsize = 0, on_start = None, on_done = None, run = False):
    """
    """

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: each(f, stage, workers=workers, maxsize=maxsize, on_start=on_start, on_done=on_done))

    stage = _to_stage(stage)

    stage = _Stage(
        worker_constructor = WORKER,
        workers = workers,
        maxsize = maxsize,
        on_start = on_start,
        on_done = on_done,
        target = _each,
        args = (f,),
        dependencies = [stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass


###########
# concat
###########
@_with_runtime
def _concat(params, x, args):
    params.output_queues.put(x)



def concat(stages, maxsize = 0):

    stages = [ _to_stage(s) for s in stages ]

    return _Stage(
        worker_constructor = WORKER,
        workers = 1,
        maxsize = maxsize,
        on_start = None,
        on_done = None,
        target = _concat,
        args = tuple(),
        dependencies = stages,
    )

################
# run
################

def run(stages, maxsize = 0):
    
    if isinstance(stages, list) and len(stages) == 0:
        raise ValueError("Expected atleast stage to run")

    elif isinstance(stages, list):
        stage = concat(stages, maxsize = maxsize)
    
    else:
        stage = stages

    stage = to_iterable(stage, maxsize = maxsize)
    
    for _ in stages:
        pass

    

################
# _to_stage
################ 

def _to_stage(obj):

    if isinstance(obj, _Stage):
        return obj

    elif hasattr(obj, "__iter__"):
        return from_iterable(obj)
    
    else:
        raise ValueError("Object {obj} is not iterable".format(obj = obj))

################
# from_iterable
################
@_with_runtime
def _from_iterable(iterable, params, args):

    for x in iterable:
        params.output_queues.put(x)
    

# @utils.maybe_partial(1)
def from_iterable(iterable = utils.UNDEFINED, worker_constructor = Thread):

    if utils.is_undefined(iterable):
        return utils.Partial(lambda iterable: from_iterable(iterable, worker_constructor=worker_constructor))

    return _Stage(
        worker_constructor = worker_constructor,
        workers = 1,
        maxsize = None,
        on_start = None,
        on_done = None,
        target = _from_iterable,
        args = (iterable,),
        dependencies = [],
    )

##############
# to_iterable
##############

def _build_queues(stage, stage_input_queue, stage_output_queues, visited):

    if stage in visited:
        return stage_input_queue, stage_output_queues
    else:
        visited.add(stage)
    
    
    if len(stage.dependencies) > 0:
        total_done = sum([ s.workers for s in stage.dependencies ])
        input_queue = _InputQueue(stage.maxsize, total_done)
        stage_input_queue[stage] = input_queue

        for _stage in stage.dependencies:
            
            if _stage not in stage_output_queues:
                stage_output_queues[_stage] = _OutputQueues([input_queue])
            else:
                stage_output_queues[_stage].append(input_queue)

            stage_input_queue, stage_output_queues = _build_queues(
                _stage,
                stage_input_queue,
                stage_output_queues,
                visited
            )

    return stage_input_queue, stage_output_queues

def _create_worker(f, args, output_queues, input_queue):

    kwargs = dict(
        output_queues = output_queues)

    if input_queue is not None:
        kwargs.update(input_queue = input_queue)

    return WORKER(target = f, args = args, kwargs = kwargs)

def _to_iterable(stage, maxsize):

    input_queue = _InputQueue(maxsize, stage.workers)

    stage_input_queue, stage_output_queues = _build_queues(
        stage = stage,
        stage_input_queue = dict(),
        stage_output_queues = dict(),
        visited = set(),
    )

    stage_output_queues[stage] = _OutputQueues([ input_queue ])
    
    processes = []
    for _stage in stage_output_queues:

        if _stage.on_done is not None:
            stage_lock = Lock()
            stage_namespace = _get_namespace()
            stage_namespace.active_workers = _stage.workers
        else:
            stage_lock = None
            stage_namespace = None

        for _ in range(_stage.workers):

            stage_params = _StageParams(
                output_queues = stage_output_queues[_stage],
                input_queue = stage_input_queue.get(_stage, None),
                on_start = _stage.on_start,
                on_done = _stage.on_done,
                stage_lock = stage_lock,
                stage_namespace = stage_namespace
            )
            process = _stage.worker_constructor(
                target = _stage.target,
                args = _stage.args + (stage_params,)
            )

            processes.append(process)

    for p in processes:
        p.daemon = True
        p.start()

    for x in input_queue:
        yield x

    
    for p in processes:
        p.join()

def to_iterable(stage = utils.UNDEFINED, maxsize = 0):

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: _to_iterable(stage, maxsize))
    else:
        return _to_iterable(stage, maxsize)
    

if __name__ == '__main__':
    import time
    import random

    def slow_square(x):
        time.sleep(random.uniform(0, 1))
        return x**2

    stage = range(10)

    stage = flat_map(lambda x: [x, x + 1, x + 2], stage)

    stage = map(slow_square, stage, workers=4)

    stage = filter(lambda x: x > 9, stage)

    print(stage)
    

    
