""" The `process` module lets you create pipelines using objects from python's [multiprocessing](https://docs.python.org/3.4/library/multiprocessing.html) module according to Pypeline's general [architecture](https://cgarciae.gitbook.io/pypeln/#architecture). Use this module when you are in need of true parallelism for CPU heavy operations but be aware of its implications (continue reading).

    from pypeln import process as pr
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    def slow_gt3(x):
        time.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pr.map(slow_add1, data, workers = 3, maxsize = 4)
    stage = pr.filter(slow_gt3, stage, workers = 2)

    data = list(stage) # e.g. [5, 6, 9, 4, 8, 10, 7]

## Stage
All functions from this module return a private `pypeln.process._Stage` object. Stages are lazy, that is, a `_Stage` objects merely contains the information needed to perform the computation of itself and the Stages it depends on. Stages are [iterables](https://docs.python.org/3/glossary.html#term-iterable) i.e. they implement `__iter__`, to actually execute the pipeline you can directly iterable them or iterate over the generator returned by `pypeln.process.to_iterable` which gives you more control.

    from pypeln import process as pr
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    data = range(10) # [0, 1, 2, ..., 9]
    stage = pr.map(slow_add1, data, workers = 3, maxsize = 4)

    for x in stage:
        print(x) # e.g. 2, 1, 5, 6, 3, 4, 7, 8, 9, 10

This iterable API makes Pypeline a very intuitive/pythonic to use and compatible with most other python code. 

## Workers
The worker type of this module is a [multiprocessing.Process](https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Process). Each worker process is instantiated with `daemon = True`. Creating each process is slow and consumes a lot of memory. Since processes are technically separate programs managed by the OS they are great for doing operations in parallel by avoiding the [GIL](https://realpython.com/python-gil) (or rather having their separate GIL).

The number of workers on each stage can usually be controled by the `workers` parameter on `pypeln.process`'s various functions. Try not to create more processes than the number of cores you have on your machine or else they will end up fighting for resources and computation will be suboptimal.

## Queue
The queue type of this module is a [multiprocessing.Queue](https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Queue). Since processes don't share memory, all information passed between them through these queues must first be serialized (pickled) which is slow, be aware of this and try to avoid sending large objects.

The number of elements each stage can hold usually be controled by the `maxsize` parameter on `pypeln.process`'s various functions. When passed this parameter sets a maximum size for the input Queue of the stage, this serves as a [backpressure](https://www.quora.com/What-is-backpressure-in-the-context-of-data-streaming) mechanism because any stages pushing data to a Queue that becomes full (reaches its `maxsize`) will have to stop their computation until space becomes available, thus, potentially preveting `OutOfMemeory` errors due to overpressure from faster stages.

## Resource Management
There are many occasions where you need to create some resource objects (e.g. http or database sessions) that for efficiency are expected to last the whole lifespan of each worker. To handle such objects many functions have the `on_start` and `on_done` arguments which expect some callback functions. 

When a worker is created it calls the `on_start` function, this functions should create and return the resource objects. These object will be passed as extra arguments to the main function and also to the `on_end` function.

    from pypeln import process as pr

    def x():
        http_session = get_http_session()
        db_session = get_db_session()
        return http_session, db_session

    def on_end(_stage_status, http_session, db_session):
        http_session.close()
        db_session.close()

    def f(x, http_session, db_session):
        # some logic
        return y

    stage = pr.map(f, stage, workers = 3, on_start = on_start)

A few notes:

* The resource objects are created per worker.
* `on_start` should return a object other that `None` or a tuple of resource objects.
* If `on_start` returns some arguments then they must be accepted by `f` and `on_end`.
* `on_end` receives a `pypeln.process.StageStatus` object followed by the resource objects created by `on_start`.

## Pipe Operator
Functions that accept a `stage` parameter return a `Partial` instead of a new stage when `stage` is not given. These `Partial`s are callables that accept the missing `stage` parameter and return the full output of the original function. For example

    pr.map(f, stage, **kwargs) = pr.map(f, **kwargs)(stage)

The important thing about partials is that they implement the pipe `|` operator as

    x | partial = partial(x)

This allows you to define pipelines in the following way:

    from pypenl import process as pr

    data = (
        range(10)
        | pr.map(slow_add1, workers = 3, maxsize = 4)
        | pr.filter(slow_gt3, workers = 2)
        | list
    )

## Recomendations
Creating processes and doing Inter-Process Communication (IPC) is expensive, therefore we recommend the following:

* Minimize the number of stages based on this module.
* Tune the number of workers based on the number of cores.
* When processing large datasets set the maxsize of the stage to prevent `OutOfMemory` errors.
* If possible don't send large objects.
* If you just need a single stage to perform a task over a collection in parallel use the `pypeln.process.each` function. 
"""


from __future__ import absolute_import, print_function

import functools
from collections import namedtuple
from . import utils
import sys
import traceback
import types
import inspect

#############
# imports pr
#############

from multiprocessing import Process as WORKER
from multiprocessing import Manager, Lock, Queue
from multiprocessing.queues import Full, Empty
from threading import Thread

from collections import namedtuple
from . import utils

_MANAGER = None


def _get_manager():
    global _MANAGER

    if _MANAGER is None:
        _MANAGER = Manager()

    return _MANAGER


def _get_namespace():
    return _get_manager().Namespace()


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
    def __init__(
        self,
        worker_constructor,
        workers,
        maxsize,
        on_start,
        on_done,
        target,
        args,
        dependencies,
    ):
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
            worker_constructor=self.worker_constructor,
            workers=self.workers,
            maxsize=self.maxsize,
            target=self.target,
            args=self.args,
            dependencies=len(self.dependencies),
        )


class _StageParams(
    namedtuple(
        "_StageParams",
        [
            "input_queue",
            "output_queues",
            "on_start",
            "on_done",
            "stage_namespace",
            "stage_lock",
            "pipeline_namespace",
            "pipeline_error_queue",
            "index",
        ],
    )
):
    pass


WorkerInfo = namedtuple("WorkerInfo", ["index"])


class StageStatus(object):
    """
    Object passed to various `on_done` callbacks. It contains information about the stage in case book keeping is needed.
    """

    def __init__(self, namespace, lock):
        self._namespace = namespace
        self._lock = lock

    @property
    def done(self):
        """
        `bool` : `True` if all workers finished. 
        """
        with self._lock:
            return self._namespace.active_workers == 0

    @property
    def active_workers(self):
        """
        `int` : Number of active workers. 
        """
        with self._lock:
            return self._namespace.active_workers

    def __str__(self):
        return "StageStatus(done = {done}, active_workers = {active_workers})".format(
            done=self.done, active_workers=self.active_workers
        )


class _InputQueue(object):
    def __init__(self, maxsize, total_done, pipeline_namespace, **kwargs):

        self.queue = Queue(maxsize=maxsize, **kwargs)
        self.lock = Lock()
        self.namespace = _get_namespace()
        self.namespace.remaining = total_done

        self.pipeline_namespace = pipeline_namespace

    def __iter__(self):

        while not self.is_done():
            x = self.get()

            if self.pipeline_namespace.error:
                return

            if not utils.is_continue(x):
                yield x

    def get(self):

        try:
            x = self.queue.get(timeout=utils.TIMEOUT)
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

    def done(self):
        self.queue.put(utils.DONE)


class _OutputQueues(list):
    def put(self, x):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.put(utils.DONE)


def _handle_exceptions(params):
    def handle_exceptions(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):

            try:
                return f(*args, **kwargs)
            except BaseException as e:
                params.pipeline_error_queue.put(
                    (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
                )
                params.pipeline_namespace.error = True

        return wrapper

    return handle_exceptions


def _run_task(f_task, params):
    try:
        if params.on_start is not None:
            n_args = len(inspect.getargspec(params.on_start).args)

            if n_args == 0:
                args = params.on_start()
            elif n_args == 1:
                worker_info = WorkerInfo(index=params.index)
                args = params.on_start(worker_info)
            else:
                args = None
        else:
            args = None

        if args is None:
            args = ()

        elif not isinstance(args, tuple):
            args = (args,)

        if params.input_queue:
            for x in params.input_queue:
                f_task(x, args)
        else:
            f_task(args)

        params.output_queues.done()

        if params.on_done is not None:
            with params.stage_lock:
                params.stage_namespace.active_workers -= 1

            stage_status = StageStatus(
                namespace=params.stage_namespace, lock=params.stage_lock
            )

            params.on_done(stage_status, *args)

    except BaseException as e:
        try:
            params.pipeline_error_queue.put(
                (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
            )
            params.pipeline_namespace.error = True
        except BaseException as e:
            print(e)


###########
# map
###########


def _map(f, params):
    @_handle_exceptions(params)
    def f_task(x, args):
        y = f(x, *args)
        params.output_queues.put(y)

    _run_task(f_task, params)


def map(f, stage=utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None):
    """
    Creates a stage that maps a function `f` over the data. Its intended to behave like python's built-in `map` function but with the added concurrency.

        from pypeln import process as pr
        import time
        from random import random

        def slow_add1(x):
            time.sleep(random()) # <= some slow computation
            return x + 1

        data = range(10) # [0, 1, 2, ..., 9]
        stage = pr.map(slow_add1, data, workers = 3, maxsize = 4)

        data = list(stage) # e.g. [2, 1, 5, 6, 3, 4, 7, 8, 9, 10]

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> y`, where `args` is the return of `on_start` if present, else the signature is just `f(x) -> y`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: map(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor=WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_map,
        args=(f,),
        dependencies=[stage],
    )


###########
# flat_map
###########


def _flat_map(f, params):
    @_handle_exceptions(params)
    def f_task(x, args):
        for y in f(x, *args):
            params.output_queues.put(y)

    _run_task(f_task, params)


def flat_map(
    f, stage=utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None
):
    """
    Creates a stage that maps a function `f` over the data, however unlike `pypeln.process.map` in this case `f` returns an iterable. As its name implies, `flat_map` will flatten out these iterables so the resulting stage just contains their elements.

        from pypeln import process as pr
        import time
        from random import random

        def slow_integer_pair(x):
            time.sleep(random()) # <= some slow computation

            if x == 0:
                yield x
            else:
                yield x
                yield -x

        data = range(10) # [0, 1, 2, ..., 9]
        stage = pr.flat_map(slow_integer_pair, data, workers = 3, maxsize = 4)

        list(stage) # e.g. [2, -2, 3, -3, 0, 1, -1, 6, -6, 4, -4, ...]

    Note that because of concurrency order is not guaranteed. Also, `flat_map` is more general than both `pypeln.process.map` and `pypeln.process.filter`, as such these expressions are equivalent:

        from pypeln import process as pr

        pr.map(f, stage) = pr.flat_map(lambda x: [f(x)], stage)
        pr.filter(f, stage) = pr.flat_map(lambda x: [x] if f(x) else [], stage)

    Using `flat_map` with a generator function is very useful as we are able to filter out unwanted elements when e.g. there are exceptions, missing data, etc.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> [y]`, where `args` is the return of `on_start` if present, else the signature is just `f(x) -> [y]`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: flat_map(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    if isinstance(f, types.GeneratorType):
        _f = f
        f = lambda *args, **kwargs: _f(*args, **kwargs)

    return _Stage(
        worker_constructor=WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_flat_map,
        args=(f,),
        dependencies=[stage],
    )


###########
# filter
###########


def _filter(f, params):
    @_handle_exceptions(params)
    def f_task(x, args):
        if f(x, *args):
            params.output_queues.put(x)

    _run_task(f_task, params)


def filter(f, stage=utils.UNDEFINED, workers=1, maxsize=0, on_start=None, on_done=None):
    """
    Creates a stage that filter the data given a predicate function `f`. It is intended to behave like python's built-in `filter` function but with the added concurrency.

        from pypeln import process as pr
        import time
        from random import random

        def slow_gt3(x):
            time.sleep(random()) # <= some slow computation
            return x > 3

        data = range(10) # [0, 1, 2, ..., 9]
        stage = pr.filter(slow_gt3, data, workers = 3, maxsize = 4)

        data = list(stage) # e.g. [5, 6, 3, 4, 7, 8, 9]

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> bool`, where `args` is the return of `on_start` if present, else the signature is just `f(x)`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.

    # **Returns**
    * If the `stage` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: filter(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    return _Stage(
        worker_constructor=WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_filter,
        args=(f,),
        dependencies=[stage],
    )


###########
# each
###########


def _each(f, params):
    @_handle_exceptions(params)
    def f_task(x, args):
        f(x, *args)

    _run_task(f_task, params)


def each(
    f,
    stage=utils.UNDEFINED,
    workers=1,
    maxsize=0,
    on_start=None,
    on_done=None,
    run=False,
):
    """
    Creates a stage that runs the function `f` for each element in the data but the stage itself yields no elements. Its useful for sink stages that perform certain actions such as writting to disk, saving to a database, etc, and dont produce any results. For example:

        from pypeln import process as pr

        def process_image(image_path):
            image = load_image(image_path)
            image = transform_image(image)
            save_image(image_path, image)

        files_paths = get_file_paths()
        stage = pr.each(process_image, file_paths, workers = 4)
        pr.run(stage)

    or alternatively

        files_paths = get_file_paths()
        pr.each(process_image, file_paths, workers = 4, run = True)

    Note that because of concurrency order is not guaranteed.

    # **Args**
    * **`f`** : a function with signature `f(x, *args) -> None`, where `args` is the return of `on_start` if present, else the signature is just `f(x)`. 
    * **`stage = Undefined`** : a stage or iterable.
    * **`workers = 1`** : the number of workers the stage should contain.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
    * **`on_start = None`** : a function with signature `on_start() -> args`, where `args` can be any object different than `None` or a tuple of objects. The returned `args` are passed to `f` and `on_done`. This function is executed once per worker at the beggining.
    * **`on_done = None`** : a function with signature `on_done(stage_status, *args)`, where `args` is the return of `on_start` if present, else the signature is just `on_done(stage_status)`, and `stage_status` is of type `pypeln.process.StageStatus`. This function is executed once per worker when the worker is done.
    * **`run = False`** : specify whether to run the stage immediately.

    # **Returns**
    * If the `stage` parameters is not given then this function returns a `Partial`, else if `return = False` (default) it return a new stage, if `run = True` then it runs the stage and returns `None`.
    """

    if utils.is_undefined(stage):
        return utils.Partial(
            lambda stage: each(
                f,
                stage,
                workers=workers,
                maxsize=maxsize,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage = _to_stage(stage)

    stage = _Stage(
        worker_constructor=WORKER,
        workers=workers,
        maxsize=maxsize,
        on_start=on_start,
        on_done=on_done,
        target=_each,
        args=(f,),
        dependencies=[stage],
    )

    if not run:
        return stage

    for _ in stage:
        pass


###########
# concat
###########


def _concat(params):
    def f_task(x, args):
        params.output_queues.put(x)

    _run_task(f_task, params)


def concat(stages, maxsize=0):
    """
    Concatenates / merges many stages into a single one by appending elements from each stage as they come, order is not preserved.

        from pypeln import process as pr

        stage_1 = [1, 2, 3]
        stage_2 = [4, 5, 6, 7]

        stage_3 = pr.concat([stage_1, stage_2]) # e.g. [1, 4, 5, 2, 6, 3, 7]

    # **Args**
    * **`stages`** : a list of stages or iterables.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    # **Returns**
    * A stage object.
    """

    stages = [_to_stage(s) for s in stages]

    return _Stage(
        worker_constructor=WORKER,
        workers=1,
        maxsize=maxsize,
        on_start=None,
        on_done=None,
        target=_concat,
        args=tuple(),
        dependencies=stages,
    )


################
# run
################


def run(stages, maxsize=0):
    """
    Iterates over one or more stages until their iterators run out of elements.

        from pypeln import process as pr

        data = get_data()
        stage = pr.each(slow_fn, data, workers = 6)

        # execute pipeline
        pr.run(stage)

    # **Args**
    * **`stages`** : a stage/iterable or list of stages/iterables to be iterated over. If a list is passed, stages are first merged using `pypeln.process.concat` before iterating.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    # **Returns**
    * `None`
    """

    if isinstance(stages, list) and len(stages) == 0:
        raise ValueError("Expected at least 1 stage to run")

    elif isinstance(stages, list):
        stage = concat(stages, maxsize=maxsize)

    else:
        stage = stages

    stage = to_iterable(stage, maxsize=maxsize)

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
        raise ValueError("Object {obj} is not iterable".format(obj=obj))


################
# from_iterable
################


def _from_iterable(iterable, params):
    def f_task(args):
        for x in iterable:
            params.output_queues.put(x)

    _run_task(f_task, params)


def from_iterable(iterable=utils.UNDEFINED, maxsize=None, worker_constructor=Thread):
    """
    Creates a stage from an iterable. This function gives you more control of how a stage is created through the `worker_constructor` parameter which can be either:
    
    * `threading.Thread`: (default) is efficient for iterables that already have the data in memory like lists or numpy arrays because threads can share memory so no serialization is needed. 
    * `multiprocessing.Process`: is efficient for iterables who's data is not in memory like arbitrary generators and benefit from escaping the GIL. This is inefficient for iterables which have data in memory because they have to be serialized when sent to the background process.

    All functions that accept stages or iterables use this function when an iterable is passed to convert it into a stage using the default arguments.

    # **Args**
    * **`iterable`** : a source iterable.
    * **`maxsize = None`** : this parameter is not used and only kept for API compatibility with the other modules.
    * **`worker_constructor = threading.Thread`** : defines the worker type for the producer stage.

    # **Returns**
    * If the `iterable` parameters is given then this function returns a new stage, else it returns a `Partial`.
    """

    if utils.is_undefined(iterable):
        return utils.Partial(
            lambda iterable: from_iterable(
                iterable, maxsize=maxsize, worker_constructor=worker_constructor
            )
        )

    return _Stage(
        worker_constructor=worker_constructor,
        workers=1,
        maxsize=None,
        on_start=None,
        on_done=None,
        target=_from_iterable,
        args=(iterable,),
        dependencies=[],
    )


##############
# to_iterable
##############


def _build_queues(
    stage, stage_input_queue, stage_output_queues, visited, pipeline_namespace
):

    if stage in visited:
        return stage_input_queue, stage_output_queues
    else:
        visited.add(stage)

    if len(stage.dependencies) > 0:
        total_done = sum([s.workers for s in stage.dependencies])
        input_queue = _InputQueue(stage.maxsize, total_done, pipeline_namespace)
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
                visited,
                pipeline_namespace=pipeline_namespace,
            )

    return stage_input_queue, stage_output_queues


def _create_worker(f, args, output_queues, input_queue):

    kwargs = dict(output_queues=output_queues)

    if input_queue is not None:
        kwargs.update(input_queue=input_queue)

    return WORKER(target=f, args=args, kwargs=kwargs)


def _to_iterable(stage, maxsize):

    pipeline_namespace = _get_namespace()
    pipeline_namespace.error = False
    pipeline_error_queue = Queue()

    input_queue = _InputQueue(maxsize, stage.workers, pipeline_namespace)

    stage_input_queue, stage_output_queues = _build_queues(
        stage=stage,
        stage_input_queue=dict(),
        stage_output_queues=dict(),
        visited=set(),
        pipeline_namespace=pipeline_namespace,
    )

    stage_output_queues[stage] = _OutputQueues([input_queue])

    processes = []
    for _stage in stage_output_queues:

        if _stage.on_done is not None:
            stage_lock = Lock()
            stage_namespace = _get_namespace()
            stage_namespace.active_workers = _stage.workers
        else:
            stage_lock = None
            stage_namespace = None

        for index in range(_stage.workers):

            stage_params = _StageParams(
                output_queues=stage_output_queues[_stage],
                input_queue=stage_input_queue.get(_stage, None),
                on_start=_stage.on_start,
                on_done=_stage.on_done,
                stage_lock=stage_lock,
                stage_namespace=stage_namespace,
                pipeline_namespace=pipeline_namespace,
                pipeline_error_queue=pipeline_error_queue,
                index=index,
            )
            process = _stage.worker_constructor(
                target=_stage.target, args=_stage.args + (stage_params,)
            )

            processes.append(process)

    for p in processes:
        p.daemon = True
        p.start()

    try:
        for x in input_queue:
            yield x

        if pipeline_namespace.error:
            error_class, _, trace = pipeline_error_queue.get()
            raise error_class("\n\nOriginal {trace}".format(trace=trace))

        for p in processes:
            p.join()

    except:
        for q in stage_input_queue.values():
            q.done()

        raise


def to_iterable(stage=utils.UNDEFINED, maxsize=0):
    """
    Creates an iterable from a stage. This function is used by the stage's `__iter__` method with the default arguments.

    # **Args**
    * **`stage`** : a stage object.
    * **`maxsize = 0`** : the maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.

    # **Returns**
    * If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if utils.is_undefined(stage):
        return utils.Partial(lambda stage: _to_iterable(stage, maxsize))
    else:
        return _to_iterable(stage, maxsize)

