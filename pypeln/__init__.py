"""
# Pypeln

_Pypeline is a simple yet powerful python library for creating concurrent data pipelines._

* Pypeln was designed to solve simple _medium_ data tasks that require concurrency and parallelism but where using frameworks like Spark or Dask feel exaggerated or unnatural.
* Pypeln exposes an easy to use, familiar, functional API.
* Pypeln enables you to build pipelines using Processes, Threads and asyncio.Tasks via the exact same API.
* Pypeln allows you to have control over the memory and cpu resources used at each stage of your pipeline.

## Installation

Install Pypeln using pip:

    pip install pypeln


## Basic Usage
With Pypeln you can easily create multi-stage data pipelines using 3 type of workers:

### Processes
You can create a pipeline based on [multiprocessing.Process](https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Process) workers by using the `process` module:


    import pypeln as pl
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    def slow_gt3(x):
        time.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pl.process.map(slow_add1, data, workers=3, maxsize=4)
    stage = pl.process.filter(slow_gt3, stage, workers=2)

    data = list(stage) # e.g. [5, 6, 9, 4, 8, 10, 7]

At each stage the you can specify the numbers of `workers`. The `maxsize` parameter limits the maximum amount of elements that the stage can hold simultaneously.

### Threads
You can create a pipeline based on [threading.Thread](https://docs.python.org/3/library/threading.html#threading.Thread) workers by using the `thread` module:

    import pypeln as pl
    import time
    from random import random

    def slow_add1(x):
        time.sleep(random()) # <= some slow computation
        return x + 1

    def slow_gt3(x):
        time.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pl.thread.map(slow_add1, data, workers=3, maxsize=4)
    stage = pl.thread.filter(slow_gt3, stage, workers=2)

    data = list(stage) # e.g. [5, 6, 9, 4, 8, 10, 7]

Here we have the exact same situation as in the previous case except that the worker are Threads.

### Tasks
You can create a pipeline based on [asyncio.Task](https://docs.python.org/3.4/library/asyncio-task.html#asyncio.Task) workers by using the `task` module:

    import pypeln as pl
    import asyncio
    from random import random

    async def slow_add1(x):
        await asyncio.sleep(random()) # <= some slow computation
        return x + 1

    async def slow_gt3(x):
        await asyncio.sleep(random()) # <= some slow computation
        return x > 3

    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pl.task.map(slow_add1, data, workers=3, maxsize=4)
    stage = pl.task.filter(slow_gt3, stage, workers=2)

    data = list(stage) # e.g. [5, 6, 9, 4, 8, 10, 7]

Conceptually similar but everything is running in a single thread and Task workers are created dynamically.

## Mixed Pipelines
You can create pipelines using different worker types such that each type is the best for its given task so you can get the maximum performance out of your code:

    data = get_iterable()
    data = pl.task.map(f1, data, workers=100)
    data = pl.thread.flat_map(f2, data, workers=10)
    data = filter(f3, data)
    data = pl.process.map(f4, data, workers=5, maxsize=200)

Notice that here we even used a regular python `filter`, since stages are iterables Pypeln integrates smoothly with any python code, just be aware of how each stage behaves.


## Pipe Operator
In the spirit of being a true pipeline library, Pypeln also lets you create your pipelines using the pipe `|` operator:


    data = (
        range(10)
        | pl.process.map(slow_add1, workers=3, maxsize=4)
        | pl.process.filter(slow_gt3, workers=2)
        | list
    )

## Architecture

A Pypeln pipeline has the following structure:

![diagram](https://github.com/cgarciae/pypeln/raw/master/docs/guide/diagram_small.png =250x250)

* Its composed of several concurrent **stages**
* At each stage it contains on or more **worker** entities that perform a task.
* Related stages are connected by a **queue**, workers from one stage *put* items into it, and workers from the other stage *get* items from it.
* Source stages consume iterables.
* Sink stages can be converted into iterables which 
consume them.

## Resources
<!-- * [Pypeln Guide](https://cgarciae.gitbook.io/pypeln) -->
* [Pypeln API Documentation](https://cgarciae.github.io/pypeln/)

## Benchmarks
* [Making an Unlimited Number of Requests with Python aiohttp + pypeln](https://medium.com/@cgarciae/making-an-infinite-number-of-requests-with-python-aiohttp-pypeln-3a552b97dc95)
  * [Code](https://github.com/cgarciae/pypeln/tree/master/benchmarks/100_million_downloads)


## Related Stuff
* [mpipe](https://vmlaker.github.io/mpipe/)
* [Process Pools](https://docs.python.org/3.4/library/multiprocessing.html?highlight=process#module-multiprocessing.pool)
* [Making 100 million requests with Python aiohttp](https://www.artificialworlds.net/blog/2017/06/12/making-100-million-requests-with-python-aiohttp/)
* [Python multiprocessing Queue memory management](https://stackoverflow.com/questions/52286527/python-multiprocessing-queue-memory-management/52286686#52286686)

## Contributors
* [cgarciae](https://github.com/cgarciae)
"""

from . import thread
from . import process
from . import task

__all__ = ["process", "thread", "task"]
__version__ = "0.2.5"

