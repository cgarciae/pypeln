# Pypeline

_Pypeline is a python library that enables you to easily create concurrent/parallel data pipelines._

* Pypeline was designed to solve simple _medium_ data tasks that require concurrency and parallelism but where using frameworks like Spark or Dask feel exaggerated or unnatural.
* Pypeline hides away all the boilerplate code required to execute concurrent/parallel tasks and exposes an easy to use, familiar, functional API.
* Pypeline enables you to build concurrent pipelines using all 3 major concurrency/parallelism mechanisms in Python (multiprocessing, threading, and asyncio) via the exact same API.

## Instalation

To install Pypeline with pip run
```bash
pip install pypeln
```
If you want the latest development version you can install via
```bash
pip install git+https://github.com/cgarciae/pypeln@develop
```

## Concept
**[MISSING INTRODUCTION]**

![diagram](docs/diagram.png)

**[MISSING DIAGRAM EXPLANATION]**

## Basic Usage
You can build a stage out of any _iterable_ object and apply certain type of transformations with the operations performed in parallel and/or concurrently depending on the mechanism you use. 


### Processes
You can create a stage based on [multiprocessing.Process](https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Process)es by using the `pr` module:

```python
from pypeln import pr
import time

def slow_add1(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = pr.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

data = list(stage) # [5, 2, 3, 4]
```
### Threads
You can create a stage based on [threading.Thread](https://docs.python.org/3/library/threading.html#threading.Thread)s by using the `th` module:
```python
from pypeln import th
import time

def slow_add1(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = th.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

data = list(stage) # [5, 2, 3, 4]
```
**[EXPLENATION NEEDED]**

### Tasks
You can create a stage based on [asyncio.Task](https://docs.python.org/3.4/library/asyncio-task.html#asyncio.Task)s by using the `io` module:
```python
from pypeln import io
import asyncio

async def slow_add1(x):
    await asyncio.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = io.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

data = list(stage) # i.e. [5, 2, 3, 4]
```
**[EXPLENATION NEEDED]**
### Multi-staged Pipelines
You can create pipelines with multiple stages by building upon a previous stage. For example
```python
from pypeln import pr
import time

def slow_add1(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

def slow_gte3(x):
    time.sleep(0.2) # <= some slow computation
    return x >= 3

data = [1,2,3,4]
stage = pr.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)
stage = pr.filter(slow_gte3, stage, workers = 3) # [3, 4, 5] (sorted)

data = list(stage) # [5, 3, 4]
```
**[EXPLENATION NEEDED]**

## Benchmarks
**[COMMING SOON]**

## Resources

* [Pypeline API Documentation](https://cgarciae.github.io/pypeln/) **[WORK IN PROGRESS]**
* Pypeline Guide: **[COMMING SOON]**


## Contributors
* [cgarciae](https://github.com/cgarciae)
* [davidnet](https://github.com/davidnet)