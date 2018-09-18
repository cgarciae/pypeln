# Pypeline

_Pypeline is a python library that enables you to easily create concurrent/parallel data pipelines._

* Pypeline was designed to solve simple _medium_ data tasks that require concurrency and parallelism but where using frameworks like Spark or Dask feel exaggerated or unnatural.
* Pypeline hides away all the boiler plate code required to execute concurrent/parallel tasks and exposes and easy to use, familiar, functional API.
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

## Basic Usage
You can build a pipeline out of any _iterable_ object and certain type of transformations in parallel and/or concurrently depending on the mechanism you use. 

### pr
Using `pr` module you can create multi-processes pipelines

```python
from pypeln import pr
import time

def slow_add1(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = pr.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

print(list(stage)) # [5, 2, 3, 4] (possible real output)
```
### th
If you want to use threads instead just switch to the `th` module
```python
from pypeln import th
import time

def slow_add1(x):
    time.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = th.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

print(list(stage)) # [5, 2, 3, 4] (possible real output)
```
### io
If the task is better suited for asynchronous code (`asyncio`) you can use the `io` module
```python
from pypeln import io
import asyncio

async def slow_add1(x):
    await asyncio.sleep(0.2) # <= some slow computation
    return x + 1

data = [1,2,3,4]
stage = io.map(slow_add1, data, workers = 2) # [2, 3, 4, 5] (sorted)

print(list(stage)) # [5, 2, 3, 4] (possible real output)
```
### Multi-staged Pipelines
You can create pipelines with multiple stages by building upon a previous stage just as you would expect. For example
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

print(list(stage)) # [5, 3, 4] (possible real output)
```
This create the following pipeline

**[MISSING DIAGRAM]**

## Benchmarks
**[COMMING SOON]**

## Resources

* [Pypeline Documentation](https://cgarciae.github.io/pypeln/) **[WORK IN PROGRESS]**
* Pypeline Guide **[COMMING SOON]**


## Contributors
* [cgarciae](https://github.com/cgarciae)
* [davidnet](https://github.com/davidnet)