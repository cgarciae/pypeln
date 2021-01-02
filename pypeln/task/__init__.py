"""
The `task` module lets you create pipelines using objects from python's [asyncio](https://docs.python.org/3/library/asyncio.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need to perform efficient asynchronous IO operations and DONT need to perform heavy CPU operations.


Most functions in this module return a `pl.task.Stage` object which implement the `Iterable`, `AsyncIterable`, and `Awaitable` interfaces which enables you to combine it seamlessly with regular Python code.

### Awaitable
You can call `await` con any `pl.thread.Stage` to get back the results of its computation:

```python
import pypeln as pl
import asyncio
from random import random

async def slow_add1(x):
    await asyncio.sleep(random()) # <= some slow computation
    return x + 1

async def slow_gt3(x):
    await asyncio.sleep(random()) # <= some slow computation
    return x > 3

async def main()
    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pl.task.map(slow_add1, data, workers=3, maxsize=4)
    stage = pl.task.filter(slow_gt3, stage, workers=2)

    data = await stage # e.g. [5, 6, 9, 4, 8, 10, 7]

asyncio.run(main())
```
When calling `await` on a stage you will get back the same result if you called `list` on it with be big difference that you wont block the current thread while waiting for the computation to materialize.

### AsyncIterable
`task` Stages are asynchronous generators so you can iterate through them using `async for` to get access each new element as soon as it become available:

```python
import pypeln as pl
import asyncio
from random import random

async def slow_add1(x):
    await asyncio.sleep(random()) # <= some slow computation
    return x + 1

async def slow_gt3(x):
    await asyncio.sleep(random()) # <= some slow computation
    return x > 3

async def main()
    data = range(10) # [0, 1, 2, ..., 9] 

    stage = pl.task.map(slow_add1, data, workers=3, maxsize=4)
    stage = pl.task.filter(slow_gt3, stage, workers=2)

    async for element in stage:
        pritn(element) # 5, 6, 9, 4, 8, 10, 7

asyncio.run(main())
```
When iterating a stage using `async for` you will get back the same result as if you called the normal `for` on it with be big difference that you wont block the current thread while waiting for the next element.

### Event Loop
When you run a `task` stage all the tasks will be scheduled in the event loop on the current thread if it exists, else Pypeln will create and keep alive a new event loop.
"""

from .api.concat import concat
from .api.each import each
from .api.filter import filter
from .api.flat_map import flat_map
from .api.from_iterable import from_iterable
from .api.map import map
from .api.run import run
from .api.ordered import ordered
from .api.to_iterable import to_iterable, to_async_iterable
from .queue import IterableQueue, OutputQueues
from .supervisor import Supervisor
from .utils import Namespace, run_coroutine_in_loop, run_function_in_loop
from .worker import StageParams, TaskPool, Worker, WorkerInfo, start_workers
from . import utils
