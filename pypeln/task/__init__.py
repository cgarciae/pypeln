"""
The `task` module lets you create pipelines using objects from python's [asyncio](https://docs.python.org/3/library/asyncio.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need to perform efficient asynchronous IO operations and DONT need to perform heavy CPU operations.
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
