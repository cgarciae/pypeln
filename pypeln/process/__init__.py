"""
The `process` module lets you create pipelines using objects from python's [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) module according to Pypeln's general [architecture](https://cgarciae.github.io/pypeln/advanced/#architecture). Use this module when you are in need of true parallelism for CPU heavy operations but be aware of its implications.
"""

from .api.concat import concat
from .api.each import each
from .api.filter import filter
from .api.flat_map import flat_map
from .api.from_iterable import from_iterable
from .api.map import map
from .api.ordered import ordered
from .api.run import run
from .api.to_iterable import to_iterable
from .queue import IterableQueue, OutputQueues
from .supervisor import Supervisor
from .utils import Namespace
from .worker import StageParams, Worker, WorkerInfo, start_workers

