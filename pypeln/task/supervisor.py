from dataclasses import dataclass
import threading
import time
import typing as tp

from pypeln import utils as pypeln_utils

from .worker import Worker
from .queue import IterableQueue


@dataclass
class Supervisor:
    workers: tp.List[Worker]
    main_queue: IterableQueue
    done: bool = False

    def __enter__(self):
        pass

    def __exit__(self, *args):
        self.done = True

        for worker in self.workers:
            worker.stop()

        while any(worker.process.cancelled() for worker in self.workers):
            time.sleep(pypeln_utils.TIMEOUT)
