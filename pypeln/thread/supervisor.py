from dataclasses import dataclass
import multiprocessing
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

    def __call__(self):

        try:
            workers = [worker for worker in self.workers if worker.timeout > 0]

            if not workers:
                return

            while not self.done:

                for worker in workers:
                    if worker.did_timeout():
                        worker.stop()
                        worker.start()

                time.sleep(pypeln_utils.TIMEOUT)

        except BaseException as e:
            self.main_queue.raise_exception(e)

    def __enter__(self):
        self.start()

    def __exit__(self, *args):
        self.done = True

        for worker in self.workers:
            worker.stop()

        while any(worker.process.is_alive() for worker in self.workers):
            time.sleep(pypeln_utils.TIMEOUT)

    def start(self):

        for worker in self.workers:
            worker.start()

        t = threading.Thread(target=self)
        t.daemon = True
        t.start()
