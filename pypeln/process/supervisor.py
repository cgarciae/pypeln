from dataclasses import dataclass
import multiprocessing
import threading
import time
import typing as tp

from pypeln import utils as pypeln_utils

from .worker import Worker


class RaisesException(pypeln_utils.Protocol):
    def raise_exception(self, exception: BaseException):
        ...


@dataclass
class Supervisor:
    workers: tp.List[Worker]
    main_queue: RaisesException
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

                time.sleep(0.05)

        except BaseException as e:
            self.main_queue.raise_exception(e)

    def __enter__(self):
        self.start()

    def __exit__(self, *args):
        self.done = True

    def start(self):

        t = threading.Thread(target=self)
        t.daemon = True
        t.start()
