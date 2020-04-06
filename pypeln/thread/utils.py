from collections import namedtuple
from queue import Empty, Full, Queue
from threading import Lock

from pypeln import utils as pypeln_utils


def get_namespace(**kwargs):
    return pypeln_utils.Namespace(**kwargs)



class IterableQueue(object):
    def __init__(self, maxsize, total_done, pipeline_namespace, **kwargs):

        self.queue = Queue(maxsize=maxsize, **kwargs)
        self.lock = Lock()
        self.queue_namespace = get_namespace()
        self.queue_namespace.remaining = total_done

        self.pipeline_namespace = pipeline_namespace

    def __iter__(self):

        while not self.is_done():
            x = self.get()

            if self.pipeline_namespace.error:
                return

            if not pypeln_utils.is_continue(x):
                yield x

    def get(self):

        try:
            x = self.queue.get(timeout=pypeln_utils.TIMEOUT)
        except (Empty, Full):
            return pypeln_utils.CONTINUE

        if not pypeln_utils.is_done(x):
            return x
        else:
            with self.lock:
                self.queue_namespace.remaining -= 1

            return pypeln_utils.CONTINUE

    def is_done(self):
        return self.queue_namespace.remaining == 0 and self.queue.empty()

    def put(self, x):
        self.queue.put(x)

    def done(self):
        self.queue.put(pypeln_utils.DONE)


class MultiQueue(list):
    def put(self, x):
        for queue in self:
            queue.put(x)

    def done(self):
        for queue in self:
            queue.done()


class StageStatus:
    """
    Object passed to various `on_done` callbacks. It contains information about the stage in case book keeping is needed.
    """

    def __init__(self, namespace, lock):
        self._namespace = namespace
        self._lock = lock

    @property
    def done(self) -> bool:
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
        return f"StageStatus(done = {done}, active_workers = {active_workers})"
