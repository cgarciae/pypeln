from collections import namedtuple
from queue import Empty, Full, Queue
from threading import Lock

from pypeln import utils as pypeln_utils


def get_namespace():
    return pypeln_utils.Namespace()


class StageStatus(object):
    def __init__(self):
        pass

    @property
    def done(self):
        return True

    @property
    def active_workers(self):
        return 0

    def __str__(self):
        return (
            f"StageStatus(done = {self.done}, active_workers = {self.active_workers})"
        )


class NoOpContext:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
