from collections import namedtuple
from multiprocessing import get_context
import multiprocessing
from multiprocessing.queues import Full, Empty

from pypeln import utils as pypeln_utils

# multiprocessing = get_context("fork")
MANAGER = multiprocessing.Manager()


class Namespace:
    def __init__(self, *args, **kwargs):
        self.namespace = MANAGER.Namespace(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.namespace, name)

    def __setattr__(self, name, value):
        setattr(self.namespace, name, value)
