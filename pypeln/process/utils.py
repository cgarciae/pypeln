from collections import namedtuple
from dataclasses import dataclass
from multiprocessing import get_context
import multiprocessing
from multiprocessing.queues import Empty, Full
import typing as tp

from pypeln import utils as pypeln_utils

MANAGER = None


def Namespace(**kwargs) -> tp.Any:
    global MANAGER

    if MANAGER is None:
        MANAGER = multiprocessing.Manager()

    return MANAGER.Namespace(**kwargs)
