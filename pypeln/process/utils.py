import typing as tp
import sys

if "multiprocess" in sys.modules:
    from multiprocess import Manager, Lock
else:
    from multiprocessing import Manager, Lock

from pypeln import utils as pypeln_utils

MANAGER = None


class Namespace:
    def __init__(self, **kwargs):
        global MANAGER

        if MANAGER is None:
            MANAGER = Manager()

        self.__dict__["_namespace"] = MANAGER.Namespace(**kwargs)
        self.__dict__["_lock"] = Lock()

    def __getattr__(self, key) -> tp.Any:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        return getattr(self._namespace, key)

    def __setattr__(self, key, value) -> None:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        setattr(self._namespace, key, value)

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, *args):
        self._lock.release()

    def _locked(self) -> bool:
        if self._lock.acquire(block=False):
            self._lock.release()
            return False
        else:
            return True
