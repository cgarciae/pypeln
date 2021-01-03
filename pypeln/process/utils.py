import multiprocessing
import multiprocessing.synchronize
import typing as tp

from pypeln import utils as pypeln_utils

MANAGER = None


class Namespace:
    def __init__(self, **kwargs):
        global MANAGER

        if MANAGER is None:
            MANAGER = multiprocessing.Manager()

        self.__dict__["_namespace"] = MANAGER.Namespace(**kwargs)
        self.__dict__["_lock"] = multiprocessing.Lock()

    def __getattr__(self, key) -> tp.Any:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        if not self._locked():
            raise pypeln_utils.NoLock(
                "Namespace not locked, use: \n\nwith namespace:\n    # code here\n\nto lock namespace."
            )

        return getattr(self.__dict__["_namespace"], key)

    def __setattr__(self, key, value) -> None:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        if not self._locked():
            raise pypeln_utils.NoLock(
                "Namespace not locked, use: \n\nwith namespace:\n    # code here\n\nto lock namespace."
            )

        setattr(self.__dict__["_namespace"], key, value)

    def __enter__(self):
        self.__dict__["_lock"].acquire()

    def __exit__(self, *args):
        self.__dict__["_lock"].release()

    def _locked(self) -> bool:
        lock = self.__dict__["_lock"]

        if lock.acquire(block=False):
            lock.release()
            return False
        else:
            return True
