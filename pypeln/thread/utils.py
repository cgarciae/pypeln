import typing as tp
import threading

from pypeln import utils as pypeln_utils


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__["_namespace"] = pypeln_utils._Namespace(**kwargs)
        self.__dict__["_lock"] = threading.Lock()

    def __getattr__(self, key) -> tp.Any:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        # if not self._lock.locked():
        #     raise pypeln_utils.NoLock(
        #         "Namespace not locked, use: \n\nwith namespace:\n    # code here\n\nto lock namespace."
        #     )

        return getattr(self._namespace, key)

    def __setattr__(self, key, value) -> None:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        # if not self._lock.locked():
        #     raise pypeln_utils.NoLock(
        #         "Namespace not locked, use: \n\nwith namespace:\n    # code here\n\nto lock namespace."
        #     )

        setattr(self._namespace, key, value)

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, *args):
        self._lock.release()
