import asyncio
import functools
import threading
import typing as tp
from concurrent.futures import Future

from pypeln import utils as pypeln_utils


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__["_namespace"] = pypeln_utils._Namespace(**kwargs)
        self.__dict__["_lock"] = asyncio.Lock()

    def __getattr__(self, key) -> tp.Any:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        return getattr(self._namespace, key)

    def __setattr__(self, key, value) -> None:
        if key in ("_namespace", "_lock"):
            raise AttributeError()

        setattr(self._namespace, key, value)

    async def __aenter__(self):
        await self._lock.acquire()

    async def __aexit__(self, *args):
        self._lock.release()


def get_running_loop() -> asyncio.AbstractEventLoop:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if not loop.is_running():

        def run():
            if not loop.is_running():
                loop.run_forever()

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()

    return loop


def run_coroutine_in_loop(
    f_coro: tp.Callable[[], tp.Awaitable],
    loop: tp.Optional[asyncio.AbstractEventLoop] = None,
) -> Future:
    loop = loop if loop else get_running_loop()

    return asyncio.run_coroutine_threadsafe(f_coro(), loop)


def run_function_in_loop(
    f: tp.Callable[[], tp.Any],
    loop: tp.Optional[asyncio.AbstractEventLoop] = None,
) -> asyncio.Handle:
    loop = loop if loop else get_running_loop()

    return loop.call_soon_threadsafe(f)


def run_test_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        return run_coroutine_in_loop(lambda: f(*args, **kwargs)).result()

    return wrapped
