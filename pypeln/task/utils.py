import asyncio
from concurrent.futures import Future
import functools
import threading
import typing as tp

from pypeln import utils as pypeln_utils


class _Namespace:
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)


def Namespace(**kwargs) -> tp.Any:
    return _Namespace(**kwargs)


def get_running_loop() -> asyncio.AbstractEventLoop:

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if not loop.is_running():

        def run():
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
    f: tp.Callable[[], tp.Any], loop: tp.Optional[asyncio.AbstractEventLoop] = None,
) -> asyncio.Handle:
    loop = loop if loop else get_running_loop()

    return loop.call_soon_threadsafe(f)


def run_test_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        return run_coroutine_in_loop(lambda: f(*args, **kwargs)).result()

    return wrapped
