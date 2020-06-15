
import asyncio
import sys
import threading
import time
import traceback
import typing as tp

from pypeln import utils as pypeln_utils


class _Namespace:
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)


def Namespace(**kwargs) -> tp.Any:
    return _Namespace(**kwargs)

def run_in_loop(f_coro: tp.Callable[[], tp.Awaitable]):

    loop = asyncio.get_event_loop()
    
    if not loop.is_running():
        def run():
            loop.run_forever()

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()

    return loop.call_soon_threadsafe(lambda: loop.create_task(f_coro()))




def execute_in_loop(f_coro: tp.Callable[[], tp.Coroutine]):

    initial_ref = object()
    output = Namespace(ref=initial_ref, exception=None, trace=None)

    async def target():
        try:
            output.ref = await f_coro()
        except BaseException as e:
            output.exception, _exception, _traceback = sys.exc_info()
            output.trace = "".join(
                traceback.format_exception(output.exception, _exception, _traceback)
            )
            output.ref = None

    run_in_loop(target)

    while output.ref is initial_ref:
        time.sleep(pypeln_utils.TIMEOUT)

    if output.exception is not None:
        try:
            exception = output.exception(f"\n\n{output.trace}")
        except:
            exception = Exception(f"\n\nOriginal: {output.exception}\n\n{output.trace}")

        raise exception

    return output.ref
