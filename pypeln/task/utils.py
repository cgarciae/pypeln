
import typing as tp
import asyncio
import threading


class _Namespace:
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)


def Namespace(**kwargs) -> tp.Any:
    return _Namespace(**kwargs)

def run_in_loop(f_coro: tp.Callable[[], tp.Coroutine]):

    loop = asyncio.get_event_loop()
    
    if not loop.is_running():
        def run():
            loop.run_forever()

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()

    return loop.call_soon_threadsafe(lambda: loop.create_task(f_coro()))