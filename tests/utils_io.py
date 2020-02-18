import asyncio
import functools


def run_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        # old_loop = asyncio.get_event_loop()
        # new_loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(new_loop)

        task = f(*args, **kwargs)
        asyncio.run(task)

    return wrapped
