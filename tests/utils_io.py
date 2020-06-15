import asyncio
import functools
import pypeln as pl


def run_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        # old_loop = asyncio.get_event_loop()
        # new_loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(new_loop)

        return pl.task.execute_in_loop(lambda: f(*args, **kwargs))

    return wrapped
