import asyncio
import functools
import pypeln as pl


def run_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        return pl.task.run_coroutine_in_loop(lambda: f(*args, **kwargs)).result()

    return wrapped
