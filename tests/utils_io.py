import asyncio
import functools
import pypeln as pl


def run_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        return pl.task.execute_in_loop(lambda: f(*args, **kwargs))

    return wrapped
