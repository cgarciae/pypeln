import asyncio
import functools
import pypeln as pl


def run_async(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        def run():
            return f(*args, **kwargs)

        run.f = f

        return pl.task.execute_in_loop(run)

    return wrapped
