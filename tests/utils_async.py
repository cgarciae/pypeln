import asyncio
import functools

def run_async(f):

    @functools.wraps(f)
    def wrapped(*args, **kwargs):

        old_loop = asyncio.get_event_loop()
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            task = f(*args, **kwargs)
            new_loop.run_until_complete(task)
        finally:
            asyncio.set_event_loop(old_loop)
        
    return wrapped