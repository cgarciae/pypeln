import asyncio
import threading
import time
from . import utils

class TaskPool(object):

    def __init__(self, workers, loop):
        self._semaphore = asyncio.Semaphore(workers, loop=loop)
        self._tasks = set()
        self._closed = False
        self._loop = loop
        

    async def put(self, coro):

        if self._closed:
            raise RuntimeError("Trying put items into a closed TaskPool")
        
        await self._semaphore.acquire()
        
        task = asyncio.ensure_future(coro, loop=self._loop)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task):
        self._tasks.remove(task)
        self._semaphore.release()

    async def join(self):
        await asyncio.gather(*self._tasks, loop=self._loop)
        self._closed = True

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()


############################
# to_async_iterable
############################


def _consume_iterable(loop, iterable, queue):

    # print("_consume_iterable", iterable)

    for x in iterable:
        while True:
            if not queue.full():
                loop.call_soon_threadsafe(queue.put_nowait, x)
                break
            else:
                time.sleep(utils.TIMEOUT)
    
    while True:
        if not queue.full():
            loop.call_soon_threadsafe(queue.put_nowait, utils.DONE)
            break
        else:
            time.sleep(utils.TIMEOUT)

async def _trivial_async_iterable(iterable):

    # print("_trivial_async_iterable", iterable)

    for i, x in enumerate(iterable):
        # print(x)
        yield x

        if i % 1000 == 0:
            await asyncio.sleep(0)

async def _async_iterable(iterable, maxsize, loop):

    # print("_async_iterable", iterable)

    queue = asyncio.Queue(maxsize=maxsize)

    task = loop.run_in_executor(None, lambda: _consume_iterable(loop, iterable, queue))
    # t = threading.Thread(target = _consume_iterable, args = (loop, iterable, queue))
    # t.daemon = True
    # t.start()

    while True:
        x = await queue.get()

        if utils.is_done(x):
            break
        
        else:
            yield x

    await task

def to_async_iterable(iterable, loop = None, maxsize = 0):

    if loop is None:
        loop = asyncio.get_event_loop()

    if hasattr(iterable, "__aiter__"):
        return iterable
    elif not hasattr(iterable, "__iter__"):
        raise ValueError("Object {iterable} most be either iterable or async iterable.".format(iterable=iterable))

    if type(iterable) in (list, dict, tuple, set):
        return _trivial_async_iterable(iterable)

    else:
        return _async_iterable(iterable, maxsize, loop)

############################
# to_sync_iterable
############################

def _run_coroutine(loop, async_iterable, queue):

    loop.run_until_complete(_consume_async_iterable(async_iterable, queue))

async def _consume_async_iterable(async_iterable, queue):

    async for x in async_iterable:
        await queue.put(x)

    await queue.put(utils.DONE)


def to_sync_iterable(async_iterable, maxsize = 0, loop = None):

    def sync_iterable():

        queue = asyncio.Queue(maxsize=maxsize)

        if loop is None:
            loop_ = asyncio.get_event_loop()
        else:
            loop_ = loop

        t = threading.Thread(target=_run_coroutine, args=(loop_, async_iterable, queue))
        t.daemon = True
        t.start()

        while True:
            if not queue.empty():
                x = queue.get_nowait()
                
                if utils.is_done(x):
                    break
                else:
                    yield x
            else:
                time.sleep(utils.TIMEOUT)

        t.join()

    return sync_iterable()


if __name__ == '__main__':
    
    def slow_generator():
        yield 0

        time.sleep(1)
        yield 1

        time.sleep(1)
        yield 2

        time.sleep(1)
        yield 3

    
    async def task1():
        for _ in range(4):
            print("x")
            await asyncio.sleep(0)

        print("done task1")

        

    async def task2():

        for x in slow_generator():
            print(x)
            await asyncio.sleep(0)

        print("done task2")

    
    async def task3():

        async for x in to_async_iterable(slow_generator()):
            print(x)

        print("done task3")


    task = asyncio.gather(task1(), task3())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(task)

        

# if __name__ == '__main__':
    
#     async def slow_generator():
#         yield 0

#         await asyncio.sleep(1)
#         yield 1

#         await asyncio.sleep(1)
#         yield 2

#         await asyncio.sleep(1)
#         yield 3

    
#     for x in to_sync_iterable(slow_generator()):
#         print(x)