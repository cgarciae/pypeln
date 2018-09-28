import asyncio
import threading
import time
from . import utils

class TaskPool(object):

    def __init__(self, workers):
        self._semaphore = asyncio.Semaphore(workers)
        self._tasks = set()
        self._closed = False
        

    async def put(self, coro):

        if self._closed:
            raise RuntimeError("Trying put items into a closed TaskPool")
        
        await self._semaphore.acquire()
        
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task):
        self._tasks.remove(task)
        self._semaphore.release()

    async def join(self):
        await asyncio.gather(*self._tasks)
        self._closed = True

    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()



def _consume_iterable(loop, iterable, queue):

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

def to_async_iterable(iterable, maxsize = 0):

    async def async_iterable():
        queue = asyncio.Queue(maxsize=maxsize)

        loop = asyncio.get_event_loop()
        task = loop.run_in_executor(None, lambda: _consume_iterable(loop, iterable, queue))


        while True:
            x = await queue.get()

            if utils.is_done(x):
                break
            
            else:
                yield x

        await task


    return async_iterable()


def _run_coroutine(loop, async_iterable, queue):

    loop.run_until_complete(_consume_async_iterable(async_iterable, queue))

async def _consume_async_iterable(async_iterable, queue):

    async for x in async_iterable:
        await queue.put(x)

    await queue.put(utils.DONE)


def to_sync_iterable(async_iterable, maxsize = 0):

    def sync_iterable():

        queue = asyncio.Queue(maxsize=maxsize)
        loop = asyncio.get_event_loop()

        t = threading.Thread(target=_run_coroutine, args=(loop, async_iterable, queue))
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