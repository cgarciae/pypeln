import asyncio

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