import asyncio

class TaskPool(object):

    def __init__(self, workers = 1):
        self._limit = workers
        self._tasks = set()
        self._closed = False

    def filter_tasks(self):
        self._tasks = { task for task in self._tasks if not task.done() }

    async def join(self):

        await asyncio.gather(*self._tasks)

        self._closed = True
        

    async def put(self, coro):

        if self._closed:
            raise RuntimeError("Trying put items into a closed TaskPool")
        
        while self._limit > 0 and len(self._tasks) >= self._limit:
            done, _pending = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)

            self._tasks -= done

        
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)


    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()