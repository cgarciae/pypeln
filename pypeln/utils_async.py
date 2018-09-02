import asyncio

class TaskPool(object):

    def __init__(self, workers = 1):
        self._limit = workers
        self._tasks = []

    def filter_tasks(self):
        self._tasks = [ task for task in self._tasks if not task.done() ]

    async def join(self):
        self.filter_tasks()

        if len(self._tasks) > 0:
            await asyncio.wait(self._tasks)

        self.filter_tasks()
        

    async def put(self, coro):
        self.filter_tasks()

        # wait until space is available
        while self._limit > 0 and len(self._tasks) >= self._limit:
            await asyncio.sleep(0)

            self.filter_tasks()

        
        task = asyncio.ensure_future(coro)
        self._tasks.append(task)


    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()