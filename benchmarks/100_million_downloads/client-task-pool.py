# client-task-pool.py

from aiohttp import ClientSession, TCPConnector
import asyncio
import sys
from pypeln.task import TaskPool

limit = 1000


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def _main(url, total_requests):
    connector = TCPConnector(limit=None)
    async with ClientSession(connector=connector) as session, TaskPool(limit) as tasks:

        for i in range(total_requests):
            await tasks.put(fetch(url.format(i), session))


url = "http://localhost:8080/{}"
loop = asyncio.get_event_loop()
loop.run_until_complete(_main(url, int(sys.argv[1])))
