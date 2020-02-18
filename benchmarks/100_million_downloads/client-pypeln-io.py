# client-pypeln-pl.task.py

from aiohttp import ClientSession, TCPConnector
import asyncio
import sys
import pypeln as pl


limit = 1000
urls = ("http://localhost:8080/{}".format(i) for i in range(int(sys.argv[1])))


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


pl.task.each(
    fetch,
    urls,
    workers=limit,
    on_start=lambda: ClientSession(connector=TCPConnector(limit=None)),
    on_done=lambda _status, session: session.close(),
    run=True,
)
