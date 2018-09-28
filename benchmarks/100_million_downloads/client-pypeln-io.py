# client-pypeln-aio.py

from aiohttp import ClientSession, TCPConnector
import asyncio
import sys
from pypeln import asyncio_task as aio


limit = 1000
urls = ( "http://localhost:8080/{}".format(i) for i in range(int(sys.argv[1])) )


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


aio.each(
    fetch, 
    urls,
    workers = limit,
    on_start = lambda: ClientSession(connector=TCPConnector(limit=None)),
    on_done = lambda _status, session: session.close(),
    run = True,
)