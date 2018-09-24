# client-pypeln-aio.py

from aiohttp import ClientSession, TCPConnector
import asyncio
import sys
from pypeln import asyncio_task as aio

limit = 1000

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def main(url, total_requests):
    connector = TCPConnector(limit=None)
    async with ClientSession(connector=connector) as session:
        data = range(total_requests)
        await aio.each(lambda i: fetch(url.format(i), session), data, workers = limit, run = False)
    


url = "http://localhost:8080/{}"
loop = asyncio.get_event_loop()
loop.run_until_complete(main(url, int(sys.argv[1])))