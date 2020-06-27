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


async def main():

    async with ClientSession(connector=TCPConnector(limit=None)) as session:

        await pl.task.each(
            lambda url: fetch(url, session), urls, workers=limit,
        )


asyncio.run(main())
