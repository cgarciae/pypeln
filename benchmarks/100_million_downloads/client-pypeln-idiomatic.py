# client-pypeln-pl.task.py

from aiohttp import ClientSession, TCPConnector
import asyncio
import sys
import pypeln as pl


limit = 1000
urls = ("http://localhost:8080/{}".format(i) for i in range(int(sys.argv[1])))


async def main():

    async with ClientSession(connector=TCPConnector(limit=0)) as session:

        async def fetch(url):
            async with session.get(url) as response:
                return await response.read()

        await pl.task.each(
            fetch,
            urls,
            workers=limit,
        )


asyncio.run(main())
