from aiohttp import ClientSession
from pypeln import asyncio_task as aio
import asyncio
import sys

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def main():
    r = 10
    url = "http://google.com"

    # r = int(sys.argv[1])
    # url = "http://localhost:8080/{}"

    async with ClientSession() as session:
        data = range(r)
        await aio.each(lambda i: fetch(url, session), data, workers=1000, run = False)
    


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

print("Finish")