from aiohttp import ClientSession
import pypeln as pl
import asyncio
import sys


async def fetch(url, session):
    print("url")
    async with session.get(url) as response:
        return await response.read()


async def main():
    try:
        r = 10
        url = "http://google.com"

        # r = int(sys.argv[1])
        # url = "http://localhost:8080/{}"

        async with ClientSession() as session:
            data = range(r)
            await pl.task.each(
                lambda i: fetch(url, session), data, workers=1000, run=False
            )
    except BaseException as e:
        print(e)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

print("Finish")
