# client-async-as-completed.py

import asyncio
import sys
from itertools import islice

from aiohttp import ClientSession, TCPConnector


def limited_as_completed(coros, limit):
    futures = [asyncio.ensure_future(c) for c in islice(coros, 0, limit)]

    async def first_to_finish():
        while True:
            await asyncio.sleep(0)
            for f in futures:
                if f.done():
                    futures.remove(f)
                    try:
                        newf = next(coros)
                        futures.append(asyncio.ensure_future(newf))
                    except StopIteration as e:
                        pass
                    return f.result()

    while len(futures) > 0:
        yield first_to_finish()


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


limit = 1000


async def print_when_done(tasks):
    for res in limited_as_completed(tasks, limit):
        await res


r = int(sys.argv[1])
url = "http://localhost:8080/{}"
loop = asyncio.get_event_loop()


async def main():
    connector = TCPConnector(limit=None)
    async with ClientSession(connector=connector) as session:
        coros = (fetch(url.format(i), session) for i in range(r))
        await print_when_done(coros)


loop.run_until_complete(main())
loop.close()
