# client-async-sem.py

import asyncio
import sys

from aiohttp import ClientSession, TCPConnector

limit = 1000


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()


async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        await fetch(url, session)


async def run(session, r):
    url = "http://localhost:8080/{}"
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(limit)
    for i in range(r):
        # pass Semaphore and session to every GET request
        task = asyncio.ensure_future(bound_fetch(sem, url.format(i), session))
        tasks.append(task)
    responses = asyncio.gather(*tasks)
    await responses


loop = asyncio.get_event_loop()


async def main():
    connector = TCPConnector(limit=None)
    async with ClientSession(connector=connector) as session:
        await run(session, int(sys.argv[1]))


loop.run_until_complete(main())
