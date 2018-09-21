#!/usr/bin/env python3.5

from aiohttp import ClientSession
import asyncio
from pypeln import io
import fire


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def _main(url, total_requests, workers):

    async with ClientSession() as session:

        await io.each(
            lambda i: fetch(url.format(i), session), 
            range(total_requests), 
            workers = workers, 
            run = False,
        )
    

def main(total_requests, workers = 1000):
    url = "http://localhost:8080/{}"
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_main(url, total_requests, workers))

if __name__ == '__main__':
    fire.Fire(main)