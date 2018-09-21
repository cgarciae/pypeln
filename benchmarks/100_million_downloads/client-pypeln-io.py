#!/usr/bin/env python3.5

from aiohttp import ClientSession
import asyncio
import sys
from pypeln import io

limit = 1000

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def _main(url, total_requests):

    async with ClientSession() as session:

        await io.each(
            lambda i: fetch(url.format(i), session), 
            range(total_requests), 
            workers = limit, 
            run = False,
        )
    


url = "http://localhost:8080/{}"
loop = asyncio.get_event_loop()
loop.run_until_complete(_main(url, int(sys.argv[1])))