from aiohttp import ClientSession
from pypeln import asyncio_task as aio
import asyncio
import sys

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

def main():
    r = 10
    url = "http://google.com"

    # r = int(sys.argv[1])
    # url = "http://localhost:8080/{}"

    session = ClientSession()
    data = range(r)
    aio.each(lambda i: fetch(url, session), data, workers=1000)

    print("FINISH")
    
main()
