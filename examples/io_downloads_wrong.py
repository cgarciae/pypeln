from aiohttp import ClientSession
from pypeln import io
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
    io.each(lambda i: fetch(url, session), data, workers=1000)

    print("FINISH")
    
main()
