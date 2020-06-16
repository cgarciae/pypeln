import typing as tp
import aiohttp
import httpx


async def abc():
    pass


client = httpx.AsyncClient()
a = client.get("http://google.com")

print(isinstance(a, tp.Awaitable))
