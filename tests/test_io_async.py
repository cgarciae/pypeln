import hypothesis as hp
from hypothesis import strategies as st
import asyncio
from pypeln import io
from utils_async import run_async
from aiohttp import ClientSession


MAX_EXAMPLES = 15
X = [None]

############
# await
############

async def _impure_add1(x):
    X[0] = 1
    
    await asyncio.sleep(0.01)

    return x + 1

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@run_async
async def test_await(nums):
    X[0] = 0

    await io.map(_impure_add1, nums)

    assert len(nums) == 0 or X[0] == 1


async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

@run_async
async def test_iohttp():
    r = 3
    url = "http://google.com"

    async with ClientSession() as session:
        data = range(r)
        await io.each(lambda i: fetch(url, session), data, workers = 10, run = False)
