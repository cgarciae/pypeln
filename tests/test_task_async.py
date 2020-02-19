import hypothesis as hp
from hypothesis import strategies as st
import asyncio
import pypeln as pl
from utils_io import run_async
from aiohttp import ClientSession


MAX_EXAMPLES = 15


############
# await
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@run_async
async def test_await(nums):
    X = [0]

    async def impure_add1(x):
        X[0] += 1

        await asyncio.sleep(0.01)

        return x + 1

    await pl.task.map(impure_add1, nums)

    assert X[0] == len(nums)


@run_async
async def test_iohttp():
    async def fetch(url, session):
        async with session.get(url) as response:
            return await response.read()

    r = 3
    url = "http://google.com"

    async with ClientSession() as session:
        data = range(r)
        await pl.task.each(lambda i: fetch(url, session), data, workers=10, run=False)
