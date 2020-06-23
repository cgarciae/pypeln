import asyncio
import sys
import time
import typing as tp
from unittest import TestCase

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")



@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_async_1(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator_async, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_async_2(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator_async, nums_pl)
    nums_pl = await nums_pl

    assert nums_pl == nums_py

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_async_3(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    async def nums_generator():
        for x in nums:
            yield x

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
    nums_pl = pl.task.flat_map(_generator_async, nums_pl)
    nums_pl = await nums_pl

    assert nums_pl == nums_py

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers_async_1(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_workers_async_2(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums)
    nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
    nums_pl = await nums_pl

    assert sorted(nums_pl) == sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_workers_async_3(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        yield x + 2

    async def nums_generator():
        for x in nums:
            yield x

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
    nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
    nums_pl = await nums_pl

    assert sorted(nums_pl) == sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_workers_async_3(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        await asyncio.sleep(0.1)
        yield x + 2

    async def nums_generator():
        for x in nums:
            yield x

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
    nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3, timeout=0.01)
    nums_pl = await nums_pl

    assert nums_py == [] or sorted(nums_pl) != sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_workers_async_4(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        await asyncio.sleep(0.01)
        yield x + 2

    async def nums_generator():
        for x in nums:
            yield x

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
    nums_pl = pl.task.flat_map(
        _generator_async, nums_pl, workers=3, timeout=0.1, maxsize=2
    )
    nums_pl = await nums_pl

    assert nums_py == [] or sorted(nums_pl) == sorted(nums_py)

@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_flat_map_square_workers_async_5(nums: tp.List[int]):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    async def _generator_async(x):
        yield x
        yield x + 1
        await asyncio.sleep(0.01)
        yield x + 2

    async def nums_generator():
        for x in nums:
            yield x

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
    nums_pl = pl.task.flat_map(
        _generator_async, nums_pl, workers=3, timeout=0.1, maxsize=0
    )
    nums_pl = await nums_pl

    assert nums_py == [] or sorted(nums_pl) == sorted(nums_py)
