import asyncio
import sys
import typing as tp
from unittest import TestCase
import unittest
from unittest import mock

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable(nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.task.from_iterable(nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @pl.task.utils.run_test_async
    async def test_from_to_iterable_async_1(nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.task.from_iterable(nums)
        nums_pl = [x async for x in nums_pl]

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable_async_iterable(nums: tp.List[int]):

        nums_py = nums

        async def iterable():
            for x in nums:
                yield x

        nums_pl = pl.task.from_iterable(iterable())
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable_pipe(nums):

        nums_py = nums

        nums_pl = nums | pl.task.from_iterable() | list

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable_pipe_async_iterable(nums):

        nums_py = nums

        async def iterable():
            for x in nums:
                yield x

        nums_pl = iterable() | pl.task.from_iterable() | list

        assert nums_pl == nums_py
