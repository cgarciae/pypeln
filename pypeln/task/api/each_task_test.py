import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
from pypeln.task.utils import run_test_async
import time

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


class TestEach(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:
            pl.task.run(nums_pl)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_list(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:

            nums_pl = list(nums_pl)

            if nums:
                assert nums_pl != nums
            else:
                assert nums_pl == nums

            assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_run(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums, run=True)

        assert nums_pl is None

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_2(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:

            nums_pl = await nums_pl

            if nums:
                assert nums_pl != nums
            else:
                assert nums_pl == nums

            assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_3(self, nums: tp.List[int]):

        nums_pl = await pl.task.each(lambda x: x, nums)

        assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_4(self, nums: tp.List[int]):

        nums_pl = await (pl.task.each(lambda x: x, nums))

        assert nums_pl == []
