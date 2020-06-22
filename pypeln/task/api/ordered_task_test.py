import sys
import typing as tp
import unittest

import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_workers_sorted(nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @pl.task.utils.run_test_async
    async def test_map_square_workers_sorted_2(nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = await nums_pl

        assert nums_pl == nums_py
