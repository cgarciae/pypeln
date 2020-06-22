import sys
import typing as tp
from unittest import TestCase

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

        nums_pl = nums
        nums_pl = pl.task.from_iterable(nums_pl)
        nums_pl = cz.partition_all(10, nums_pl)
        nums_pl = pl.task.map(sum, nums_pl)
        nums_pl = pl.task.to_iterable(nums_pl)
        nums_pl = list(nums_pl)

        nums_py = nums
        nums_py = cz.partition_all(10, nums_py)
        nums_py = map(sum, nums_py)
        nums_py = list(nums_py)

        assert nums_py == nums_pl

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @pl.task.utils.run_test_async
    async def test_from_to_iterable_2(nums: tp.List[int]):

        nums_pl = nums
        nums_pl = pl.task.from_iterable(nums_pl)
        nums_pl = cz.partition_all(10, nums_pl)
        nums_pl = pl.task.map(sum, nums_pl)
        nums_pl = pl.task.to_async_iterable(nums_pl)
        nums_pl = [x async for x in nums_pl]

        nums_py = nums
        nums_py = cz.partition_all(10, nums_py)
        nums_py = map(sum, nums_py)
        nums_py = list(nums_py)

        assert nums_py == nums_pl
