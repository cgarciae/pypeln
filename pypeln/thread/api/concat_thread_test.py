import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl


MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_basic(nums: tp.List[int]):

    nums_py = list(map(lambda x: x + 1, nums))
    nums_py1 = list(map(lambda x: x**2, nums_py))
    nums_py2 = list(map(lambda x: -x, nums_py))
    nums_py = nums_py1 + nums_py2

    nums_pl = pl.thread.map(lambda x: x + 1, nums)
    nums_pl1 = pl.thread.map(lambda x: x**2, nums_pl)
    nums_pl2 = pl.thread.map(lambda x: -x, nums_pl)
    nums_pl = pl.thread.concat([nums_pl1, nums_pl2])

    assert sorted(nums_pl) == sorted(nums_py)


# @hp.given(nums=st.lists(st.integers()))
# @hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_multiple(nums: tp.List[int] = [1, 2, 3]):

    nums_py = [x + 1 for x in nums]
    nums_py1 = nums_py + nums_py
    nums_py2 = nums_py1 + nums_py

    nums_pl = pl.thread.map(lambda x: x + 1, nums)
    nums_pl1 = pl.thread.concat([nums_pl, nums_pl])
    nums_pl2 = pl.thread.concat([nums_pl1, nums_pl])

    # assert sorted(nums_py1) == sorted(list(nums_pl1))
    assert sorted(nums_py2) == sorted(list(nums_pl2))
