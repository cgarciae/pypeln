import time
import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each(nums: tp.List[int]):
    nums_pl = pl.process.each(lambda x: x, nums)

    assert nums is not None

    if nums_pl is not None:
        pl.process.run(nums_pl)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each_list(nums: tp.List[int]):
    nums_pl = pl.process.each(lambda x: x, nums)

    assert nums is not None

    if nums_pl is not None:
        nums_pl = list(nums_pl)

        if nums:
            assert nums_pl != nums
        else:
            assert nums_pl == nums

        assert nums_pl == []
