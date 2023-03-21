import time

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each(nums):
    nums_pl = pl.sync.each(lambda x: x, nums)
    pl.sync.run(nums_pl)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each_list(nums):
    nums_pl = pl.sync.each(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == []
