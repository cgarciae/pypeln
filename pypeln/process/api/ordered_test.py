import typing as tp
import unittest

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")



@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers_sorted(nums: tp.List[int]):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = pl.process.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py
