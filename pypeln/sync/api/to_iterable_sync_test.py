import time

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):
    nums_pl = nums
    nums_pl = pl.sync.from_iterable(nums_pl)
    nums_pl = cz.partition_all(10, nums_pl)
    nums_pl = pl.sync.map(sum, nums_pl)
    nums_pl = pl.sync.to_iterable(nums_pl)
    nums_pl = list(nums_pl)

    nums_py = nums
    nums_py = cz.partition_all(10, nums_py)
    nums_py = map(sum, nums_py)
    nums_py = list(nums_py)

    assert nums_py == nums_pl
