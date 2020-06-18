import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
import cytoolz as cz

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums: tp.List[int]):

    nums_pl = nums
    nums_pl = pl.process.from_iterable(nums_pl)
    nums_pl = cz.partition_all(10, nums_pl)
    nums_pl = pl.process.map(sum, nums_pl)
    nums_pl = pl.process.to_iterable(nums_pl)
    nums_pl = list(nums_pl)

    nums_py = nums
    nums_py = cz.partition_all(10, nums_py)
    nums_py = map(sum, nums_py)
    nums_py = list(nums_py)

    assert nums_py == nums_pl

