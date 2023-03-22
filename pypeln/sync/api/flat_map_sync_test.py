import time

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.sync.map(lambda x: x**2, nums)
    nums_pl = pl.sync.flat_map(_generator, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.sync.map(lambda x: x**2, nums)
    nums_pl = pl.sync.flat_map(_generator, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)
