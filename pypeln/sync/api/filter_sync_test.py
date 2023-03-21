import hypothesis as hp
from hypothesis import strategies as st
import time
import pypeln as pl
import cytoolz as cz

MAX_EXAMPLES = 10


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_filter_workers(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = cz.filter(lambda x: x > 1, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.sync.map(lambda x: x**2, nums)
    nums_pl = pl.sync.flat_map(_generator, nums_pl, workers=3)
    nums_pl = pl.sync.filter(lambda x: x > 1, nums_pl)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_filter_workers_pipe(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = cz.filter(lambda x: x > 1, nums_py)
    nums_py = list(nums_py)

    nums_pl = (
        nums
        | pl.sync.map(lambda x: x**2)
        | pl.sync.flat_map(_generator, workers=3)
        | pl.sync.filter(lambda x: x > 1)
        | list
    )

    assert sorted(nums_pl) == sorted(nums_py)
