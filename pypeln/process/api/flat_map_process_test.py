import time
import typing as tp
from unittest import TestCase

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(nums: tp.List[int]):
    def generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x**2, nums)
    nums_pl = pl.process.flat_map(generator, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(nums: tp.List[int]):
    def generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x**2, nums)
    nums_py = cz.mapcat(generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x**2, nums)
    nums_pl = pl.process.flat_map(generator, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)
