import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums: tp.List[int]):

    nums_py = nums

    nums_pl = pl.thread.from_iterable(nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable_process(nums: tp.List[int]):

    nums_py = nums

    nums_pl = pl.thread.from_iterable(nums, use_thread=False)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable_pipe(nums):

    nums_py = nums

    nums_pl = nums | pl.thread.from_iterable() | list

    assert nums_pl == nums_py
