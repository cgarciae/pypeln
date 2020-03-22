import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz
import functools as ft
import time
import random
import pypeln as pl

MAX_EXAMPLES = 15


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_process_thread(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.process.map(f, nums, workers=2)
    nums_pl = pl.thread.map(f, nums_pl, workers=2)
    nums_pl = pl.thread.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_process_sync(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.process.map(f, nums, workers=2)
    nums_pl = pl.sync.map(f, nums_pl, workers=2)
    nums_pl = pl.sync.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_thread_process(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.thread.map(f, nums, workers=2)
    nums_pl = pl.process.map(f, nums_pl, workers=2)
    nums_pl = pl.process.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_thread_sync(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.thread.map(f, nums, workers=2)
    nums_pl = pl.sync.map(f, nums_pl, workers=2)
    nums_pl = pl.sync.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_sync_process(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.sync.map(f, nums, workers=2)
    nums_pl = pl.process.map(f, nums_pl, workers=2)
    nums_pl = pl.process.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_sync_thread(nums):
    def f(x):
        time.sleep(random.random() * 0.01)
        return x

    nums_pl = pl.sync.map(f, nums, workers=2)
    nums_pl = pl.thread.map(f, nums_pl, workers=2)
    nums_pl = pl.thread.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums
