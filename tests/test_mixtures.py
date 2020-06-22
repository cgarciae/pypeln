import random
import sys
import time

import hypothesis as hp
from hypothesis import strategies as st
import pytest

import pypeln as pl

MAX_EXAMPLES = 10
SLEEP = 0.001

#########################################################
# process
#########################################################
@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_process_thread(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.process.map(f, nums, workers=2)
    nums_pl = pl.thread.map(f, nums_pl, workers=2)
    nums_pl = pl.thread.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_process_sync(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.process.map(f, nums, workers=2)
    nums_pl = pl.sync.map(f, nums_pl, workers=2)
    nums_pl = pl.sync.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_process_task(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.process.map(f, nums, workers=2)
        nums_pl = pl.task.map(f, nums_pl, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums


#########################################################
# thread
#########################################################


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_thread_process(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.thread.map(f, nums, workers=2)
    nums_pl = pl.process.map(f, nums_pl, workers=2)
    nums_pl = pl.process.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_thread_sync(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.thread.map(f, nums, workers=2)
    nums_pl = pl.sync.map(f, nums_pl, workers=2)
    nums_pl = pl.sync.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_thread_task(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.thread.map(f, nums, workers=2)
        nums_pl = pl.task.map(f, nums_pl, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums


#########################################################
# sync
#########################################################
@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_sync_process(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.sync.map(f, nums, workers=2)
    nums_pl = pl.process.map(f, nums_pl, workers=2)
    nums_pl = pl.process.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_sync_thread(nums):
    def f(x):
        time.sleep(random.random() * SLEEP)
        return x

    nums_pl = pl.sync.map(f, nums, workers=2)
    nums_pl = pl.thread.map(f, nums_pl, workers=2)
    nums_pl = pl.thread.ordered(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums


if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_sync_task(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.sync.map(f, nums, workers=2)
        nums_pl = pl.task.map(f, nums_pl, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums


#########################################################
# task
#########################################################
if sys.version_info >= (3, 7):

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_task_process(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.task.map(f, nums, workers=2)
        nums_pl = pl.process.map(f, nums_pl, workers=2)
        nums_pl = pl.process.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_task_thread(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.task.map(f, nums, workers=2)
        nums_pl = pl.thread.map(f, nums_pl, workers=2)
        nums_pl = pl.thread.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_task_sync(nums):
        def f(x):
            time.sleep(random.random() * SLEEP)
            return x

        nums_pl = pl.task.map(f, nums, workers=2)
        nums_pl = pl.sync.map(f, nums_pl, workers=2)
        nums_pl = pl.sync.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums
