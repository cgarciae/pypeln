import typing as tp
from unittest import TestCase

import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
import time

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(nums: tp.List[int]):

    nums_py = nums

    nums_pl = pl.process.map(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id_pipe(nums: tp.List[int]):

    nums_pl = nums | pl.process.map(lambda x: x) | list

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(nums: tp.List[int]):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_start(nums: tp.List[int]):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    namespace = pl.process.Namespace()
    namespace.x = 0

    def on_start():
        namespace.x = 1

    nums_pl = pl.process.map(lambda x: x ** 2, nums, on_start=on_start)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py
    assert namespace.x == 1


def test_timeout():

    nums = list(range(10))

    def f(x):
        if x == 2:
            while True:
                time.sleep(0.02)

        return x

    nums_pl = pl.process.map(f, nums, timeout=0.5)
    nums_pl = list(nums_pl)

    assert len(nums_pl) == 9


def test_worker_info():

    nums = range(100)
    n_workers = 4

    def on_start(worker_info):
        return dict(worker_info=worker_info)

    nums_pl = pl.process.map(
        lambda x, worker_info: worker_info.index,
        nums,
        on_start=on_start,
        workers=n_workers,
    )
    nums_pl = set(nums_pl)

    assert nums_pl.issubset(set(range(n_workers)))


def test_kwargs():

    nums = range(100)
    n_workers = 4
    letters = "abc"
    namespace = pl.process.Namespace()
    namespace.on_done = None

    def on_start():
        return dict(y=letters)

    def on_done(y):
        namespace.on_done = y

    nums_pl = pl.process.map(
        lambda x, y: y,
        nums,
        on_start=on_start,
        on_done=on_done,
        workers=n_workers,
    )
    nums_pl = list(nums_pl)

    assert namespace.on_done == letters
    assert nums_pl == [letters] * len(nums)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_end(nums: tp.List[int]):

    namespace = pl.process.Namespace()
    namespace.x = 0
    namespace.done = False
    namespace.active_workers = -1

    def on_start():
        namespace.x = 1

    def on_done(stage_status):
        namespace.x = 2
        namespace.active_workers = stage_status.active_workers
        namespace.done = stage_status.done

    nums_pl = pl.process.map(
        lambda x: x ** 2, nums, workers=3, on_start=on_start, on_done=on_done
    )
    nums_pl = list(nums_pl)

    time.sleep(0.1)

    assert namespace.x == 2
    assert namespace.done == True
    assert namespace.active_workers == 0


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers(nums: tp.List[int]):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


class MyError(Exception):
    pass


def test_error_handling():

    error = None

    def raise_error(x):
        raise MyError()

    stage = pl.process.map(raise_error, range(10))

    try:
        list(stage)

    except MyError as e:
        error = e

    assert isinstance(error, MyError)


def test_maxsize():

    namespace = pl.process.utils.Namespace(count=0)

    def f(x) -> tp.Any:
        namespace.count += 1
        return x

    stage = pl.process.map(f, range(20))
    stage = pl.process.to_iterable(stage, maxsize=3)

    iterator = iter(stage)
    next(iterator)

    time.sleep(0.1)

    # + 1 element which was yieled on next(...)
    # + 3 elements which are on the queue.
    # + 1 element which it pending to be put.
    # -------------------------------------------
    # + 5 total
    assert namespace.count == 5