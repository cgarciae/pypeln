import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz
import functools as ft
import time

import pypeln as pl

MAX_EXAMPLES = 15

############
# trivial
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):

    nums_py = nums

    nums_pl = pl.process.from_iterable(nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable_pipe(nums):

    nums_py = nums

    nums_pl = nums | pl.process.from_iterable() | list

    assert nums_pl == nums_py


############
# map
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(nums):

    nums_py = nums

    nums_pl = pl.process.map(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id_pipe(nums):

    nums_pl = nums | pl.process.map(lambda x: x) | list

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_start(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    namespace = pl.process.get_namespace()
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
                time.sleep(0.1)

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
    namespace = pl.process.get_namespace()
    namespace.on_done = None

    def on_start():
        return dict(y=letters)

    def on_done(y):
        namespace.on_done = y

    nums_pl = pl.process.map(
        lambda x, y: y, nums, on_start=on_start, on_done=on_done, workers=n_workers,
    )
    nums_pl = list(nums_pl)

    assert namespace.on_done == letters
    assert nums_pl == [letters] * len(nums)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_end(nums):

    namespace = pl.process.get_namespace()
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

    assert namespace.x == 2
    assert namespace.done == True
    assert namespace.active_workers == 0


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers_sorted(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = pl.process.sorted(nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


############
# flat_map
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums)
    nums_pl = pl.process.flat_map(_generator, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums)
    nums_pl = pl.process.flat_map(_generator, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


############
# filter
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_filter_workers(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = cz.filter(lambda x: x > 1, nums_py)
    nums_py = list(nums_py)

    nums_pl = pl.process.map(lambda x: x ** 2, nums)
    nums_pl = pl.process.flat_map(_generator, nums_pl, workers=3)
    nums_pl = pl.process.filter(lambda x: x > 1, nums_pl)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_filter_workers_pipe(nums):
    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = cz.filter(lambda x: x > 1, nums_py)
    nums_py = list(nums_py)

    nums_pl = (
        nums
        | pl.process.map(lambda x: x ** 2)
        | pl.process.flat_map(_generator, workers=3)
        | pl.process.filter(lambda x: x > 1)
        | list
    )

    assert sorted(nums_pl) == sorted(nums_py)


############
# concat
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_basic(nums):

    nums_py = list(map(lambda x: x + 1, nums))
    nums_py1 = list(map(lambda x: x ** 2, nums_py))
    nums_py2 = list(map(lambda x: -x, nums_py))
    nums_py = nums_py1 + nums_py2

    nums_pl = pl.process.map(lambda x: x + 1, nums)
    nums_pl1 = pl.process.map(lambda x: x ** 2, nums_pl)
    nums_pl2 = pl.process.map(lambda x: -x, nums_pl)
    nums_pl = pl.process.concat([nums_pl1, nums_pl2])

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_multiple(nums):

    nums_py = [x + 1 for x in nums]
    nums_py1 = nums_py + nums_py
    nums_py2 = nums_py1 + nums_py

    nums_pl = pl.process.map(lambda x: x + 1, nums)
    nums_pl1 = pl.process.concat([nums_pl, nums_pl])
    nums_pl2 = pl.process.concat([nums_pl1, nums_pl])

    # assert sorted(nums_py1) == sorted(list(nums_pl1))
    assert sorted(nums_py2) == sorted(list(nums_pl2))


###################
# error handling
###################


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


###################
# from_to_iterable
###################
@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):

    nums_pl = nums
    nums_pl = pl.process.from_iterable(nums_pl)
    nums_pl = cz.partition_all(10, nums_pl)
    nums_pl = pl.process.map(sum, nums_pl)
    nums_pl = list(nums_pl)

    nums_py = nums
    nums_py = cz.partition_all(10, nums_py)
    nums_py = map(sum, nums_py)
    nums_py = list(nums_py)

    assert nums_py == nums_pl


if __name__ == "__main__":
    error = None

    def raise_error(x):
        raise MyError()

    stage = pl.process.map(raise_error, range(10))

    list(stage)
