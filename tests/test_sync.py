import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz
import time

import pypeln as pl

MAX_EXAMPLES = 10

############
# trivial
############

############
# map
############

############
# ordered
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers_sorted(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.sync.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = pl.sync.ordered(nums_pl)
    nums_pl = list(nums_pl)


############
# each
############


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each(nums):

    nums_pl = pl.sync.each(lambda x: x, nums)
    pl.sync.run(nums_pl)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_each_list(nums):

    nums_pl = pl.sync.each(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == []


############
# flat_map
############


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

    nums_pl = pl.sync.map(lambda x: x ** 2, nums)
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

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = cz.filter(lambda x: x > 1, nums_py)
    nums_py = list(nums_py)

    nums_pl = (
        nums
        | pl.sync.map(lambda x: x ** 2)
        | pl.sync.flat_map(_generator, workers=3)
        | pl.sync.filter(lambda x: x > 1)
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

    nums_pl = pl.sync.map(lambda x: x + 1, nums)
    nums_pl1 = pl.sync.map(lambda x: x ** 2, nums_pl)
    nums_pl2 = pl.sync.map(lambda x: -x, nums_pl)
    nums_pl = pl.sync.concat([nums_pl1, nums_pl2])

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_multiple(nums):

    nums_py = [x + 1 for x in nums]
    nums_py1 = nums_py + nums_py
    nums_py2 = nums_py1 + nums_py

    nums_pl = pl.sync.map(lambda x: x + 1, nums)
    nums_pl1 = pl.sync.concat([nums_pl, nums_pl])
    nums_pl2 = pl.sync.concat([nums_pl1, nums_pl])

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

    stage = pl.sync.map(raise_error, range(10))

    try:
        list(stage)

    except MyError as e:
        error = e
    except BaseException as e:
        error = e
        print(e)

    assert isinstance(error, MyError)


###################
# from_to_iterable
###################
@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):

    nums_pl = nums
    nums_pl = pl.sync.from_iterable(nums_pl)
    nums_pl = cz.partition_all(10, nums_pl)
    nums_pl = pl.sync.map(sum, nums_pl)
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

    stage = pl.sync.map(raise_error, range(10))

    list(stage)
