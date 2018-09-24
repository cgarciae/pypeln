
import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz

from pypeln import thread as th

MAX_EXAMPLES = 15

############
# trivial
############

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):

    nums_py = nums

    nums_pl = th.from_iterable(nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable_pipe(nums):

    nums_py = nums

    nums_pl = (
        nums
        | th.from_iterable()
        | list 
    )

    assert nums_pl == nums_py

############
# map
############

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(nums):

    nums_py = nums

    nums_pl = th.map(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id_pipe(nums):

    nums_pl = (
        nums
        | th.map(lambda x: x)
        | list
    )

    assert nums_pl == nums

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(nums):

    
    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = th.map(lambda x: x ** 2, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = th.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


############
# flat_map
############

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(nums):

    def _generator(x):
        yield x
        yield x + 1
        yield x + 2

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = th.map(lambda x: x ** 2, nums)
    nums_pl = th.flat_map(_generator, nums_pl)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(nums):

    def _generator(x):
        yield x
        yield x + 1
        yield x + 2
    
    nums_py = map(lambda x: x ** 2, nums)
    nums_py = cz.mapcat(_generator, nums_py)
    nums_py = list(nums_py)

    nums_pl = th.map(lambda x: x ** 2, nums)
    nums_pl = th.flat_map(_generator, nums_pl, workers=3)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)

############
# filter
############

@hp.given(nums = st.lists(st.integers()))
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

    nums_pl = th.map(lambda x: x ** 2, nums)
    nums_pl = th.flat_map(_generator, nums_pl, workers=3)
    nums_pl = th.filter(lambda x: x > 1, nums_pl)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums = st.lists(st.integers()))
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
        | th.map(lambda x: x ** 2)
        | th.flat_map(_generator, workers=3)
        | th.filter(lambda x: x > 1)
        | list
    )

    assert sorted(nums_pl) == sorted(nums_py)

############
# concat
############


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_basic(nums):

    nums_py = list(map(lambda x: x + 1, nums))
    nums_py1 = list(map(lambda x: x ** 2, nums_py))
    nums_py2 = list(map(lambda x: -x, nums_py))
    nums_py = nums_py1 + nums_py2

    nums_pl = th.map(lambda x: x + 1, nums)
    nums_pl1 = th.map(lambda x: x ** 2, nums_pl)
    nums_pl2 = th.map(lambda x: -x, nums_pl)
    nums_pl = th.concat([nums_pl1, nums_pl2])

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_multiple(nums):

    nums_py = [ x + 1 for x in nums ]
    nums_py1 = nums_py + nums_py
    nums_py2 = nums_py1 + nums_py

    nums_pl = th.map(lambda x: x + 1, nums)
    nums_pl1 = th.concat([nums_pl, nums_pl])
    nums_pl2 = th.concat([nums_pl1, nums_pl])

    assert sorted(nums_py1) == sorted(list(nums_pl1))
    assert sorted(nums_py2) == sorted(list(nums_pl2))

