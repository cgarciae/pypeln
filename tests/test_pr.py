
import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz

from pypeln import pr

MAX_EXAMPLES = 15

############
# trivial
############

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(nums):

    nums_py = nums

    nums_pl = pr._from_iterable(nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py

############
# map
############

@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(nums):

    nums_py = nums

    nums_pl = pr.map(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(nums):

    
    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pr.map(lambda x: x ** 2, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pr.map(lambda x: x ** 2, nums, workers=2)
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

    nums_pl = pr.map(lambda x: x ** 2, nums)
    nums_pl = pr.flat_map(_generator, nums_pl)
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
    nums_py = cz.mapcat(_generator, nums)
    nums_py = list(nums_py)

    nums_pl = pr.map(lambda x: x ** 2, nums)
    nums_pl = pr.flat_map(_generator, nums, workers=3)
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

    nums_pl = pr.map(lambda x: x ** 2, nums)
    nums_pl = pr.flat_map(_generator, nums_pl, workers=3)
    nums_pl = pr.filter(lambda x: x > 1, nums_pl)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)


############
# concat
############


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_basic(nums):

    
    nums_py1 = map(lambda x: x ** 2, nums)
    nums_py2 = map(lambda x: -x, nums)
    nums_py = list(nums_py1) + list(nums_py2)

    nums_pl1 = pr.map(lambda x: x ** 2, nums)
    nums_pl2 = pr.map(lambda x: -x, nums)
    nums_pl = pr.concat([nums_pl1, nums_pl2])

    assert sorted(nums_pl) == sorted(nums_py)


@hp.given(nums = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_concat_multiple(nums):

    nums_py1 = nums + nums
    nums_py2 = nums_py1 + nums

    nums_pl1 = pr.concat([nums, nums])
    nums_pl2 = pr.concat([nums_pl1, nums])

    assert sorted(nums_py1) == sorted(list(nums_pl1))
    assert sorted(nums_py2) == sorted(list(nums_pl2))

