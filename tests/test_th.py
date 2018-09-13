
import hypothesis as hp
from hypothesis import strategies as st
import cytoolz as cz

from pypeln import th

MAX_EXAMPLES = 15

############
# trivial
############

@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_from_to_iterable(numbers):

    numbers_py = numbers

    numbers_pl = th._from_iterable(numbers)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py

############
# map
############

@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(numbers):

    numbers_py = numbers

    numbers_pl = th.map(lambda x: x, numbers)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py


@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(numbers):

    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = list(numbers_py)

    numbers_pl = th.map(lambda x: x ** 2, numbers)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py


@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_workers(numbers):

    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = list(numbers_py)

    numbers_pl = th.map(lambda x: x ** 2, numbers, workers=2)
    numbers_pl = list(numbers_pl)

    assert sorted(numbers_pl) == sorted(numbers_py)


############
# flat_map
############

@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square(numbers):

    def _generator(x):
        yield x
        yield x + 1
        yield x + 2
    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = cz.mapcat(_generator, numbers_py)
    numbers_py = list(numbers_py)

    numbers_pl = th.map(lambda x: x ** 2, numbers)
    numbers_pl = th.flat_map(_generator, numbers_pl)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py


@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_workers(numbers):

    def _generator(x):
        yield x
        yield x + 1
        yield x + 2
    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = cz.mapcat(_generator, numbers)
    numbers_py = list(numbers_py)

    numbers_pl = th.map(lambda x: x ** 2, numbers)
    numbers_pl = th.flat_map(_generator, numbers, workers=3)
    numbers_pl = list(numbers_pl)

    assert sorted(numbers_pl) == sorted(numbers_py)

############
# filter
############

@hp.given(numbers = st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_flat_map_square_filter_workers(numbers):

    def _generator(x):
        yield x
        yield x + 1
        yield x + 2
    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = cz.mapcat(_generator, numbers)
    numbers_py = cz.filter(lambda x: x > 1, numbers_py)
    numbers_py = list(numbers_py)

    numbers_pl = th.map(lambda x: x ** 2, numbers)
    numbers_pl = th.flat_map(_generator, numbers, workers=3)
    numbers_pl = th.filter(lambda x: x > 1, numbers_pl)
    numbers_pl = list(numbers_pl)

    assert sorted(numbers_pl) == sorted(numbers_py)

