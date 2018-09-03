
import hypothesis as hp
from hypothesis import strategies as st

from pypeln import io

@hp.given(numbers = st.lists(st.integers()))
def test_map_id(numbers):

    
    numbers_py = numbers

    numbers_pl = io.map(lambda x: x, numbers)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py


@hp.given(numbers = st.lists(st.integers()))
def test_map_square(numbers):

    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = list(numbers_py)

    numbers_pl = io.map(lambda x: x ** 2, numbers)
    numbers_pl = list(numbers_pl)

    assert numbers_pl == numbers_py


@hp.given(numbers = st.lists(st.integers()))
def test_map_square_workers(numbers):

    
    numbers_py = map(lambda x: x ** 2, numbers)
    numbers_py = list(numbers_py)

    numbers_pl = io.map(lambda x: x ** 2, numbers, workers=2)
    numbers_pl = list(numbers_pl)

    assert sorted(numbers_pl) == sorted(numbers_py)