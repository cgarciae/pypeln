import hypothesis as hp
from hypothesis import strategies as st
import time
import pypeln as pl

MAX_EXAMPLES = 10


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id(nums):

    nums_py = nums

    nums_pl = pl.sync.map(lambda x: x, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_id_pipe(nums):

    nums_pl = nums | pl.sync.map(lambda x: x) | list

    assert nums_pl == nums


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    nums_pl = pl.sync.map(lambda x: x ** 2, nums)
    nums_pl = list(nums_pl)

    assert nums_pl == nums_py


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_start(nums):

    nums_py = map(lambda x: x ** 2, nums)
    nums_py = list(nums_py)

    namespace = pl.sync.get_namespace()
    namespace.x = 0

    def on_start():
        namespace.x = 1

    nums_pl = pl.sync.map(lambda x: x ** 2, nums, on_start=on_start)
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

    nums_pl = pl.sync.map(f, nums, timeout=0.5)
    nums_pl = list(nums_pl)

    assert len(nums_pl) == 9


def test_worker_info():

    nums = range(100)
    n_workers = 4

    def on_start(worker_info):
        return dict(index=worker_info.index)

    def _lambda(x, index):
        return index

    nums_pl = pl.sync.map(_lambda, nums, on_start=on_start, workers=n_workers,)
    nums_pl = set(nums_pl)

    assert nums_pl.issubset(set(range(n_workers)))


def test_kwargs():

    nums = range(100)
    n_workers = 4
    letters = "abc"
    namespace = pl.sync.get_namespace()
    namespace.on_done = None

    def on_start():
        return dict(y=letters)

    def on_done(y):
        namespace.on_done = y

    nums_pl = pl.sync.map(
        lambda x, y: y, nums, on_start=on_start, on_done=on_done, workers=n_workers,
    )
    nums_pl = list(nums_pl)

    assert nums_pl == [letters] * len(nums)
    assert namespace.on_done == letters


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_map_square_event_end(nums):

    namespace = pl.sync.get_namespace()
    namespace.x = 0
    namespace.done = False
    namespace.active_workers = -1

    def on_start():
        namespace.x = 1

    def on_done(stage_status):
        namespace.x = 2
        namespace.active_workers = stage_status.active_workers
        namespace.done = stage_status.done

    nums_pl = pl.sync.map(
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

    nums_pl = pl.sync.map(lambda x: x ** 2, nums, workers=2)
    nums_pl = list(nums_pl)

    assert sorted(nums_pl) == sorted(nums_py)



    assert nums_pl == nums_py
