import typing as tp
from unittest import TestCase
import unittest
from unittest import mock

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st

from pypeln import utils as pypeln_utils
import pypeln as pl
from utils_io import run_test_async
import asyncio

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


# ----------------------------------------------------------------
# from iterable
# ----------------------------------------------------------------


# # ----------------------------------------------------------------
# # map
# # ----------------------------------------------------------------


class TestMap(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.task.map(lambda x: x, nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_id_async(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.task.map(lambda x: x, nums)
        nums_pl = await nums_pl

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_id_async_iterable(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.task.map(lambda x: x, nums)
        nums_pl = [x async for x in nums_pl]

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id_pipe(self, nums: tp.List[int]):

        nums_pl = nums | pl.task.map(lambda x: x) | list

        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_event_start(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.task.Namespace()
        namespace.x = 0

        def on_start():
            namespace.x = 1

        nums_pl = pl.task.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py
        assert namespace.x == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_event_start_async_1(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.task.Namespace()
        namespace.x = 0

        async def on_start():
            namespace.x = 1

        nums_pl = pl.task.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py
        assert namespace.x == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_square_event_start_async_2(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.task.Namespace()
        namespace.x = 0

        async def on_start():
            namespace.x = 1

        nums_pl = pl.task.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = await nums_pl

        assert nums_pl == nums_py
        assert namespace.x == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_square_event_start_async_2(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.task.Namespace()
        namespace.x = 0

        async def on_start():
            await asyncio.sleep(0.01)
            namespace.x = 1

        nums_pl = pl.task.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = await nums_pl

        assert nums_pl == nums_py
        assert namespace.x == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_square_event_start_async(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.task.Namespace()
        namespace.x = 0

        def on_start():
            namespace.x = 1

        nums_pl = pl.task.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = await nums_pl

        assert nums_pl == nums_py
        assert namespace.x == 1

    def test_timeout(self):

        nums = list(range(10))

        async def f(x):
            if x == 2:
                while True:
                    await asyncio.sleep(0.1)

            return x

        nums_pl = pl.task.map(f, nums, timeout=0.01)
        nums_pl = list(nums_pl)

        assert len(nums_pl) == 9

    @run_test_async
    async def test_timeout_async(self):

        nums = list(range(10))

        async def f(x):
            if x == 2:
                while True:
                    await asyncio.sleep(0.1)

            return x

        nums_pl = pl.task.map(f, nums, timeout=0.01)
        nums_pl = await nums_pl

        assert len(nums_pl) == 9

    def test_worker_info(self):

        nums = range(100)
        n_workers = 4

        def on_start(worker_info):
            return dict(worker_info=worker_info)

        nums_pl = pl.task.map(
            lambda x, worker_info: worker_info.index,
            nums,
            on_start=on_start,
            workers=n_workers,
        )
        nums_pl = set(nums_pl)

        assert nums_pl.issubset(set(range(n_workers)))

    @run_test_async
    async def test_worker_info_async(self):

        nums = range(100)
        n_workers = 4

        def on_start(worker_info):
            return dict(worker_info=worker_info)

        nums_pl = pl.task.map(
            lambda x, worker_info: worker_info.index,
            nums,
            on_start=on_start,
            workers=n_workers,
        )
        nums_pl = await nums_pl

        assert set(nums_pl).issubset(set(range(n_workers)))

    def test_kwargs(self):

        nums = range(100)
        n_workers = 4
        letters = "abc"
        namespace = pl.task.Namespace()
        namespace.on_done = None

        def on_start():
            return dict(y=letters)

        def on_done(y):
            namespace.on_done = y

        nums_pl = pl.task.map(
            lambda x, y: y, nums, on_start=on_start, on_done=on_done, workers=n_workers,
        )
        nums_pl = list(nums_pl)

        assert namespace.on_done == letters
        assert nums_pl == [letters] * len(nums)

    @run_test_async
    async def test_kwargs_async(self):

        nums = range(100)
        n_workers = 4
        letters = "abc"
        namespace = pl.task.Namespace()
        namespace.on_done = None

        def on_start():
            return dict(y=letters)

        def on_done(y):
            namespace.on_done = y

        nums_pl = pl.task.map(
            lambda x, y: y, nums, on_start=on_start, on_done=on_done, workers=n_workers,
        )
        nums_pl = await nums_pl

        assert namespace.on_done == letters
        assert nums_pl == [letters] * len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_event_end(self, nums: tp.List[int]):

        namespace = pl.task.Namespace()
        namespace.x = 0
        namespace.done = False
        namespace.active_workers = -1

        def on_start():
            namespace.x = 1

        def on_done(stage_status):
            namespace.x = 2
            namespace.active_workers = stage_status.active_workers
            namespace.done = stage_status.done

        nums_pl = pl.task.map(
            lambda x: x ** 2, nums, workers=3, on_start=on_start, on_done=on_done
        )
        nums_pl = list(nums_pl)

        assert namespace.x == 2
        assert namespace.done == True
        assert namespace.active_workers == 0

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_square_event_end_async(self, nums: tp.List[int]):

        namespace = pl.task.Namespace()
        namespace.x = 0
        namespace.done = False
        namespace.active_workers = -1

        def on_start():
            namespace.x = 1

        def on_done(stage_status):
            namespace.x = 2
            namespace.active_workers = stage_status.active_workers
            namespace.done = stage_status.done

        nums_pl = pl.task.map(
            lambda x: x ** 2, nums, workers=3, on_start=on_start, on_done=on_done
        )
        nums_pl = await nums_pl

        assert namespace.x == 2
        assert namespace.done == True
        assert namespace.active_workers == 0

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_map_square_workers_async(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = await nums_pl

        assert sorted(nums_pl) == sorted(nums_py)


class TestOrdered(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_workers_sorted(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = pl.task.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py


# ----------------------------------------------------------------
# each
# ----------------------------------------------------------------


class TestEach(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:
            pl.task.run(nums_pl)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_list(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:

            nums_pl = list(nums_pl)

            if nums:
                assert nums_pl != nums
            else:
                assert nums_pl == nums

            assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_run(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums, run=True)

        assert nums_pl is None

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_2(self, nums: tp.List[int]):

        nums_pl = pl.task.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:

            nums_pl = await nums_pl

            if nums:
                assert nums_pl != nums
            else:
                assert nums_pl == nums

            assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_3(self, nums: tp.List[int]):

        nums_pl = await pl.task.each(lambda x: x, nums)

        assert nums_pl == []

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_each_list_4(self, nums: tp.List[int]):

        nums_pl = await (pl.task.each(lambda x: x, nums))

        assert nums_pl == []


# ----------------------------------------------------------------
# flat_map
# ----------------------------------------------------------------


class TestFlatMap(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator, nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_async_1(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator_async, nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_async_2(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator_async, nums_pl)
        nums_pl = await nums_pl

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_async_3(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        async def nums_generator():
            for x in nums:
                yield x

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
        nums_pl = pl.task.flat_map(_generator_async, nums_pl)
        nums_pl = await nums_pl

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_workers(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator, nums_pl, workers=3)
        nums_pl = list(nums_pl)

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_workers_async_1(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
        nums_pl = list(nums_pl)

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_workers_async_2(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
        nums_pl = await nums_pl

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_workers_async_3(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            yield x + 2

        async def nums_generator():
            for x in nums:
                yield x

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
        nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3)
        nums_pl = await nums_pl

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_workers_async_3(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            await asyncio.sleep(0.1)
            yield x + 2

        async def nums_generator():
            for x in nums:
                yield x

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
        nums_pl = pl.task.flat_map(_generator_async, nums_pl, workers=3, timeout=0.01)
        nums_pl = await nums_pl

        assert nums_py == [] or sorted(nums_pl) != sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_workers_async_4(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            await asyncio.sleep(0.01)
            yield x + 2

        async def nums_generator():
            for x in nums:
                yield x

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
        nums_pl = pl.task.flat_map(
            _generator_async, nums_pl, workers=3, timeout=0.1, maxsize=2
        )
        nums_pl = await nums_pl

        assert nums_py == [] or sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_workers_async_5(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        async def _generator_async(x):
            yield x
            yield x + 1
            await asyncio.sleep(0.01)
            yield x + 2

        async def nums_generator():
            for x in nums:
                yield x

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums_generator())
        nums_pl = pl.task.flat_map(
            _generator_async, nums_pl, workers=3, timeout=0.1, maxsize=0
        )
        nums_pl = await nums_pl

        assert nums_py == [] or sorted(nums_pl) == sorted(nums_py)


# ----------------------------------------------------------------
# filter
# ----------------------------------------------------------------


class TestFilter(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_filter_workers(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = cz.filter(lambda x: x > 1, nums_py)
        nums_py = list(nums_py)

        nums_pl = pl.task.map(lambda x: x ** 2, nums)
        nums_pl = pl.task.flat_map(_generator, nums_pl, workers=2)
        nums_pl = pl.task.filter(lambda x: x > 1, nums_pl)
        nums_pl = list(nums_pl)

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_filter_workers_pipe(self, nums: tp.List[int]):
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
            | pl.task.map(lambda x: x ** 2)
            | pl.task.flat_map(_generator, workers=3)
            | pl.task.filter(lambda x: x > 1)
            | list
        )

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_flat_map_square_filter_workers_pipe_2(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = cz.filter(lambda x: x > 1, nums_py)
        nums_py = list(nums_py)

        async def gt1(x):
            return x > 1

        nums_pl = (
            nums
            | pl.task.map(lambda x: x ** 2)
            | pl.task.flat_map(_generator, workers=3)
            | pl.task.filter(gt1)
            | list
        )

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_flat_map_square_filter_workers_pipe_3(self, nums: tp.List[int]):
        def _generator(x):
            yield x
            yield x + 1
            yield x + 2

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = cz.mapcat(_generator, nums_py)
        nums_py = cz.filter(lambda x: x > 1, nums_py)
        nums_py = list(nums_py)

        async def gt1(x):
            return x > 1

        nums_pl = await (
            nums
            | pl.task.map(lambda x: x ** 2)
            | pl.task.flat_map(_generator, workers=3)
            | pl.task.filter(gt1)
        )

        assert sorted(nums_pl) == sorted(nums_py)


# ----------------------------------------------------------------
# concat
# ----------------------------------------------------------------


class TestConcat(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_basic(self, nums: tp.List[int]):

        nums_py = list(map(lambda x: x + 1, nums))
        nums_py1 = list(map(lambda x: x ** 2, nums_py))
        nums_py2 = list(map(lambda x: -x, nums_py))
        nums_py = nums_py1 + nums_py2

        nums_pl = pl.task.map(lambda x: x + 1, nums)
        nums_pl1 = pl.task.map(lambda x: x ** 2, nums_pl)
        nums_pl2 = pl.task.map(lambda x: -x, nums_pl)
        nums_pl = pl.task.concat([nums_pl1, nums_pl2])

        assert sorted(nums_pl) == sorted(nums_py)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_concat_basic_2(self, nums: tp.List[int]):

        nums_py = list(map(lambda x: x + 1, nums))
        nums_py1 = list(map(lambda x: x ** 2, nums_py))
        nums_py2 = list(map(lambda x: -x, nums_py))
        nums_py = nums_py1 + nums_py2

        nums_pl = pl.task.map(lambda x: x + 1, nums)
        nums_pl1 = pl.task.map(lambda x: x ** 2, nums_pl)
        nums_pl2 = pl.task.map(lambda x: -x, nums_pl)
        nums_pl = await pl.task.concat([nums_pl1, nums_pl2])

        assert sorted(nums_pl) == sorted(nums_py)

    # @hp.given(nums=st.lists(st.integers()))
    # @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_multiple(self, nums: tp.List[int] = [1, 2, 3]):

        nums_py = [x + 1 for x in nums]
        nums_py1 = nums_py + nums_py
        nums_py2 = nums_py1 + nums_py

        nums_pl = pl.task.map(lambda x: x + 1, nums)
        nums_pl1 = pl.task.concat([nums_pl, nums_pl])
        nums_pl2 = pl.task.concat([nums_pl1, nums_pl])

        # assert sorted(nums_py1) == sorted(list(nums_pl1))
        assert sorted(nums_py2) == sorted(list(nums_pl2))

    @run_test_async
    async def test_concat_multiple_2(self, nums: tp.List[int] = [1, 2, 3]):

        nums_py = [x + 1 for x in nums]
        nums_py1 = nums_py + nums_py
        nums_py2 = nums_py1 + nums_py

        nums_pl = pl.task.map(lambda x: x + 1, nums)
        nums_pl1 = pl.task.concat([nums_pl, nums_pl])
        nums_pl2 = await pl.task.concat([nums_pl1, nums_pl])

        # assert sorted(nums_py1) == sorted(list(nums_pl1))
        assert sorted(nums_py2) == sorted(list(nums_pl2))


# ----------------------------------------------------------------#######
# error handling
# ----------------------------------------------------------------#######


class MyError(Exception):
    pass


class TestErrorHandling(unittest.TestCase):
    def test_error_handling(self):

        error = None

        def raise_error(x):
            raise MyError()

        stage = pl.task.map(raise_error, range(10))

        try:
            list(stage)

        except MyError as e:
            error = e

        assert isinstance(error, MyError)

    @run_test_async
    async def test_error_handling_async(self):

        error = None

        def raise_error(x):
            raise MyError()

        stage = pl.task.map(raise_error, range(10))

        try:
            await stage

        except MyError as e:
            error = e

        assert isinstance(error, MyError)


# ----------------------------------------------------------------#######
# from_to_iterable
# ----------------------------------------------------------------#######


class TestToIterable(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable(self, nums: tp.List[int]):

        nums_pl = nums
        nums_pl = pl.task.from_iterable(nums_pl)
        nums_pl = cz.partition_all(10, nums_pl)
        nums_pl = pl.task.map(sum, nums_pl)
        nums_pl = pl.task.to_iterable(nums_pl)
        nums_pl = list(nums_pl)

        nums_py = nums
        nums_py = cz.partition_all(10, nums_py)
        nums_py = map(sum, nums_py)
        nums_py = list(nums_py)

        assert nums_py == nums_pl

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_from_to_iterable_2(self, nums: tp.List[int]):

        nums_pl = nums
        nums_pl = pl.task.from_iterable(nums_pl)
        nums_pl = cz.partition_all(10, nums_pl)
        nums_pl = pl.task.map(sum, nums_pl)
        nums_pl = pl.task.to_async_iterable(nums_pl)
        nums_pl = [x async for x in nums_pl]

        nums_py = nums
        nums_py = cz.partition_all(10, nums_py)
        nums_py = map(sum, nums_py)
        nums_py = list(nums_py)

        assert nums_py == nums_pl


# if __name__ == "__main__":
#     error = None

#     def raise_error(x):
#         raise MyError()

#     stage = pl.task.map(raise_error, range(10))

#     list(stage)
