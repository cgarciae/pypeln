import typing as tp
from unittest import TestCase
from unittest import mock
import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
import time
import pytest
from pypeln import utils as pypeln_utils

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


class MyException(Exception):
    pass


class TestQueue(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done(self, nums):

        queue = pl.thread.IterableQueue()

        def worker():
            for i in nums:
                queue.put(i)

            queue.worker_done()

        processes = pl.thread.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done_many(self, nums):
        n_workers = 3

        queue = pl.thread.IterableQueue(total_sources=n_workers)

        def worker():
            for i in nums:
                queue.put(i)

            queue.worker_done()

        processes = pl.thread.start_workers(worker, n_workers=n_workers)

        nums_pl = list(queue)

        assert len(processes) == n_workers
        assert len(nums_pl) == (len(nums) * 3)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_stop(self, nums):

        queue = pl.thread.IterableQueue()

        def worker():
            for i in nums:
                queue.put(i)

            time.sleep(0.01)
            queue.stop()

        processes = pl.thread.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_kill(self, nums):

        queue = pl.thread.IterableQueue()

        def worker():
            for i in nums:
                queue.put(i)

            queue.kill()

        processes = pl.thread.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert len(nums_pl) <= len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_raise(self, nums):

        queue = pl.thread.IterableQueue()

        def worker():
            try:
                raise MyException()
            except BaseException as e:
                queue.raise_exception(e)

        processes = pl.thread.start_workers(worker)

        with pytest.raises(MyException):
            nums_pl = list(queue)

        assert len(processes) == 1


# ----------------------------------------------------------------
# output queues
# ----------------------------------------------------------------


class TestOutputQueues(TestCase):
    def test_basic(self):
        queues: pl.thread.OutputQueues[int] = pl.thread.OutputQueues()
        queue: pl.thread.IterableQueue[int] = pl.thread.IterableQueue()

        queues.append(queue)

        queues.put(3)

        x = queue.get()

        assert isinstance(queues, list)
        assert x == 3

    def test_done(self):
        queues: pl.thread.OutputQueues[int] = pl.thread.OutputQueues()
        queue = pl.thread.IterableQueue()

        queues.append(queue)

        queues.worker_done()

        time.sleep(0.1)

        assert all(q.is_done() for q in queues)

    def test_stop(self):
        queues: pl.thread.OutputQueues[int] = pl.thread.OutputQueues()
        queue = pl.thread.IterableQueue()

        queues.append(queue)

        assert queue.namespace.remaining == 1

        queues.stop()

        assert queue.namespace.remaining == 0

    def test_kill(self):
        queues: pl.thread.OutputQueues[int] = pl.thread.OutputQueues()
        queue = pl.thread.IterableQueue()

        queues.append(queue)

        assert queue.namespace.force_stop == False

        queues.kill()

        assert queue.namespace.remaining == True
