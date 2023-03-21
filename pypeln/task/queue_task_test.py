import asyncio
import sys
import time
import typing as tp
import unittest
from dataclasses import dataclass
from unittest import TestCase, mock

import cytoolz as cz
import hypothesis as hp
import pytest
from hypothesis import strategies as st

import pypeln as pl
from pypeln import utils as pypeln_utils
from pypeln.task.utils import run_test_async

MAX_EXAMPLES = 10
T = tp.TypeVar("T")
# ----------------------------------------------------------------
# queue
# ----------------------------------------------------------------


class MyException(Exception):
    pass


class TestQueue(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_done_async(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker)

        nums_pl = [x async for x in queue]

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_get(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker)

        if len(nums) > 0:
            x = await queue.get()
            assert x == nums[0]

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_get_2(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker)

        await asyncio.sleep(0.01)

        if len(nums) > 0:
            x = queue._get_nowait()
            assert x == nums[0]

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done_nowait(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            queue.worker_done_nowait()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done_many(self, nums):
        n_workers = 3

        queue = pl.task.IterableQueue(total_sources=n_workers)

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker, n_workers=n_workers)

        nums_pl = list(queue)

        assert len(processes) == n_workers
        assert len(nums_pl) == (len(nums) * 3)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_done_many_async(self, nums):
        n_workers = 3

        queue = pl.task.IterableQueue(total_sources=n_workers)

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.worker_done()

        processes = pl.task.start_workers(worker, n_workers=n_workers)

        nums_pl = [x async for x in queue]

        assert len(processes) == n_workers
        assert len(nums_pl) == (len(nums) * 3)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_stop(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.stop()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_stop_async(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.stop()

        processes = pl.task.start_workers(worker)

        nums_pl = [x async for x in queue]

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_stop_nowait(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            queue.stop_nowait()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_kill(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.kill()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert len(nums_pl) <= len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_kill_async(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.kill()

        processes = pl.task.start_workers(worker)

        nums_pl = [x async for x in queue]

        assert len(processes) == 1
        assert len(nums_pl) <= len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_kill_nowait(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            queue.kill_nowait()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert len(nums_pl) <= len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_raise(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            try:
                raise MyException()
            except BaseException as e:
                await queue.raise_exception(e)

        processes = pl.task.start_workers(worker)

        with pytest.raises(MyException):
            nums_pl = list(queue)

        assert len(processes) == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_test_async
    async def test_raise_async(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            try:
                raise MyException()
            except BaseException as e:
                await queue.raise_exception(e)

        processes = pl.task.start_workers(worker)

        with pytest.raises(MyException):
            nums_pl = [x async for x in queue]

        assert len(processes) == 1

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_raise_nowait(self, nums):
        queue = pl.task.IterableQueue()

        async def worker():
            try:
                raise MyException()
            except BaseException as e:
                queue.raise_exception_nowait(e)

        processes = pl.task.start_workers(worker)

        with pytest.raises(MyException):
            nums_pl = list(queue)

        assert len(processes) == 1


class TestOutputQueues(TestCase):
    def test_basic_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue: pl.task.IterableQueue[int] = pl.task.IterableQueue()

        queues.append(queue)

        queues.put_nowait(3)

        x = queue._get_nowait()

        assert isinstance(queues, list)
        assert x == 3

    @run_test_async
    async def test_basic(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue: pl.task.IterableQueue[int] = pl.task.IterableQueue()

        queues.append(queue)

        await queues.put(3)

        x = await queue.get()

        assert isinstance(queues, list)
        assert x == 3

    @run_test_async
    async def test_done(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        await queues.worker_done()

        x = await queue.get()

        assert isinstance(x, pl.utils.Done)

    def test_done_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        queues.worker_done_nowait()

        with pytest.raises(asyncio.QueueEmpty):
            x = queue._get_nowait()

    @run_test_async
    async def test_stop(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        assert queue.namespace.remaining == 1

        await queues.stop()

        assert queue.namespace.remaining == 0

    def test_stop_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        assert queue.namespace.remaining == 1

        queues.stop_nowait()

        assert queue.namespace.remaining == 0

    @run_test_async
    async def test_kill(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        assert queue.namespace.force_stop == False

        await queues.kill()

        assert queue.namespace.remaining == True

    def test_kill_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        assert queue.namespace.force_stop == False

        queues.kill_nowait()

        assert queue.namespace.remaining == True


class TestTaskPool(unittest.TestCase):
    @run_test_async
    async def test_basic(self):
        namespace = pl.task.Namespace(x=0)

        async def task():
            await asyncio.sleep(0.1)
            namespace.x = 1

        tasks = pl.task.TaskPool.create(workers=0)

        await tasks.put(task)

        assert namespace.x == 0

        await tasks.join()

        assert namespace.x == 1

    @run_test_async
    async def test_context(self):
        namespace = pl.task.Namespace(x=0)

        async def task():
            await asyncio.sleep(0.1)
            namespace.x = 1

        async with pl.task.TaskPool.create(workers=0) as tasks:
            await tasks.put(task)

        assert namespace.x == 1

    @run_test_async
    async def test_put_wait(self):
        timeout = 0.1
        namespace = pl.task.Namespace(x=0)

        async def task():
            await asyncio.sleep(timeout)
            namespace.x = 1

        async def no_task():
            pass

        async with pl.task.TaskPool.create(workers=1) as tasks:
            await tasks.put(task)
            assert len(tasks.tasks) == 1

            t0 = time.time()
            await tasks.put(no_task)

            assert time.time() - t0 > timeout

        assert namespace.x == 1

    @run_test_async
    async def test_put_no_wait(self):
        timeout = 0.1
        namespace = pl.task.Namespace(x=0)

        async def task():
            await asyncio.sleep(timeout)
            namespace.x = 1

        async def no_task():
            pass

        async with pl.task.TaskPool.create(workers=2) as tasks:
            await tasks.put(task)
            assert len(tasks.tasks) == 1

            t0 = time.time()
            await tasks.put(no_task)

            assert len(tasks.tasks) == 2
            assert time.time() - t0 < timeout

        assert namespace.x == 1
