from dataclasses import dataclass
import time
import typing as tp
from unittest import TestCase
import unittest
from unittest import mock

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st
import pytest

from pypeln import utils as pypeln_utils
import pypeln as pl
from utils_io import run_async
import asyncio

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

            await queue.done()

        processes = pl.task.start_workers(worker)

        nums_pl = list(queue)

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_async
    async def test_done_async(self, nums):

        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.done()

        processes = pl.task.start_workers(worker)

        nums_pl = [x async for x in queue]

        assert len(processes) == 1
        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_async
    async def test_get(self, nums):

        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.done()

        processes = pl.task.start_workers(worker)

        if len(nums) > 0:
            x = await queue.get()
            assert x == nums[0]

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_async
    async def test_get_2(self, nums):

        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.done()

        processes = pl.task.start_workers(worker)

        await asyncio.sleep(0.01)

        if len(nums) > 0:
            x = queue.get_nowait()
            assert x == nums[0]

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_done_nowait(self, nums):

        queue = pl.task.IterableQueue()

        async def worker():
            for i in nums:
                await queue.put(i)

            queue.done_nowait()

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

            await queue.done()

        processes = pl.task.start_workers(worker, n_workers=n_workers)

        nums_pl = list(queue)

        assert len(processes) == n_workers
        assert len(nums_pl) == (len(nums) * 3)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_async
    async def test_done_many_async(self, nums):
        n_workers = 3

        queue = pl.task.IterableQueue(total_sources=n_workers)

        async def worker():
            for i in nums:
                await queue.put(i)

            await queue.done()

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
    @run_async
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
    @run_async
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
    @run_async
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


# ----------------------------------------------------------------
# output queues
# ----------------------------------------------------------------


class TestOutputQueues(TestCase):
    def test_basic_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue: pl.task.IterableQueue[int] = pl.task.IterableQueue()

        queues.append(queue)

        queues.put_nowait(3)

        x = queue.get_nowait()

        assert isinstance(queues, list)
        assert x == 3

    @run_async
    async def test_basic(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue: pl.task.IterableQueue[int] = pl.task.IterableQueue()

        queues.append(queue)

        await queues.put(3)

        x = await queue.get()

        assert isinstance(queues, list)
        assert x == 3

    @run_async
    async def test_done(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        await queues.done()

        x = await queue.get()

        assert isinstance(x, pypeln_utils.Done)

    def test_done_nowait(self):
        queues: pl.task.OutputQueues[int] = pl.task.OutputQueues()
        queue = pl.task.IterableQueue()

        queues.append(queue)

        queues.done_nowait()

        x = queue.get_nowait()

        assert isinstance(x, pypeln_utils.Done)

    @run_async
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

    @run_async
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
    @run_async
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

    @run_async
    async def test_context(self):

        namespace = pl.task.Namespace(x=0)

        async def task():
            await asyncio.sleep(0.1)
            namespace.x = 1

        async with pl.task.TaskPool.create(workers=0) as tasks:
            await tasks.put(task)

        assert namespace.x == 1

    @run_async
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

    @run_async
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


# ----------------------------------------------------------------
# worker
# ----------------------------------------------------------------


@dataclass
class CustomWorker(pl.task.Worker[int]):
    async def process_fn(self, f_args: tp.List[str], **kwargs):
        await self.f(self, **kwargs)


class TestWorker(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_basic(self, nums):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            for x in nums:
                await self.stage_params.output_queues.put(x)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f
        )

        worker.start()
        assert not worker.process.cancelled()

        nums_pl = list(output_queue)

        assert nums_pl == nums

        time.sleep(0.01)
        assert worker.is_done

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    @run_async
    async def test_basic_async(self, nums):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            for x in nums:
                await self.stage_params.output_queues.put(x)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f
        )

        worker.start()

        nums_pl = [x async for x in output_queue]

        assert nums_pl == nums

        await asyncio.sleep(0.01)
        assert worker.is_done

    def test_raises(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            raise MyException()

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = list(output_queue)

        time.sleep(0.01)
        assert worker.is_done

    @run_async
    async def test_raises_async(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            raise MyException()

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = [x async for x in output_queue]

        await asyncio.sleep(0.01)
        assert worker.is_done

    def test_timeout(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])
        namespace = pl.task.Namespace(x=0)

        async def f(self: CustomWorker):
            async def task():
                await asyncio.sleep(0.01)
                namespace.x = 1

            await self.tasks.put(task)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f, timeout=0.001,
        )

        # wait for worker to start
        time.sleep(0.02)

        worker.start()

        while len(worker.tasks.tasks) > 0:
            time.sleep(0.001)

        assert namespace.x == 0

        time.sleep(0.01)

        assert worker.is_done

    @run_async
    async def test_timeout_async(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])
        namespace = pl.task.Namespace(x=0)

        async def f(self: CustomWorker):
            async def task():
                await asyncio.sleep(0.01)
                namespace.x = 1

            await self.tasks.put(task)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f, timeout=0.001,
        )

        # wait for worker to start
        await asyncio.sleep(0.02)

        worker.start()

        await worker.tasks.join()

        assert namespace.x == 0

        await asyncio.sleep(0.01)

        assert worker.is_done

    def test_no_timeout(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])
        namespace = pl.task.Namespace(x=0)

        async def f(self: CustomWorker):
            async def task():
                await asyncio.sleep(0.01)
                namespace.x = 1

            await self.tasks.put(task)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker.create(
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            timeout=0.02,
            max_tasks=0,
        )
        worker.start()

        # wait for worker to start
        time.sleep(0.02)

        while len(worker.tasks.tasks) > 0:
            time.sleep(0.001)

        assert namespace.x == 1

        assert worker.is_done

    @run_async
    async def test_no_timeout_async(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])
        namespace = pl.task.Namespace(x=0)

        async def f(self: CustomWorker):
            async def task():
                await asyncio.sleep(0.01)
                namespace.x = 1

            await self.tasks.put(task)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker.create(
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            timeout=0.02,
            max_tasks=0,
        )
        worker.start()

        # wait for worker to start
        await asyncio.sleep(0.02)

        await worker.tasks.join()

        assert namespace.x == 1

        assert worker.is_done

    def test_del1(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            for _ in range(1000):
                await asyncio.sleep(0.01)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()
        task = worker.process

        time.sleep(0.01)

        worker.stop()
        time.sleep(0.02)

        assert task.cancelled()
        assert worker.is_done

    @run_async
    async def test_del1_async(self):
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(self: CustomWorker):
            for _ in range(1000):
                await asyncio.sleep(0.01)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker.create(
            stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()
        task = worker.process

        await asyncio.sleep(0.01)

        worker.stop()
        await asyncio.sleep(0.01)

        assert task.cancelled()
        assert worker.is_done

    def test_del3(self):
        def start_worker():
            input_queue = pl.task.IterableQueue()
            output_queue = pl.task.IterableQueue()
            output_queues = pl.task.OutputQueues([output_queue])

            async def f(self: CustomWorker):
                for _ in range(1000):
                    await asyncio.sleep(0.01)

            stage_params: pl.task.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            stage_params: pl.task.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            worker = CustomWorker.create(
                stage_params=stage_params, main_queue=output_queue, f=f,
            )
            worker.start()

            time.sleep(0.01)

            assert not worker.process.cancelled()

            return worker, worker.process

        worker, task = start_worker()

        assert not task.cancelled()
        assert not worker.is_done

    @run_async
    async def test_del3_async(self):
        async def start_worker():
            input_queue = pl.task.IterableQueue()
            output_queue = pl.task.IterableQueue()
            output_queues = pl.task.OutputQueues([output_queue])

            async def f(self: CustomWorker):
                for _ in range(1000):
                    await asyncio.sleep(0.01)

            stage_params: pl.task.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            stage_params: pl.task.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            worker = CustomWorker.create(
                stage_params=stage_params, main_queue=output_queue, f=f,
            )
            worker.start()

            await asyncio.sleep(0.01)

            assert not worker.process.cancelled()

            return worker, worker.process

        worker, task = await start_worker()

        await asyncio.sleep(0.01)

        assert not task.cancelled()
        assert not worker.is_done


class TestSupervisor(TestCase):
    def test_basic(self):

        queue = pl.task.IterableQueue()

        worker: pl.task.Worker = mock.Mock(timeout=1)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        time.sleep(0.1)

        supervisor.done = True

        worker.stop.assert_not_called()

    def test_stop_nowait(self):

        queue = pl.task.IterableQueue()

        worker: pl.task.Worker = mock.Mock(timeout=1)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        time.sleep(0.1)

        supervisor.stop_nowait()

        worker.stop.assert_called_once()

    @run_async
    async def test_stop(self):

        queue = pl.task.IterableQueue()

        worker: pl.task.Worker = mock.Mock(timeout=1)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        time.sleep(0.1)

        await supervisor.stop()

        worker.stop.assert_called_once()

    def test_context(self):

        queue: pl.task.IterableQueue = mock.Mock()

        worker: pl.task.Worker = mock.Mock(timeout=0, is_done=True)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        with supervisor:
            assert not supervisor.done
            worker.stop.assert_not_called()

        assert supervisor.done

        worker.stop.assert_called_once()

    @run_async
    async def test_context_async(self):

        queue: pl.task.IterableQueue = mock.Mock()

        worker: pl.task.Worker = mock.Mock(timeout=0, is_done=True)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        async with supervisor:
            assert not supervisor.done
            worker.stop.assert_not_called()

        assert supervisor.done

        worker.stop.assert_called_once()

    def test_context_gc(self):

        queue: pl.task.IterableQueue = mock.Mock()

        worker: pl.task.Worker = mock.Mock(timeout=0, is_done=True)

        supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

        def generator():

            with supervisor:
                yield

        iterator = iter(generator())
        next(iterator)

        assert not supervisor.done
        time.sleep(0.02)
        worker.stop.assert_not_called()

        del iterator
        time.sleep(0.02)

        assert supervisor.done
        worker.stop.assert_called_once()
