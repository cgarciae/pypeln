import asyncio
from dataclasses import dataclass
import time
import typing as tp
from unittest import TestCase
import unittest
from unittest import mock
import sys

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st
import pytest

from pypeln import utils as pypeln_utils
import pypeln as pl

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


@dataclass
class CustomProcess:
    f: tp.Callable[..., tp.Awaitable]

    async def __call__(self, worker: pl.task.Worker, **kwargs):
        await self.f(worker, **kwargs)


class MyException(Exception):
    pass


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
def test_basic(nums):
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        for x in nums:
            await worker.stage_params.output_queues.put(x)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1),
    )

    worker.start()
    assert not worker.process.cancelled()

    nums_pl = list(output_queue)

    assert nums_pl == nums

    time.sleep(0.01)
    assert worker.is_done


@hp.given(nums=st.lists(st.integers()))
@hp.settings(max_examples=MAX_EXAMPLES)
@pl.task.utils.run_test_async
async def test_basic_async(nums):
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        for x in nums:
            await worker.stage_params.output_queues.put(x)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1),
    )

    worker.start()

    nums_pl = [x async for x in output_queue]

    assert nums_pl == nums

    await asyncio.sleep(0.01)
    assert worker.is_done


def test_raises():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        raise MyException()

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1),
    )

    worker.start()

    with pytest.raises(MyException):
        nums_pl = list(output_queue)

    time.sleep(0.01)
    assert worker.is_done


@pl.task.utils.run_test_async
async def test_raises_async():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        raise MyException()

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1),
    )

    worker.start()

    with pytest.raises(MyException):
        nums_pl = [x async for x in output_queue]

    await asyncio.sleep(0.01)
    assert worker.is_done


def test_timeout_base():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])
    namespace = pl.task.Namespace(x=0)

    async def f(worker: pl.task.Worker):
        async def task():
            for i in range(1000):
                await asyncio.sleep(0.01)
            namespace.x = 1

        await worker.tasks.put(task)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    timeout = 0.001

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=timeout,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1, timeout=timeout),
    )

    worker.start()

    # wait for worker to start
    time.sleep(0.02)

    while len(worker.tasks.tasks) > 0:
        time.sleep(0.001)

    assert namespace.x == 0

    time.sleep(0.1)

    assert worker.is_done


@pl.task.utils.run_test_async
async def test_timeout_async():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])
    namespace = pl.task.Namespace(x=0)

    async def f(worker: pl.task.Worker):
        async def task():
            await asyncio.sleep(0.01)
            namespace.x = 1

        await worker.tasks.put(task)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    timeout = 0.001

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=timeout,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(
            workers=1,
            timeout=timeout,
        ),
    )

    # wait for worker to start
    await asyncio.sleep(0.02)

    worker.start()

    while len(worker.tasks.tasks) > 0:
        await asyncio.sleep(0.01)

    assert namespace.x == 0

    await asyncio.sleep(0.01)

    assert worker.is_done


def test_no_timeout():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])
    namespace = pl.task.Namespace(x=0)

    async def f(worker: pl.task.Worker):
        async def task():
            await asyncio.sleep(0.01)
            namespace.x = 1

        await worker.tasks.put(task)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    timeout = 0.0

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=timeout,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=0, timeout=timeout),
    )
    worker.start()

    # wait for worker to start
    time.sleep(0.02)

    while len(worker.tasks.tasks) > 0:
        time.sleep(0.001)

    assert namespace.x == 1

    assert worker.is_done


@pl.task.utils.run_test_async
async def test_no_timeout_async():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])
    namespace = pl.task.Namespace(x=0)

    async def f(worker: pl.task.Worker):
        async def task():
            await asyncio.sleep(0.01)
            namespace.x = 1

        await worker.tasks.put(task)

    timeout = 0.02

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )
    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=timeout,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=0, timeout=timeout),
    )
    worker.start()

    # wait for worker to start
    await asyncio.sleep(0.02)

    await worker.tasks.join()

    assert namespace.x == 1

    assert worker.is_done


def test_del1():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        for _ in range(1000):
            await asyncio.sleep(0.01)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=1),
    )

    worker.start()
    task = worker.process

    time.sleep(0.01)

    worker.stop()
    time.sleep(0.2)

    assert task.cancelled()
    assert worker.is_done


@pl.task.utils.run_test_async
async def test_del1_async():
    input_queue = pl.task.IterableQueue()
    output_queue = pl.task.IterableQueue()
    output_queues = pl.task.OutputQueues([output_queue])

    async def f(worker: pl.task.Worker):
        for _ in range(1000):
            await asyncio.sleep(0.01)

    stage_params: pl.task.StageParams = mock.Mock(
        input_queue=input_queue,
        output_queues=output_queues,
        total_workers=1,
    )

    worker = pl.task.Worker.create(
        process_fn=CustomProcess(f),
        stage_params=stage_params,
        main_queue=output_queue,
        timeout=0,
        on_start=None,
        on_done=None,
        f_args=[],
        tasks=pl.task.TaskPool.create(workers=0),
    )

    worker.start()
    task = worker.process

    await asyncio.sleep(0.01)

    worker.stop()
    await asyncio.sleep(0.2)

    assert task.cancelled()
    assert worker.is_done


def test_del3():
    def start_worker():
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(worker: pl.task.Worker):
            for _ in range(1000):
                await asyncio.sleep(0.01)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.task.Worker.create(
            process_fn=CustomProcess(f),
            stage_params=stage_params,
            main_queue=output_queue,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
            tasks=pl.task.TaskPool.create(workers=1),
        )
        worker.start()

        time.sleep(0.01)

        assert not worker.process.cancelled()

        return worker, worker.process

    worker, task = start_worker()

    assert not task.cancelled()
    assert not worker.is_done


@pl.task.utils.run_test_async
async def test_del3_async():
    async def start_worker():
        input_queue = pl.task.IterableQueue()
        output_queue = pl.task.IterableQueue()
        output_queues = pl.task.OutputQueues([output_queue])

        async def f(worker: pl.task.Worker):
            for _ in range(1000):
                await asyncio.sleep(0.01)

        stage_params: pl.task.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.task.Worker.create(
            process_fn=CustomProcess(f),
            stage_params=stage_params,
            main_queue=output_queue,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
            tasks=pl.task.TaskPool.create(workers=1),
        )
        worker.start()

        await asyncio.sleep(0.01)

        assert not worker.process.cancelled()

        return worker, worker.process

    worker, task = await start_worker()

    await asyncio.sleep(0.01)

    assert not task.cancelled()
    assert not worker.is_done
