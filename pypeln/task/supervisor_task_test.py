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

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


def test_basic():
    queue = pl.task.IterableQueue()

    worker: pl.task.Worker = mock.Mock(timeout=1)

    supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

    time.sleep(0.1)

    supervisor.done = True

    worker.stop.assert_not_called()


def test_stop_nowait():
    queue = pl.task.IterableQueue()

    worker: pl.task.Worker = mock.Mock(timeout=1)

    supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

    time.sleep(0.1)

    supervisor.stop_nowait()

    worker.stop.assert_called_once()


@pl.task.utils.run_test_async
async def test_stop():
    queue = pl.task.IterableQueue()

    worker: pl.task.Worker = mock.Mock(timeout=1)

    supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

    time.sleep(0.1)

    await supervisor.stop()

    worker.stop.assert_called_once()


def test_context():
    queue: pl.task.IterableQueue = mock.Mock()

    worker: pl.task.Worker = mock.Mock(timeout=0, is_done=True)

    supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

    with supervisor:
        assert not supervisor.done
        worker.stop.assert_not_called()

    assert supervisor.done

    worker.stop.assert_called_once()


@pl.task.utils.run_test_async
async def test_context_async():
    queue: pl.task.IterableQueue = mock.Mock()

    worker: pl.task.Worker = mock.Mock(timeout=0, is_done=True)

    supervisor = pl.task.Supervisor(workers=[worker], main_queue=queue)

    async with supervisor:
        assert not supervisor.done
        worker.stop.assert_not_called()

    assert supervisor.done

    worker.stop.assert_called_once()


def test_context_gc():
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
