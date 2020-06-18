import typing as tp
from unittest import TestCase
from unittest import mock
import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
import time

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


def test_basic():
    def did_timeout():
        while True:
            yield False

    queue = pl.process.IterableQueue()

    worker: pl.process.Worker = mock.Mock(timeout=1)

    worker.did_timeout.side_effect = did_timeout()

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)
    supervisor.start()

    time.sleep(0.1)

    supervisor.done = True

    worker.did_timeout.assert_called()
    worker.stop.assert_not_called()
    worker.start.assert_called_once()


def test_timeout():
    def did_timeout():
        yield False
        yield True

        while True:
            yield False

    queue = pl.process.IterableQueue()

    worker: pl.process.Worker = mock.Mock(timeout=1)

    worker.did_timeout.side_effect = did_timeout()

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)
    supervisor.start()

    time.sleep(0.1)

    supervisor.done = True

    worker.did_timeout.assert_called()
    worker.stop.assert_called_once()
    worker.start.assert_called()


def test_error():
    def did_timeout():
        yield False
        yield True

        yield ValueError()

        while True:
            yield False

    queue: pl.process.IterableQueue = mock.Mock()

    worker: pl.process.Worker = mock.Mock(timeout=1)

    worker.did_timeout.side_effect = did_timeout()

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)
    supervisor.start()

    time.sleep(0.2)

    supervisor.done = True

    worker.did_timeout.assert_called()
    worker.stop.assert_called_once()
    worker.start.assert_called()

    queue.raise_exception.assert_called_once()


def test_no_timeout():

    queue: pl.process.IterableQueue = mock.Mock()

    worker: pl.process.Worker = mock.Mock(timeout=0)

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)
    supervisor.start()

    time.sleep(0.2)

    supervisor.done = True

    worker.did_timeout.assert_not_called()
    worker.stop.assert_not_called()
    worker.start.assert_called_once()

    queue.raise_exception.assert_not_called()


def test_context():

    queue: pl.process.IterableQueue = mock.Mock()

    worker: pl.process.Worker = mock.Mock(timeout=0)
    worker.process.is_alive.return_value = False

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)

    with supervisor:
        assert not supervisor.done
        worker.stop.assert_not_called()
        worker.did_timeout.assert_not_called()

    assert supervisor.done

    worker.stop.assert_called_once()


def test_context_gc():

    queue: pl.process.IterableQueue = mock.Mock()

    worker: pl.process.Worker = mock.Mock(timeout=0)
    worker.process.is_alive.return_value = False

    supervisor = pl.process.Supervisor(workers=[worker], main_queue=queue)

    def generator():

        with supervisor:
            yield

    iterator = iter(generator())
    next(iterator)

    assert not supervisor.done
    worker.stop.assert_not_called()
    worker.did_timeout.assert_not_called()

    del iterator
    time.sleep(0.02)

    assert supervisor.done
    worker.stop.assert_called_once()

