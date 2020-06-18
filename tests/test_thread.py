from dataclasses import dataclass, field
import functools as ft
import multiprocessing
import multiprocessing.synchronize
import threading
import time
import typing as tp
from typing import Iterable
from unittest import TestCase
import unittest
from unittest import mock

import cytoolz as cz
import hypothesis as hp
from hypothesis import strategies as st
import pytest

from pypeln import utils as pypeln_utils
import pypeln as pl

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

        queue = pl.thread.IterableQueue()

        def worker():
            for i in nums:
                queue.put(i)

            queue.done()

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

            queue.done()

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

        queues.done()

        x = queue.get()

        assert isinstance(x, pypeln_utils.Done)

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


# ----------------------------------------------------------------
# worker
# ----------------------------------------------------------------


@dataclass
class CustomWorker(pl.thread.Worker[int]):
    def process_fn(self, f_args: tp.List[str], **kwargs):
        self.f(self, **kwargs)


class TestWorkerProcess(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_basic(self, nums):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            for x in nums:
                self.stage_params.output_queues.put(x)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0, stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()

        nums_pl = list(output_queue)

        assert nums_pl == nums

    def test_raises(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            raise MyException()

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0, stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = list(output_queue)

        worker

    def test_timeout(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            with self.measure_task_time():
                time.sleep(0.2)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker(
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            timeout=0.001,
        )
        worker.start()

        assert not worker.did_timeout()
        time.sleep(0.01)
        assert worker.did_timeout()

    def test_del1(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            for _ in range(1000):
                time.sleep(0.01)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0, stage_params=stage_params, main_queue=output_queue, f=f,
        )

        worker.start()
        thread = worker.process

        worker.stop()
        time.sleep(0.01)

        assert not thread.is_alive()

    def test_del3(self):
        def start_worker():
            input_queue = pl.thread.IterableQueue()
            output_queue = pl.thread.IterableQueue()
            output_queues = pl.thread.OutputQueues([output_queue])

            def f(self: CustomWorker):
                for _ in range(1000):
                    time.sleep(0.01)

            stage_params: pl.thread.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            stage_params: pl.thread.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            worker = CustomWorker(
                index=0, stage_params=stage_params, main_queue=output_queue, f=f,
            )
            worker.start()

            time.sleep(0.01)

            assert worker.process.is_alive()

            return worker, worker.process

        worker, thread = start_worker()

        assert thread.is_alive()


class TestWorkerThread(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_basic(self, nums):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            for x in nums:
                self.stage_params.output_queues.put(x)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            use_threads=True,
        )

        worker.start()

        nums_pl = list(output_queue)

        assert nums_pl == nums

    def test_raises(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            raise MyException()

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            use_threads=True,
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = list(output_queue)

    def test_timeout(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            with self.measure_task_time():
                time.sleep(0.2)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )
        worker = CustomWorker(
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            timeout=0.001,
            use_threads=True,
        )
        worker.start()

        assert not worker.did_timeout()
        time.sleep(0.02)
        assert worker.did_timeout()

    def test_del1(self):
        input_queue = pl.thread.IterableQueue()
        output_queue = pl.thread.IterableQueue()
        output_queues = pl.thread.OutputQueues([output_queue])

        def f(self: CustomWorker):
            for _ in range(1000):
                time.sleep(0.01)

        stage_params: pl.thread.StageParams = mock.Mock(
            input_queue=input_queue, output_queues=output_queues, total_workers=1,
        )

        worker = CustomWorker(
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            f=f,
            use_threads=True,
        )

        worker.start()
        thread = worker.process

        worker.stop()
        time.sleep(0.1)

        assert not thread.is_alive()

    def test_del3(self):
        def start_worker():
            input_queue = pl.thread.IterableQueue()
            output_queue = pl.thread.IterableQueue()
            output_queues = pl.thread.OutputQueues([output_queue])

            def f(self: CustomWorker):
                for _ in range(1000):
                    time.sleep(0.01)

            stage_params: pl.thread.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            stage_params: pl.thread.StageParams = mock.Mock(
                input_queue=input_queue, output_queues=output_queues, total_workers=1,
            )

            worker = CustomWorker(
                index=0,
                stage_params=stage_params,
                main_queue=output_queue,
                f=f,
                use_threads=True,
            )
            worker.start()

            time.sleep(0.01)

            assert worker.process.is_alive()

            worker.stop()

            return worker, worker.process

        worker, thread = start_worker()

        assert thread.is_alive()


class TestSupervisor(TestCase):
    def test_basic(self):
        def did_timeout():
            while True:
                yield False

        queue = pl.thread.IterableQueue()

        worker: pl.thread.Worker = mock.Mock(timeout=1)

        worker.did_timeout.side_effect = did_timeout()

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)
        supervisor.start()

        time.sleep(0.1)

        supervisor.done = True

        worker.did_timeout.assert_called()
        worker.stop.assert_not_called()
        worker.start.assert_not_called()

    def test_timeout(self):
        def did_timeout():
            yield False
            yield True

            while True:
                yield False

        queue = pl.thread.IterableQueue()

        worker: pl.thread.Worker = mock.Mock(timeout=1)

        worker.did_timeout.side_effect = did_timeout()

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)
        supervisor.start()

        time.sleep(0.1)

        supervisor.done = True

        worker.did_timeout.assert_called()
        worker.stop.assert_called_once()
        worker.start.assert_called_once()

    def test_error(self):
        def did_timeout():
            yield False
            yield True

            yield ValueError()

            while True:
                yield False

        queue: pl.thread.IterableQueue = mock.Mock()

        worker: pl.thread.Worker = mock.Mock(timeout=1)

        worker.did_timeout.side_effect = did_timeout()

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)
        supervisor.start()

        time.sleep(0.2)

        supervisor.done = True

        worker.did_timeout.assert_called()
        worker.stop.assert_called_once()
        worker.start.assert_called_once()

        queue.raise_exception.assert_called_once()

    def test_no_timeout(self):

        queue: pl.thread.IterableQueue = mock.Mock()

        worker: pl.thread.Worker = mock.Mock(timeout=0)

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)
        supervisor.start()

        time.sleep(0.2)

        supervisor.done = True

        worker.did_timeout.assert_not_called()
        worker.stop.assert_not_called()
        worker.start.assert_not_called()

        queue.raise_exception.assert_not_called()

    def test_context(self):

        queue: pl.thread.IterableQueue = mock.Mock()

        worker: pl.thread.Worker = mock.Mock(timeout=0)
        worker.process.is_alive.return_value = False

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)

        with supervisor:
            assert not supervisor.done
            worker.stop.assert_not_called()
            worker.did_timeout.assert_not_called()

        assert supervisor.done

        worker.stop.assert_called_once()

    def test_context_gc(self):

        queue: pl.thread.IterableQueue = mock.Mock()

        worker: pl.thread.Worker = mock.Mock(timeout=0)
        worker.process.is_alive.return_value = False

        supervisor = pl.thread.Supervisor(workers=[worker], main_queue=queue)

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


# ----------------------------------------------------------------
# from iterable
# ----------------------------------------------------------------


class TestFromIterable(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.thread.from_iterable(nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable_process(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.thread.from_iterable(nums, use_thread=False)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_from_to_iterable_pipe(self, nums):

        nums_py = nums

        nums_pl = nums | pl.thread.from_iterable() | list

        assert nums_pl == nums_py


# ----------------------------------------------------------------
# map
# ----------------------------------------------------------------


class TestMap(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id(self, nums: tp.List[int]):

        nums_py = nums

        nums_pl = pl.thread.map(lambda x: x, nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_id_pipe(self, nums: tp.List[int]):

        nums_pl = nums | pl.thread.map(lambda x: x) | list

        assert nums_pl == nums

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.thread.map(lambda x: x ** 2, nums)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_event_start(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        namespace = pl.thread.Namespace()
        namespace.x = 0

        def on_start():
            namespace.x = 1

        nums_pl = pl.thread.map(lambda x: x ** 2, nums, on_start=on_start)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py
        assert namespace.x == 1

    def test_timeout(self):

        nums = list(range(10))

        def f(x):
            if x == 2:
                while True:
                    time.sleep(0.1)

            return x

        nums_pl = pl.thread.map(f, nums, timeout=0.5)
        nums_pl = list(nums_pl)

        assert len(nums_pl) == 9

    def test_worker_info(self):

        nums = range(100)
        n_workers = 4

        def on_start(worker_info):
            return dict(worker_info=worker_info)

        nums_pl = pl.thread.map(
            lambda x, worker_info: worker_info.index,
            nums,
            on_start=on_start,
            workers=n_workers,
        )
        nums_pl = set(nums_pl)

        assert nums_pl.issubset(set(range(n_workers)))

    def test_kwargs(self):

        nums = range(100)
        n_workers = 4
        letters = "abc"
        namespace = pl.thread.Namespace()
        namespace.on_done = None

        def on_start():
            return dict(y=letters)

        def on_done(y):
            namespace.on_done = y

        nums_pl = pl.thread.map(
            lambda x, y: y, nums, on_start=on_start, on_done=on_done, workers=n_workers,
        )
        nums_pl = list(nums_pl)

        assert namespace.on_done == letters
        assert nums_pl == [letters] * len(nums)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_event_end(self, nums: tp.List[int]):

        namespace = pl.thread.Namespace()
        namespace.x = 0
        namespace.done = False
        namespace.active_workers = -1

        def on_start():
            namespace.x = 1

        def on_done(stage_status):
            namespace.x = 2
            namespace.active_workers = stage_status.active_workers
            namespace.done = stage_status.done

        nums_pl = pl.thread.map(
            lambda x: x ** 2, nums, workers=3, on_start=on_start, on_done=on_done
        )
        nums_pl = list(nums_pl)

        assert namespace.x == 2
        assert namespace.done == True
        assert namespace.active_workers == 0

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_workers(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.thread.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = list(nums_pl)

        assert sorted(nums_pl) == sorted(nums_py)


class TestOrdered(unittest.TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_map_square_workers_sorted(self, nums: tp.List[int]):

        nums_py = map(lambda x: x ** 2, nums)
        nums_py = list(nums_py)

        nums_pl = pl.thread.map(lambda x: x ** 2, nums, workers=2)
        nums_pl = pl.thread.ordered(nums_pl)
        nums_pl = list(nums_pl)

        assert nums_pl == nums_py


# ----------------------------------------------------------------
# each
# ----------------------------------------------------------------


class TestEach(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each(self, nums: tp.List[int]):

        nums_pl = pl.thread.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:
            pl.thread.run(nums_pl)

    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_each_list(self, nums: tp.List[int]):

        nums_pl = pl.thread.each(lambda x: x, nums)

        assert nums is not None

        if nums_pl is not None:

            nums_pl = list(nums_pl)

            if nums:
                assert nums_pl != nums
            else:
                assert nums_pl == nums

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

        nums_pl = pl.thread.map(lambda x: x ** 2, nums)
        nums_pl = pl.thread.flat_map(_generator, nums_pl)
        nums_pl = list(nums_pl)

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

        nums_pl = pl.thread.map(lambda x: x ** 2, nums)
        nums_pl = pl.thread.flat_map(_generator, nums_pl, workers=3)
        nums_pl = list(nums_pl)

        assert sorted(nums_pl) == sorted(nums_py)


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

        nums_pl = pl.thread.map(lambda x: x ** 2, nums)
        nums_pl = pl.thread.flat_map(_generator, nums_pl, workers=2)
        nums_pl = pl.thread.filter(lambda x: x > 1, nums_pl)
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
            | pl.thread.map(lambda x: x ** 2)
            | pl.thread.flat_map(_generator, workers=3)
            | pl.thread.filter(lambda x: x > 1)
            | list
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

        nums_pl = pl.thread.map(lambda x: x + 1, nums)
        nums_pl1 = pl.thread.map(lambda x: x ** 2, nums_pl)
        nums_pl2 = pl.thread.map(lambda x: -x, nums_pl)
        nums_pl = pl.thread.concat([nums_pl1, nums_pl2])

        assert sorted(nums_pl) == sorted(nums_py)

    # @hp.given(nums=st.lists(st.integers()))
    # @hp.settings(max_examples=MAX_EXAMPLES)
    def test_concat_multiple(self, nums: tp.List[int] = [1, 2, 3]):

        nums_py = [x + 1 for x in nums]
        nums_py1 = nums_py + nums_py
        nums_py2 = nums_py1 + nums_py

        nums_pl = pl.thread.map(lambda x: x + 1, nums)
        nums_pl1 = pl.thread.concat([nums_pl, nums_pl])
        nums_pl2 = pl.thread.concat([nums_pl1, nums_pl])

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

        stage = pl.thread.map(raise_error, range(10))

        try:
            list(stage)

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
        nums_pl = pl.thread.from_iterable(nums_pl)
        nums_pl = cz.partition_all(10, nums_pl)
        nums_pl = pl.thread.map(sum, nums_pl)
        nums_pl = pl.thread.to_iterable(nums_pl)
        nums_pl = list(nums_pl)

        nums_py = nums
        nums_py = cz.partition_all(10, nums_py)
        nums_py = map(sum, nums_py)
        nums_py = list(nums_py)

        assert nums_py == nums_pl


if __name__ == "__main__":
    error = None

    def raise_error(x):
        raise MyError()

    stage = pl.thread.map(raise_error, range(10))

    list(stage)
