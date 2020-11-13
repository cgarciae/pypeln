import typing as tp
from unittest import TestCase
from unittest import mock
import hypothesis as hp
from hypothesis import strategies as st
import pypeln as pl
import time
import pytest

MAX_EXAMPLES = 10
T = tp.TypeVar("T")


class MyException(Exception):
    pass


class TestWorkerThread(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_basic(self, nums):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(worker: pl.process.Worker):
            for x in nums:
                worker.stage_params.output_queues.put(x)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=True,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()

        nums_pl = list(output_queue)

        assert nums_pl == nums

    def test_raises(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(worker: pl.process.Worker) -> None:
            raise MyException()

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=True,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = list(output_queue)

    def test_timeout(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(self: pl.process.Worker):
            with self.measure_task_time():
                time.sleep(0.2)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=True,
            timeout=0.001,
            on_start=None,
            on_done=None,
            f_args=[],
        )
        worker.start()

        assert not worker.did_timeout()
        time.sleep(0.02)
        assert worker.did_timeout()

    def test_del1(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(self: pl.process.Worker):
            for _ in range(1000):
                time.sleep(0.01)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=True,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()
        process = worker.process

        worker.stop()
        time.sleep(0.1)

        assert not process.is_alive()

    def test_del3(
        self,
    ):
        def start_worker():
            input_queue = pl.process.IterableQueue()
            output_queue = pl.process.IterableQueue()
            output_queues = pl.process.OutputQueues([output_queue])

            def f(self: pl.process.Worker):
                for _ in range(1000):
                    time.sleep(0.01)

            stage_params: pl.process.StageParams = mock.Mock(
                input_queue=input_queue,
                output_queues=output_queues,
                total_workers=1,
            )

            stage_params: pl.process.StageParams = mock.Mock(
                input_queue=input_queue,
                output_queues=output_queues,
                total_workers=1,
            )

            worker = pl.process.Worker(
                process_fn=f,
                index=0,
                stage_params=stage_params,
                main_queue=output_queue,
                use_threads=True,
                timeout=0,
                on_start=None,
                on_done=None,
                f_args=[],
            )
            worker.start()

            time.sleep(0.01)

            assert worker.process.is_alive()

            worker.stop()

            return worker, worker.process

        worker, process = start_worker()

        assert process.is_alive()


class TestWorkerProcess(TestCase):
    @hp.given(nums=st.lists(st.integers()))
    @hp.settings(max_examples=MAX_EXAMPLES)
    def test_basic(self, nums):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(worker: pl.process.Worker):
            for x in nums:
                worker.stage_params.output_queues.put(x)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=False,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()

        nums_pl = list(output_queue)

        assert nums_pl == nums

    def test_raises(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(worker: pl.process.Worker) -> None:
            raise MyException()

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=False,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()

        with pytest.raises(MyException):
            nums_pl = list(output_queue)

    def test_timeout(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(self: pl.process.Worker):
            with self.measure_task_time():
                time.sleep(0.2)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=False,
            timeout=0.001,
            on_start=None,
            on_done=None,
            f_args=[],
        )
        worker.start()

        assert not worker.did_timeout()
        time.sleep(0.1)
        assert worker.did_timeout()

    def test_del1(
        self,
    ):
        input_queue = pl.process.IterableQueue()
        output_queue = pl.process.IterableQueue()
        output_queues = pl.process.OutputQueues([output_queue])

        def f(self: pl.process.Worker):
            for _ in range(1000):
                time.sleep(0.01)

        stage_params: pl.process.StageParams = mock.Mock(
            input_queue=input_queue,
            output_queues=output_queues,
            total_workers=1,
        )

        worker = pl.process.Worker(
            process_fn=f,
            index=0,
            stage_params=stage_params,
            main_queue=output_queue,
            use_threads=False,
            timeout=0,
            on_start=None,
            on_done=None,
            f_args=[],
        )

        worker.start()
        process = worker.process

        worker.stop()
        time.sleep(0.1)

        assert not process.is_alive()

    def test_del3(
        self,
    ):
        def start_worker():
            input_queue = pl.process.IterableQueue()
            output_queue = pl.process.IterableQueue()
            output_queues = pl.process.OutputQueues([output_queue])

            def f(self: pl.process.Worker):
                for _ in range(1000):
                    time.sleep(0.01)

            stage_params: pl.process.StageParams = mock.Mock(
                input_queue=input_queue,
                output_queues=output_queues,
                total_workers=1,
            )

            stage_params: pl.process.StageParams = mock.Mock(
                input_queue=input_queue,
                output_queues=output_queues,
                total_workers=1,
            )

            worker = pl.process.Worker(
                process_fn=f,
                index=0,
                stage_params=stage_params,
                main_queue=output_queue,
                use_threads=False,
                timeout=0,
                on_start=None,
                on_done=None,
                f_args=[],
            )
            worker.start()

            time.sleep(0.01)

            assert worker.process.is_alive()

            worker.stop()

            return worker, worker.process

        worker, process = start_worker()

        assert process.is_alive()
