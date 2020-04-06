import asyncio
import inspect
import sys
import traceback
from collections import namedtuple
from queue import Queue
from threading import Lock, Thread
from abc import ABC, abstractmethod

from pypeln import utils as pypeln_utils

from . import utils


class Stage(pypeln_utils.BaseStage):
    def __init__(
        self, f, workers, maxsize, on_start, on_done, dependencies, timeout,
    ):
        self.f = f
        self.workers = workers
        self.maxsize = maxsize
        self.on_start = on_start
        self.on_done = on_done
        self.timeout = timeout
        self.dependencies = dependencies
        self.output_queues = utils.MultiQueue()
        self.f_args = pypeln_utils.function_args(self.f) if self.f else set()
        self.on_start_args = (
            pypeln_utils.function_args(self.on_start) if self.on_start else set()
        )
        self.on_done_args = (
            pypeln_utils.function_args(self.on_done) if self.on_done else set()
        )
        ######################################
        # build fields
        ######################################
        self.input_queue = None
        self.stage_namespace = None
        self.stage_lock = None
        self.pipeline_namespace = None
        self.pipeline_error_queue = None
        self.pipeline_stages = None
        self.loop = None

    async def process(self, **kwargs) -> None:
        async with utils.TaskPool(self.workers) as tasks:
            async for x in self.input_queue:
                task = self.apply(x, **kwargs)

                if self.timeout:
                    task = asyncio.wait_for(task, timeout=self.timeout)

                await tasks.put(task)

    async def run(self):
        worker_info = pypeln_utils.WorkerInfo(index=0)

        try:
            if self.on_start is not None:
                on_start_kwargs = dict(worker_info=worker_info)
                kwargs = self.on_start(
                    **{
                        key: value
                        for key, value in on_start_kwargs.items()
                        if key in self.on_start_args
                    }
                )
            else:
                kwargs = {}

            if kwargs is None:
                kwargs = {}

            if hasattr(kwargs, "__await__"):
                kwargs = await kwargs

            kwargs.setdefault("worker_info", worker_info)

            await self.process(
                **{key: value for key, value in kwargs.items() if key in self.f_args}
            )

            if self.on_done is not None:

                kwargs.setdefault(
                    "stage_status", utils.StageStatus(),
                )

                done_resp = self.on_done(
                    **{
                        key: value
                        for key, value in kwargs.items()
                        if key in self.on_done_args
                    }
                )

                if hasattr(done_resp, "__await__"):
                    await done_resp

            await self.output_queues.done()

        except BaseException as e:
            for stage in self.pipeline_stages:
                await stage.input_queue.done()

            try:
                self.pipeline_error_queue.put_nowait(
                    (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
                )
                self.pipeline_namespace.error = True
            except BaseException as e:
                print(e)

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)

    def __aiter__(self):
        return self.to_async_iterable(maxsize=0, return_index=False).__aiter__()

    async def to_async_iterable(self, maxsize, return_index):

        pipeline_namespace = utils.get_namespace()
        pipeline_namespace.error = False
        pipeline_error_queue = Queue()

        f_coro, output_queue = self.to_coroutine(
            maxsize=maxsize,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
            loop=asyncio.get_event_loop(),
        )

        self.loop.create_task(f_coro())

        async for elem in output_queue:
            if return_index:
                yield elem
            else:
                yield elem.value

        if pipeline_namespace.error:
            error_class, _, trace = pipeline_error_queue.get()

            try:
                error = error_class(f"\n\nOriginal {trace}")
            except:
                raise Exception(f"\n\nError: {trace}")

            raise error

    async def await_(self):
        return [x async for x in self.to_async_iterable(maxsize=0, return_index=False)]

    def __await__(self):
        return self.await_().__await__()

    def build(
        self,
        pipeline_stages: set,
        output_queue: utils.IterableQueue,
        pipeline_namespace,
        pipeline_error_queue,
        loop,
    ):

        if (
            self.pipeline_namespace is not None
            and self.pipeline_namespace != pipeline_namespace
        ):
            raise pypeln_utils.StageReuseError(
                f"Traying to reuse stage {self} in two different pipelines. This behavior is not supported."
            )

        self.output_queues.append(output_queue)

        if self in pipeline_stages:
            return

        pipeline_stages.add(self)

        total_done = len(self.dependencies)

        self.pipeline_namespace = pipeline_namespace
        self.pipeline_error_queue = pipeline_error_queue
        self.pipeline_stages = pipeline_stages
        self.loop = loop
        self.input_queue = utils.IterableQueue(
            self.maxsize, total_done, pipeline_namespace, loop=loop
        )

        for stage in self.dependencies:
            stage: Stage

            stage.build(
                pipeline_stages,
                self.input_queue,
                pipeline_namespace,
                pipeline_error_queue,
                loop,
            )

    def to_iterable(self, maxsize, return_index):

        pipeline_namespace = utils.get_namespace()
        pipeline_namespace.error = False
        pipeline_error_queue = Queue()

        f_coro, output_queue = self.to_coroutine(
            maxsize=maxsize,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
            loop=utils.LOOP,
        )

        utils.run_on_loop(f_coro)

        for elem in output_queue:
            if return_index:
                yield elem
            else:
                yield elem.value

        if pipeline_namespace.error:
            error_class, _, trace = pipeline_error_queue.get()

            try:
                error = error_class(f"\n\nOriginal {trace}")
            except:
                raise Exception(f"\n\nError: {trace}")

            raise error

    def to_coroutine(self, maxsize, pipeline_namespace, pipeline_error_queue, loop):

        total_done = 1
        output_queue = utils.IterableQueue(
            maxsize=maxsize,
            total_done=total_done,
            pipeline_namespace=pipeline_namespace,
            loop=loop,
        )
        pipeline_stages = set()

        self.build(
            pipeline_stages=pipeline_stages,
            output_queue=output_queue,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
            loop=loop,
        )

        async def f_coro():
            await asyncio.gather(*[stage.run() for stage in pipeline_stages])

        return f_coro, output_queue
