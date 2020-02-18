import asyncio
import inspect
import sys
import traceback
from collections import namedtuple
from queue import Queue
from threading import Lock, Thread

from pypeln import utils as pypeln_utils

from . import utils


class Stage(pypeln_utils.BaseStage):
    def __init__(
        self, f, workers, maxsize, on_start, on_done, dependencies,
    ):
        assert hasattr(self, "apply") != hasattr(
            self, "process"
        ), f"{self.__class__} must define either 'apply' or 'process'"

        self.f = f
        self.workers = workers
        self.maxsize = maxsize
        self.on_start = on_start
        self.on_done = on_done
        self.dependencies = dependencies
        self.output_queues = utils.MultiQueue()
        ######################################
        # build fields
        ######################################
        self.input_queue = None
        self.namespace = None
        self.stage_namespace = None
        self.stage_lock = None
        self.pipeline_namespace = None
        self.pipeline_error_queue = None
        self.pipeline_stages = None
        self.loop = None

    async def run(self):
        try:
            if self.on_start is not None:
                on_start_kwargs = {}

                if "worker_info" in inspect.getfullargspec(self.on_start).args:
                    on_start_kwargs["worker_info"] = utils.WorkerInfo(index=0)

                f_kwargs = self.on_start(**on_start_kwargs)
            else:
                f_kwargs = {}

            if f_kwargs is None:
                f_kwargs = {}

            if hasattr(f_kwargs, "__await__"):
                f_kwargs = await f_kwargs

            if hasattr(self, "apply"):
                async with utils.TaskPool(self.workers) as tasks:

                    async for x in self.input_queue:
                        task = self.apply(x, **f_kwargs)
                        await tasks.put(task)
            else:
                await self.process(**f_kwargs)

            await self.output_queues.done()

            if self.on_done is not None:

                on_done_kwargs = {}

                if "stage_status" in inspect.getfullargspec(self.on_done).args:
                    on_done_kwargs["stage_status"] = utils.StageStatus()

                done_resp = self.on_done(**on_done_kwargs)

                if hasattr(done_resp, "__await__"):
                    await done_resp

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
        return self.to_iterable(maxsize=0)

    async def await_(self):

        maxsize = 0
        pipeline_namespace = utils.get_namespace()
        pipeline_namespace.error = False
        pipeline_error_queue = Queue()

        f_coro, _input_queue = self.to_coroutine(
            maxsize=maxsize,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
            loop=asyncio.get_event_loop(),
        )

        output = await f_coro()

        if pipeline_namespace.error:
            error_class, _, trace = pipeline_error_queue.get()

            try:
                error = error_class(f"\n\nOriginal {trace}")
            except:
                raise Exception(f"\n\nError: {trace}")

            raise error

        return output

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
            raise utils.StageReuseError(
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

    def to_iterable(self, maxsize):

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

        try:
            for x in output_queue:
                yield x

            if pipeline_namespace.error:
                error_class, _, trace = pipeline_error_queue.get()

                try:
                    error = error_class(f"\n\nOriginal {trace}")
                except:
                    raise Exception(f"\n\nError: {trace}")

                raise error

        except:

            raise

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
            return await asyncio.gather(*[stage.run() for stage in pipeline_stages])

        return f_coro, output_queue
