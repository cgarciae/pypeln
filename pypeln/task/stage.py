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
        self,
        f,
        workers,
        maxsize,
        on_start,
        on_done,
        dependencies,
        worker_constructor=None,
    ):
        assert hasattr(self, "apply") != hasattr(
            self, "process"
        ), f"{self.__class__} must define either 'apply' or 'process'"

        def worker_constructor(f, args):
            return f(*args)

        self.f = f
        self.workers = workers
        self.maxsize = maxsize
        self.on_start = on_start
        self.on_done = on_done
        self.dependencies = dependencies
        self.output_queues = utils.MultiQueue()
        self.worker_constructor = worker_constructor
        ######################################
        # build fields
        ######################################
        self.input_queue = None
        self.namespace = None
        self.stage_namespace = None
        self.stage_lock = None
        self.pipeline_namespace = None
        self.pipeline_error_queue = None

    async def run(self, index):
        try:

            if self.on_start is not None:
                on_start_kwargs = {}

                if "worker_info" in inspect.getfullargspec(self.on_start).args:
                    on_start_kwargs["worker_info"] = utils.WorkerInfo(index=index)

                f_kwargs = self.on_start(**on_start_kwargs)
            else:
                f_kwargs = {}

            if f_kwargs is None:
                f_kwargs = {}

            if hasattr(f_kwargs, "__await__"):
                f_kwargs = await f_kwargs

            if hasattr(self, "apply"):
                async with utils.TaskPool(self.workers, self.loop) as tasks:

                    async for x in self.input_queue:
                        task = self.apply(x, **f_kwargs)
                        await tasks.put(task)
            else:
                await self.process(**f_kwargs)

            await self.output_queues.done()

            if self.on_done is not None:

                on_done_kwargs = {}

                if "stage_status" in inspect.getfullargspec(self.on_done).args:
                    on_done_kwargs["stage_status"] = utils.StageStatus(
                        namespace=self.stage_namespace, lock=self.stage_lock
                    )

                done_resp = self.on_done(**on_done_kwargs)

                if hasattr(done_resp, "__await__"):
                    await done_resp

        except BaseException as e:
            try:
                self.pipeline_error_queue.put_nowait(
                    (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
                )
                self.pipeline_namespace.error = True
            except BaseException as e:
                print(e)

    def __iter__(self):
        return self.to_iterable(maxsize=0)

    def build(
        self,
        built_stages: set,
        output_queue: utils.IterableQueue,
        pipeline_namespace,
        pipeline_error_queue,
    ):

        if (
            self.pipeline_namespace is not None
            and self.pipeline_namespace != pipeline_namespace
        ):
            raise utils.StageReuseError(
                f"Traying to reuse stage {self} in two different pipelines. This behavior is not supported."
            )

        self.output_queues.append(output_queue)

        if self in built_stages:
            return

        built_stages.add(self)

        total_done = len(self.dependencies)

        self.pipeline_namespace = pipeline_namespace
        self.pipeline_error_queue = pipeline_error_queue
        self.input_queue = utils.IterableQueue(
            self.maxsize, total_done, pipeline_namespace
        )

        for stage in self.dependencies:
            stage: Stage

            stage.build(
                built_stages, self.input_queue, pipeline_namespace, pipeline_error_queue
            )

    def run_coroutine(self, coro, loop):

        if loop.is_running():
            loop.create_task(coro)
        else:
            loop.run_until_complete(coro)

    def to_iterable(self, maxsize):

        loop = asyncio.get_event_loop()

        pipeline_namespace = utils.get_namespace()
        pipeline_namespace.error = False
        pipeline_error_queue = Queue()

        output_queue = utils.IterableQueue(maxsize, self.workers, pipeline_namespace)
        built_stages = set()

        coro, output_queue = self.to_coroutine(
            maxsize, pipeline_namespace, pipeline_error_queue
        )

        thread = Thread(target=self.run_coroutine, args=(coro, loop))
        thread.daemon = True
        thread.start()

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

            thread.join()

        except:
            for stage in built_stages:
                stage.input_queue.done()

            raise

    def to_coroutine(self, maxsize, pipeline_namespace, pipeline_error_queue):

        total_done = 1
        output_queue = utils.IterableQueue(maxsize, total_done, pipeline_namespace)

        built_stages = set()

        self.build(
            built_stages=built_stages,
            output_queue=output_queue,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
        )

        workers = []
        for stage in built_stages:
            for index in range(stage.workers):
                workers.append(stage.worker_constructor(stage.run, args=(index,)))

        task = asyncio.gather(*workers)

        return task, output_queue
