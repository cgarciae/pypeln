import inspect
import sys
import traceback
from collections import namedtuple

from pypeln import utils as pypeln_utils

from . import utils


class Stage:
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

        if worker_constructor is None:
            worker_constructor = utils.CONTEXT.Process

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

    def process(self, **kwargs) -> None:
        for x in self.input_queue:
            self.apply(x, **kwargs)

    def run(self, index):
        try:
            if self.on_start is not None:
                on_start_kwargs = {}

                if "worker_info" in inspect.getfullargspec(self.on_start).args:
                    on_start_kwargs["worker_info"] = utils.WorkerInfo(index=index)

                kwargs = self.on_start(**on_start_kwargs)
            else:
                kwargs = {}

            if kwargs is None:
                kwargs = {}

            self.process(**kwargs)

            if self.on_done is not None:
                with self.stage_lock:
                    self.stage_namespace.active_workers -= 1

                if "stage_status" in inspect.getfullargspec(self.on_done).args:
                    kwargs["stage_status"] = utils.StageStatus(
                        namespace=self.stage_namespace, lock=self.stage_lock
                    )

                self.on_done(**kwargs)

            self.output_queues.done()

        except BaseException as e:
            try:
                self.pipeline_error_queue.put(
                    (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
                )
                self.pipeline_namespace.error = True
            except BaseException as e:
                print(e)

    def __iter__(self):
        return self.to_iterable(maxsize=0)

    def build(
        self,
        pipeline_stages: set,
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

        if self in pipeline_stages:
            return

        pipeline_stages.add(self)

        total_done = sum([stage.workers for stage in self.dependencies])

        self.pipeline_namespace = pipeline_namespace
        self.pipeline_error_queue = pipeline_error_queue
        self.stage_lock = utils.CONTEXT.Lock()
        self.stage_namespace = utils.get_namespace()
        self.stage_namespace.active_workers = self.workers
        self.input_queue = utils.IterableQueue(
            self.maxsize, total_done, pipeline_namespace
        )

        for stage in self.dependencies:
            stage: Stage

            stage.build(
                pipeline_stages,
                self.input_queue,
                pipeline_namespace,
                pipeline_error_queue,
            )

    def to_iterable(self, maxsize):

        pipeline_namespace = utils.get_namespace()
        pipeline_namespace.error = False
        pipeline_error_queue = utils.CONTEXT.Queue()

        output_queue = utils.IterableQueue(maxsize, self.workers, pipeline_namespace)
        pipeline_stages = set()

        self.build(
            pipeline_stages=pipeline_stages,
            output_queue=output_queue,
            pipeline_namespace=pipeline_namespace,
            pipeline_error_queue=pipeline_error_queue,
        )

        workers = []
        for stage in pipeline_stages:
            for index in range(stage.workers):
                workers.append(
                    stage.worker_constructor(target=stage.run, args=(index,))
                )

        for p in workers:
            p.daemon = True
            p.start()

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

            for p in workers:
                p.join()

        except:
            for stage in pipeline_stages:
                stage.input_queue.done()

            raise

    def __or__(self, f):
        return f(self)
