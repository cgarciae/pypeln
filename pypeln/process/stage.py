from collections import namedtuple
import multiprocessing
import sys
import threading
import time
import traceback
import typing as tp
from dataclasses import dataclass
from multiprocessing import synchronize, Lock

from pypeln import utils as pypeln_utils

from . import utils
from .queue import IterableQueue, OutputQueues
from .worker import Worker, StageParams, WorkerConstructor
from .supervisor import Supervisor


T = tp.TypeVar("T")


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    stage_params: StageParams
    worker_constructor: WorkerConstructor
    workers: int
    total_sources: int
    dependencies: tp.List["Stage"]
    built: bool = False
    started: bool = False

    @classmethod
    def create(
        cls,
        workers: int,
        total_sources: int,
        maxsize: int,
        worker_constructor: WorkerConstructor,
        dependencies: tp.List["Stage"],
    ):

        input_queue: IterableQueue[T] = IterableQueue(
            maxsize=maxsize, total_sources=total_sources,
        )

        for dependency in dependencies:
            dependency.stage_params.output_queues.append(input_queue)

        return cls(
            stage_params=StageParams.create(
                input_queue=input_queue,
                output_queues=OutputQueues(),
                total_workers=workers,
            ),
            workers=workers,
            total_sources=total_sources,
            worker_constructor=worker_constructor,
            dependencies=dependencies,
        )

    def build(self) -> tp.Iterable["Stage"]:

        if self.started:
            raise pypeln_utils.StageReuseError()

        if self.built:
            return

        self.built = True

        yield self

        for dependency in self.dependencies:
            yield from dependency.build()

    def start(self, main_queue: IterableQueue) -> tp.Iterable[Worker]:

        self.started = True

        for i in range(self.workers):
            worker = self.worker_constructor(i, self.stage_params, main_queue)
            worker.start()

            yield worker

    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[T]:

        # build stages first to verify reuse
        stages = list(self.build())

        main_queue: IterableQueue[pypeln_utils.Element[T]] = IterableQueue(
            maxsize=maxsize, total_sources=self.workers,
        )

        # add main_queue before starting
        self.stage_params.output_queues.append(main_queue)

        workers: tp.List[Worker] = list(
            pypeln_utils.concat(stage.start(main_queue) for stage in stages)
        )
        supervisor = Supervisor(workers=workers, main_queue=main_queue)

        with supervisor:
            for elem in main_queue:
                if return_index:
                    yield elem
                else:
                    yield elem.value

    def __iter__(self):
        return self.to_iterable(maxsize=0, return_index=False)


# class Stage2(pypeln_utils.BaseStage):
#     def __init__(
#         self,
#         f,
#         workers,
#         maxsize,
#         on_start,
#         on_done,
#         dependencies,
#         timeout,
#         worker_constructor=None,
#     ):

#         if worker_constructor is None:
#             worker_constructor = utils.CONTEXT.Process

#         self.f = f
#         self.workers = workers
#         self.maxsize = maxsize
#         self.on_start = on_start
#         self.on_done = on_done
#         self.timeout = timeout
#         self.dependencies = dependencies
#         self.output_queues = utils.MultiQueue()
#         self.worker_constructor = worker_constructor
#         self.f_args = pypeln_utils.function_args(self.f) if self.f else set()
#         self.on_start_args = (
#             pypeln_utils.function_args(self.on_start) if self.on_start else set()
#         )
#         self.on_done_args = (
#             pypeln_utils.function_args(self.on_done) if self.on_done else set()
#         )
#         ######################################
#         # build fields
#         ######################################
#         self.input_queue = None
#         self.stage_namespace = None
#         self.stage_lock = None
#         self.pipeline_namespace = None
#         self.pipeline_error_queue = None

#     def __iter__(self):
#         return self.to_iterable(maxsize=0, return_index=False)

#     def build(
#         self,
#         pipeline_stages: set,
#         output_queue: utils.IterableQueue,
#         pipeline_namespace,
#         pipeline_error_queue,
#     ):

#         if (
#             self.pipeline_namespace is not None
#             and self.pipeline_namespace != pipeline_namespace
#         ):
#             raise pypeln_utils.StageReuseError(
#                 f"Traying to reuse stage {self} in two different pipelines. This behavior is not supported."
#             )

#         self.output_queues.append(output_queue)

#         if self in pipeline_stages:
#             return

#         pipeline_stages.add(self)

#         total_done = sum([stage.workers for stage in self.dependencies])

#         self.pipeline_namespace = pipeline_namespace
#         self.pipeline_error_queue = pipeline_error_queue
#         self.stage_lock = utils.CONTEXT.Lock()
#         self.stage_namespace = utils.get_namespace(active_workers=self.workers)
#         self.input_queue = utils.IterableQueue(
#             self.maxsize, total_done, pipeline_namespace
#         )

#         for stage in self.dependencies:
#             stage: Stage

#             stage.build(
#                 pipeline_stages,
#                 self.input_queue,
#                 pipeline_namespace,
#                 pipeline_error_queue,
#             )

#     def to_iterable(self, maxsize, return_index):

#         self._iter_done = False

#         pipeline_namespace = utils.get_namespace(error=False)
#         pipeline_error_queue = utils.CONTEXT.Queue()

#         output_queue = utils.IterableQueue(maxsize, self.workers, pipeline_namespace)
#         pipeline_stages = set()

#         self.build(
#             pipeline_stages=pipeline_stages,
#             output_queue=output_queue,
#             pipeline_namespace=pipeline_namespace,
#             pipeline_error_queue=pipeline_error_queue,
#         )

#         workers = []

#         for stage in pipeline_stages:
#             for index in range(stage.workers):
#                 worker_namespace = utils.get_namespace(task_start_time=None, done=False)

#                 workers.append(
#                     dict(
#                         process=stage.worker_constructor(
#                             target=stage.run, args=(index, worker_namespace)
#                         ),
#                         index=index,
#                         stage=stage,
#                         worker_namespace=worker_namespace,
#                     )
#                 )

#         for worker in workers:
#             worker["process"].daemon = True
#             worker["process"].start()

#         supervisor = threading.Thread(target=self.worker_supervisor, args=(workers,))
#         supervisor.daemon = True
#         supervisor.start()

#         try:
#             for elem in output_queue:
#                 if return_index:
#                     yield elem
#                 else:
#                     yield elem.value

#             if pipeline_namespace.error:
#                 error_class, _, trace = pipeline_error_queue.get()

#                 try:
#                     error = error_class(f"\n\nOriginal {trace}")
#                 except:
#                     raise Exception(f"\n\nError: {trace}")

#                 raise error

#             for worker in workers:
#                 worker["process"].join()

#         except:
#             for stage in pipeline_stages:
#                 stage.input_queue.done()

#             raise

#         finally:
#             self._iter_done = True

#     def worker_supervisor(self, workers):
#         try:
#             workers = [worker for worker in workers if worker["stage"].timeout > 0]

#             if not workers:
#                 return

#             while not self._iter_done:

#                 for worker in workers:
#                     if (
#                         not worker["worker_namespace"].done
#                         and worker["worker_namespace"].task_start_time is not None
#                     ):
#                         if (
#                             time.time() - worker["worker_namespace"].task_start_time
#                             > worker["stage"].timeout
#                         ):
#                             worker["worker_namespace"].task_start_time = None
#                             worker["process"].terminate()
#                             worker["process"] = worker["stage"].worker_constructor(
#                                 target=worker["stage"].run,
#                                 args=(worker["index"], worker["worker_namespace"]),
#                             )
#                             worker["process"].daemon = True
#                             worker["process"].start()

#                 time.sleep(0.05)

#         except BaseException as e:
#             self.signal_error(e)

#     def signal_error(self, e):
#         try:
#             self.pipeline_error_queue.put(
#                 (type(e), e, "".join(traceback.format_exception(*sys.exc_info())))
#             )
#             self.pipeline_namespace.error = True
#         except BaseException as e:
#             print(e)
