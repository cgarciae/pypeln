from dataclasses import dataclass
import threading
import time
import typing as tp

from pypeln import utils as pypeln_utils

from .worker import Worker
from .queue import IterableQueue
import asyncio


@dataclass
class Supervisor:
    workers: tp.List[Worker]
    main_queue: IterableQueue
    done: bool = False

    async def stop(self):
        self.done = True

        for worker in self.workers:
            worker.stop()

        while any(not worker.is_done for worker in self.workers):
            await asyncio.sleep(pypeln_utils.TIMEOUT)

    def stop_nowait(self):
        self.done = True

        for worker in self.workers:
            worker.stop()

        while any(not worker.is_done for worker in self.workers):
            # for worker in self.workers:
            #     worker.stop()
            time.sleep(pypeln_utils.TIMEOUT)

    def start(self):
        for worker in self.workers:
            worker.start()

    def __enter__(self):
        self.start()

    def __exit__(self, *args):
        self.stop_nowait()

    async def __aenter__(self):
        self.start()

    async def __aexit__(self, *args):
        await self.stop()
