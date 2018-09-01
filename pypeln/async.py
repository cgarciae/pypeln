from __future__ import absolute_import, print_function

from collections import namedtuple
import asyncio
import sys
from .utils import DONE


class Stream(namedtuple("Stream", ["coroutine", "queue"])):

    def __await__(self):
        return self.coroutine.__await__()


class TaskPool(object):

    def __init__(self, limit = 0):
        self._limit = limit
        self._tasks = []

    def filter_tasks(self):
        self._tasks = [ task for task in self._tasks if not task.done() ]

    async def join(self):
        self.filter_tasks()

        if len(self._tasks) > 0:
            await asyncio.wait(self._tasks)

        self.filter_tasks()
        

    async def put(self, coro):
        self.filter_tasks()

        # wait until space is available
        while self._limit > 0 and len(self._tasks) >= self._limit:
            await asyncio.sleep(0)

            self.filter_tasks()

        
        task = asyncio.ensure_future(coro)
        self._tasks.append(task)


    async def __aenter__(self):
        return self

    def __aexit__(self, exc_type, exc, tb):
        return self.join()
    





def filter_wrapper(f, queue):
    async def _filter_wrapper(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        if bool(y):
            await queue.put(x)
    
    return _filter_wrapper

def map_wrapper(f, queue):
    async def _map_wrapper(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y

        await queue.put(y)
    
    return _map_wrapper

def each_wrapper(f):
    async def _each_wrapper(x):

        y = f(x)

        if hasattr(y, "__await__"):
            y = await y
    
    return _each_wrapper
        
        
def map(f, stream, limit = 0, queue_maxsize = 0):

    qout = asyncio.Queue(maxsize = queue_maxsize)
    
    async def _map():
        coroin = stream.coroutine
        qin = stream.queue
        
        coroin_task = asyncio.ensure_future(coroin)
        f_wrapped = map_wrapper(f, queue = qout)

        async with TaskPool(limit = limit) as tasks:

            x = await qin.get()
            while x is not DONE:

                fcoro = f_wrapped(x)
                await tasks.put(fcoro)

                x = await qin.get()

        # end async with: wait all tasks to finish
        await qout.put(DONE)
        await coroin_task

    return Stream(_map(), qout)


def filter(f, stream, limit = 0, queue_maxsize = 0):
    
    async def _filter(f, qout):
        coroin = stream.coroutine
        qin = stream.queue
        coroin_task = asyncio.ensure_future(coroin)
        f = filter_wrapper(f, qout)

        async with TaskPool(limit = limit) as tasks:

            x = await qin.get()
            
            while x is not DONE:
                
                fcoro = f(x)
                await tasks.put(fcoro)

                x = await qin.get()

        # end async with: wait all tasks to finish

        await qout.put(DONE)
        await coroin_task

    qout = asyncio.Queue(maxsize = queue_maxsize)
        
    return Stream(_filter(f, qout), qout)


def from_iterable(iterable, queue_maxsize = 0):
    
    async def _from_iterable(qout):
        
        for x in iterable:
            await qout.put(x)
            
        await qout.put(DONE)
    
    qout = asyncio.Queue(maxsize=queue_maxsize)

    return Stream(_from_iterable(qout), qout)

async def each(f, stream, limit = 0):
    

    coroin = stream.coroutine
    qin = stream.queue
    
    coroin_task = asyncio.ensure_future(coroin)
    f = each_wrapper(f)

    async with TaskPool(limit = limit) as tasks:
        
        x = await qin.get()
        while x is not DONE:
            
            fcoro = f(x)
            await tasks.put(fcoro)

            x = await qin.get()

    await coroin_task


def run(stream):
    return stream.coroutine



if __name__ == '__main__':
    import os
    import aiohttp
    import aiofiles


    def download_file(path):
        async def do_download_file(url):
            
            filename = os.path.basename(url)
            filepath = os.path.join(path, filename)

            async with aiohttp.request("GET", url) as resp:
                context = await resp.read()

            async with aiofiles.open(filepath, "wb") as f:
                await f.write(context)
        
        return do_download_file

    def handle_async_exception(loop, ctx):
        loop.stop()
        raise Exception(f"Exception in async task: {ctx}")

    urls = [
        "https://static.pexels.com/photos/67843/splashing-splash-aqua-water-67843.jpeg",
        "https://cdn.pixabay.com/photo/2016/10/27/22/53/heart-1776746_960_720.jpg",
        "http://www.qygjxz.com/data/out/240/4321276-wallpaper-images-download.jpg"
    ] * 1000000

    path = "/data/tmp/images"

    stream = from_iterable(urls)
    stream = map(download_file(path), stream, limit = 2)

    loop = asyncio.get_event_loop()
    loop.set_exception_handler(handle_async_exception)
    loop.run_until_complete(stream)