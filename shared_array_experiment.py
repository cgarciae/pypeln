import pypeln as pl
import numpy as np
import time
from tqdm import tqdm
import typing as tp

N = 100
shape = (2000, 2000, 3)


def create_array():
    return np.random.uniform(size=shape).astype(np.float32)


def shared_to_array(shared):
    return np.ndarray(shape, dtype=np.float32, buffer=shared.buf)


# --------------------------------------------------------------
# base
# --------------------------------------------------------------


def routine():
    def get_data(i):
        return create_array()

    stage = range(N)
    stage = map(get_data, stage)

    t0 = time.time()

    for _ in tqdm(stage, total=N, desc="base"):
        pass
    print(f"base: {time.time() - t0}")
    # print(np.stack(arrays).mean())


routine()

# --------------------------------------------------------------
# process
# --------------------------------------------------------------


def routine():
    def get_data(i):
        return create_array()

    stage = range(N)
    stage = pl.process.map(get_data, stage)

    t0 = time.time()

    for _ in tqdm(stage, total=N, desc="process"):
        pass

    print(f"process: {time.time() - t0}")
    # print(np.stack(arrays).mean())


routine()


# # ----------------------------------------------------------------
# # shared
# # ----------------------------------------------------------------
# from multiprocessing.shared_memory import SharedMemory
# from multiprocessing import Queue
# from shared_array import SharedArray


# def routine():
#     x = create_array()
#     free = pl.process.IterableQueue()

#     for _ in range(10):
#         free.put(SharedArray.from_array(x).clean())

#     queue = pl.process.IterableQueue()

#     def get_data():
#         try:

#             for i in range(N):
#                 shared_array = free.get().load()
#                 # print(shared_array[0, 0])

#                 x = create_array()
#                 # print(x.shape, x.dtype)

#                 # print(x[0, 0])
#                 shared_array[:] = x[:]

#                 # print(shared_array[0, 0])

#                 queue.put(shared_array.clean())

#             queue.done()
#         except BaseException as e:
#             queue.raise_exception(e)

#     t0 = time.time()
#     pl.process.start_workers(get_data)

#     def get_array_and_free(shared_array: SharedArray):

#         shared_array.load()
#         a = shared_array.array.copy()

#         free.put(shared_array.clean())

#         return a

#     stage = map(get_array_and_free, queue)
#     for _ in tqdm(stage, total=N, desc="shared"):
#         pass

#     print(f"shared: {time.time() - t0}")

#     free.done()

#     print("closing")
#     for shared_array in free:
#         shared_array.close()


# routine()


# ----------------------------------------------------------------
# mmap single
# ----------------------------------------------------------------
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Queue
from shared_array import SharedArray2


def routine():
    x = create_array()
    free = pl.process.IterableQueue()
    shared = SharedArray2.create(shape=(10,) + x.shape, dtype=x.dtype)
    shared.load("w+")
    shared.clean()

    for i in range(10):
        free.put(i)

    queue = pl.process.IterableQueue()

    def get_data():
        try:
            arr = shared.load("c")

            for i in range(N):
                i = free.get()
                # print(shared_array[0, 0])

                x = create_array()
                # print(x.shape, x.dtype)

                # print(x[0, 0])
                arr[i][:] = x[:]

                # print(shared_array[0, 0])

                queue.put(i)

            queue.done()
        except BaseException as e:
            queue.raise_exception(e)

    t0 = time.time()
    pl.process.start_workers(get_data)

    shared.load("c")

    def get_array_and_free(i: int):

        a = shared[i].copy()

        free.put(i)

        return a

    stage = map(get_array_and_free, queue)
    for _ in tqdm(stage, total=N, desc="mmap single"):
        pass

    print(f"mmap single: {time.time() - t0}")

    free.done()

    print("closing")
    shared.close()


routine()

# ----------------------------------------------------------------
# single
# ----------------------------------------------------------------
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Queue
from shared_array import MapedArray, ArrayInfo


def routine():
    x = create_array()
    free = pl.process.IterableQueue()
    shared = MapedArray.create(
        shape=(10,) + x.shape, dtype=x.dtype, nbytes=x.nbytes * 10
    )

    for i in range(10):
        free.put(i)

    queue = pl.process.IterableQueue()

    def get_data():
        try:
            arr = shared.load()
            for i in range(N):
                i = free.get()
                # print(shared_array[0, 0])

                x = create_array()
                # print(x.shape, x.dtype)

                # print(x[0, 0])
                arr[i][:] = x[:]

                # print(shared_array[0, 0])

                queue.put(i)

            queue.done()
        except BaseException as e:
            queue.raise_exception(e)

    t0 = time.time()
    pl.process.start_workers(get_data)

    # shared.load()

    def get_array_and_free(i: int):

        a = shared[i].copy()

        free.put(i)

        return a

    stage = map(get_array_and_free, queue)
    for _ in tqdm(stage, total=N, desc="single"):
        pass

    print(f"single: {time.time() - t0}")

    free.done()

    print("closing")
    shared.close()


routine()
# # ----------------------------------------------------------------
# # memmap
# # ----------------------------------------------------------------
# print("starting memmap")
# from multiprocessing.shared_memory import SharedMemory
# from multiprocessing import Queue
# from shared_array import SharedArray2


# def routine():
#     x = create_array()
#     free = pl.process.IterableQueue()

#     for _ in range(10):
#         free.put(SharedArray2.from_array(x).clean())

#     queue = pl.process.IterableQueue()

#     def get_data():
#         try:

#             for i in range(N):
#                 shared_array: SharedArray2 = free.get()
#                 shared_array.load("w+")
#                 # print(shared_array[0, 0])

#                 x = create_array()
#                 # print(x.shape, x.dtype)

#                 # print(x[0, 0])
#                 shared_array[:] = x[:]

#                 # print(shared_array[0, 0])

#                 queue.put(shared_array.clean())

#             queue.done()
#         except BaseException as e:
#             queue.raise_exception(e)

#     t0 = time.time()
#     pl.process.start_workers(get_data)

#     def get_array_and_free(shared_array: SharedArray2):

#         shared_array.load("r")
#         a = np.asarray(shared_array.memory).copy()

#         free.put(shared_array)

#         return a

#     stage = map(get_array_and_free, queue)
#     for _ in tqdm(stage, total=N, desc="memmap"):
#         pass

#     print(f"memmap: {time.time() - t0}")
#     # print(np.stack(arrays).mean())

#     free.done()

#     for shared_array in free:
#         shared_array.close()


# routine()

# # ----------------------------------------------------------------
# # Array
# # ----------------------------------------------------------------
# from multiprocessing import Array
# from multiprocessing.sharedctypes import RawArray
# from pypeln.process.utils import Namespace
# import ctypes


# x_ = Array(ctypes.c_float, int(np.prod(shape)))
# queue = pl.process.IterableQueue()

# print(np.frombuffer(x_.get_obj()).mean())


# def get_data(x_):
#     try:
#         x_1 = x_.get_obj()
#         for i in range(N):
#             x = create_array()

#             x_1[:] = x.ravel()

#             if i == N - 1:
#                 queue.put(x)
#             else:
#                 queue.put(i)

#         queue.done()
#     except BaseException as e:
#         queue.raise_exception(e)


# pl.process.start_workers(get_data, kwargs=dict(x_=x_))
# t0 = time.time()

# arrays = list(tqdm(queue, total=N, desc="Array"))
# print(f"Array: {time.time() - t0}")
# print(np.frombuffer(x_.get_obj(), dtype=np.float32).mean())
# print(arrays[-1].mean())

