from dataclasses import dataclass
from multiprocessing import Queue
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
import typing as tp
import uuid

import numpy as np


@dataclass
class SharedArray:
    shape: tp.Tuple[int, ...]
    dtype: tp.Any
    nbytes: int
    name: str
    memory: tp.Optional[SharedMemory] = None
    array: tp.Optional[np.ndarray] = None

    @classmethod
    def create(
        cls, shape: tp.Tuple[int, ...], dtype: tp.Any, nbytes: int,
    ) -> "SharedArray":
        memory = SharedMemory(create=True, size=nbytes)
        new_array = np.ndarray(shape, dtype=dtype, buffer=memory.buf)

        return cls(
            shape=shape,
            dtype=dtype,
            nbytes=nbytes,
            name=memory.name,
            memory=memory,
            array=new_array,
        )

    @classmethod
    def from_array(cls, array: np.ndarray) -> "SharedArray":
        memory = SharedMemory(create=True, size=array.nbytes)
        new_array = np.ndarray(array.shape, dtype=array.dtype, buffer=memory.buf)

        return cls(
            shape=array.shape,
            dtype=array.dtype,
            nbytes=array.nbytes,
            name=memory.name,
            memory=memory,
            array=new_array,
        )

    def load(self) -> "SharedArray":
        self.memory = SharedMemory(name=self.name)
        self.array = np.ndarray(self.shape, dtype=self.dtype, buffer=self.memory.buf)

        return self

    def clean(self) -> "SharedArray":
        self.array = None
        if self.memory:
            self.memory.close()
            self.memory = None

        return self

    def close(self):
        self.array = None
        if self.memory:
            self.memory.close()
            self.memory.unlink()
            self.memory = None

    def __getitem__(self, *arg, **kwargs):
        return self.array.__getitem__(*arg, **kwargs)

    def __setitem__(self, *arg, **kwargs):
        return self.array.__setitem__(*arg, **kwargs)


@dataclass
class SharedArray2:
    shape: tp.Tuple[int, ...]
    dtype: tp.Any
    nbytes: int
    name: str
    memory: tp.Optional[np.memmap] = None

    @classmethod
    def create(
        cls, shape: tp.Tuple[int, ...], dtype: tp.Any, nbytes: int,
    ) -> "SharedArray2":
        name = str(uuid.uuid4())

        return cls(shape=shape, dtype=dtype, nbytes=nbytes, name=name, memory=None,)

    @classmethod
    def from_array(cls, array: np.ndarray) -> "SharedArray2":
        name = str(uuid.uuid4())

        return cls(
            shape=array.shape,
            dtype=array.dtype,
            nbytes=array.nbytes,
            name=name,
            memory=None,
        )

    def load(self, mode: str) -> "SharedArray2":
        self.memory = np.memmap(
            self.name, dtype=self.dtype, mode=mode, shape=self.shape
        )

        return self

    def clean(self) -> "SharedArray2":
        if self.memory is not None:
            self.memory = None

        return self

    def close(self):
        Path(self.name).unlink()
        if self.memory is not None:
            self.memory = None

    def __getitem__(self, *arg, **kwargs):
        return self.memory.__getitem__(*arg, **kwargs)

    def __setitem__(self, *arg, **kwargs):
        return self.memory.__setitem__(*arg, **kwargs)

