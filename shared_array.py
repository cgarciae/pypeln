from dataclasses import dataclass
from multiprocessing import Queue
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
import typing as tp
import uuid

import numpy as np


class ArrayInfo(tp.NamedTuple):
    index: int
    shape: tp.Tuple[int, ...]
    dtype: tp.Any
    nbytes: int
    name: str


class MapedArray(tp.NamedTuple):
    memory: tp.Optional[SharedMemory] = None
    array: tp.Optional[np.ndarray] = None

    @property
    def name(self) -> str:
        return self.memory.name

    @classmethod
    def create(
        cls, shape: tp.Tuple[int, ...], dtype: tp.Any, nbytes: int,
    ) -> "MapedArray":
        memory = SharedMemory(create=True, size=nbytes)
        array = np.ndarray(shape, dtype=dtype, buffer=memory.buf)

        return cls(memory=memory, array=array)

    @classmethod
    def from_info(cls, info: ArrayInfo) -> "MapedArray":
        memory = SharedMemory(name=info.name)
        array = np.ndarray(info.shape, info=info.dtype, buffer=memory.buf)

        return cls(memory=memory, array=array)

    def close(self):
        self.memory.close()
        self.memory.unlink()

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

