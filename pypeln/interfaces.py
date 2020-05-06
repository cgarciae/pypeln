try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

import typing as tp
import abc


T = tp.TypeVar("T")


class IterableQueue(tp.Generic[T], tp.Iterable[T], abc.ABC):
    @abc.abstractmethod
    def start(self) -> tp.Set["Worker"]:
        ...

    @abc.abstractmethod
    def stop(self):
        ...

    @abc.abstractmethod
    def raise_exception(self, exception: BaseException):
        ...


class AsyncIterableQueue(
    IterableQueue[T], tp.AsyncIterable[T], tp.Awaitable[tp.List[T]]
):
    pass


class Stage(tp.Generic[T], tp.Iterable[T], abc.ABC):
    @abc.abstractmethod
    def start(self) -> tp.Set["Worker"]:
        ...


class Worker(abc.ABC):
    @abc.abstractmethod
    def start(self):
        ...

    @abc.abstractmethod
    def stop(self):
        ...
