import inspect
import typing as tp
from abc import ABC, abstractmethod

from typing import Protocol

TIMEOUT = 0.0001
MAXSIZE = 100


Kwargs = tp.Dict[str, tp.Any]
T = tp.TypeVar("T")
A = tp.TypeVar("A")
B = tp.TypeVar("B")


class Element(tp.NamedTuple):
    index: tp.Tuple[int, ...]
    value: T

    def set(self, value: T):
        return Element(self.index, value)


class BaseStage(tp.Generic[T], tp.Iterable[T], ABC):
    @abstractmethod
    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[Element]:
        pass

    def __or__(self, f):
        return f(self)


class StopThreadException(BaseException):
    def __str__(self):
        return "StopThreadException"


class StageReuseError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Partial(tp.Generic[T]):
    def __init__(self, f):
        self.f = f

    def __or__(self, stage) -> T:
        return self.f(stage)

    def __ror__(self, stage) -> T:
        return self.f(stage)

    def __call__(self, stage) -> T:
        return self.f(stage)


class Namespace(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Done(object):
    def __str__(self):
        return "DONE"


DONE = Done()


def is_done(x):
    return isinstance(x, Done)


class Continue(object):
    def __str__(self):
        return "Continue"


CONTINUE = Continue()


def is_continue(x):
    return isinstance(x, Continue)


class Undefined(object):
    def __str__(self):
        return "Undefined"

    def __repr__(self):
        return "Undefined"


UNDEFINED = Undefined()


def is_undefined(x):
    return isinstance(x, Undefined)


def function_args(f) -> tp.List[str]:
    return list(inspect.signature(f).parameters.keys())


def concat(iterables: tp.Iterable[tp.Iterable[T]]) -> tp.Iterable[T]:
    for iterable in iterables:
        for item in iterable:
            yield item


def no_op():
    pass


def get_callable(f, *args, **kwargs) -> tp.Callable:
    return lambda: f(*args, **kwargs)
