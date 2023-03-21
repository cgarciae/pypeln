import dataclasses
import inspect
import sys
import typing as tp
from abc import ABC, abstractmethod

if sys.version_info >= (3, 8):
    from typing import Protocol, runtime_checkable
else:
    from typing_extensions import Protocol, runtime_checkable

TIMEOUT = 0.0001


Kwargs = tp.Dict[str, tp.Any]
T = tp.TypeVar("T")
A = tp.TypeVar("A")
B = tp.TypeVar("B")


@dataclasses.dataclass
class PipelineException(BaseException):
    exception: tp.Optional[tp.Type[BaseException]]
    trace: str

    def __iter__(self):
        self.n = 0
        return self

    def __next__(self):
        # Workaround to make PipelineException act more like the
        # namedtuple it was designed to be
        try:
            item = {0: self.exception, 1: self.trace}[self.n]
        except KeyError as e:
            raise StopIteration from e

        self.n += 1
        return item


class NoLock(Exception):
    pass


class Element(tp.NamedTuple):
    index: tp.Tuple[int, ...]
    value: T

    def set(self, value: T):
        return Element(self.index, value)

    def __lt__(self, x: "Element") -> bool:
        return self.index < x.index

    def __le__(self, x: "Element") -> bool:
        return self.index <= x.index

    def __gt__(self, x: "Element") -> bool:
        return self.index > x.index

    def __ge__(self, x: "Element") -> bool:
        return self.index >= x.index

    def __eq__(self, x: "Element") -> bool:
        return self.value == x.index


class BaseStage(tp.Generic[T], tp.Iterable[T], ABC):
    @abstractmethod
    def to_iterable(self, maxsize: int, return_index: bool) -> tp.Iterable[Element]:
        pass

    def __or__(self, f):
        return f(self)


class StopThreadException(BaseException):
    pass


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


class _Namespace(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class Done(object):
    pass


DONE = Done()


class Continue(object):
    pass


CONTINUE = Continue()


class Undefined(object):
    pass


UNDEFINED = Undefined()


def function_args(f) -> tp.List[str]:
    return list(inspect.signature(f).parameters.keys())


def get_callable(f, *args, **kwargs) -> tp.Callable:
    return lambda: f(*args, **kwargs)
