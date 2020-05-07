import functools
import traceback
from collections import namedtuple
import inspect
import typing as tp

try:
    from typing import Protocol
except ImportError:
    from typing_extensions import Protocol

TIMEOUT = 0.0001
MAXSIZE = 100


T = tp.TypeVar("T")


class BaseStage:
    def __or__(self, f):
        return f(self)


class Element(tp.NamedTuple, tp.Generic[T]):
    index: int
    value: T

    def set(self, value: T):
        return Element(self.index, value)


class StopThreadException(BaseException):
    def __str__(self):
        return "StopThreadException"


class StageReuseError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Partial(object):
    def __init__(self, f):
        self.f = f

    def __or__(self, stage):
        return self.f(stage)

    def __ror__(self, stage):
        return self.f(stage)

    def __call__(self, stage):
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
    return inspect.getfullargspec(f).args
