import functools
import traceback
from collections import namedtuple
import inspect


TIMEOUT = 0.0001
MAXSIZE = 100


WorkerInfo = namedtuple("WorkerInfo", ["index"])


class BaseStage:
    def __or__(self, f):
        return f(self)


class Element(namedtuple("Element", ["index", "value"])):
    def set(self, value):
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


def function_args(f):
    return inspect.getfullargspec(f).args
