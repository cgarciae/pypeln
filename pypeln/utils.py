import functools
import traceback
from collections import namedtuple


TIMEOUT = 0.0001


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


class Value(namedtuple("Value", "value")):
    pass


def is_value(x):
    return isinstance(x, Value)


class Continue(object):
    def __str__(self):
        return "Continue"


CONTINUE = Continue()


def is_continue(x):
    return isinstance(x, Continue)


class _None(object):
    def __str__(self):
        return "_None"


NONE = _None()


def is_none(x):
    return isinstance(x, _None)


class Undefined(object):
    def __str__(self):
        return "Undefined"

    def __repr__(self):
        return "Undefined"


UNDEFINED = Undefined()


def is_undefined(x):
    return isinstance(x, Undefined)


def chunks(n, l):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        if i + n <= len(l):
            yield l[i : i + n]


def print_error(f):
    @functools.wraps(f)
    def _lambda(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            raise e

    return _lambda


# try:
#     from wrapt import decorator as wrapt_decorator
# except ImportError:
#     def wrapt_decorator(f):

#         @functools.wraps(f)
#         def wrapper_f(g):

#             @functools.wraps(g)
#             def wrapper_g(*args, **kwargs):
#                 return f(g, None, args, kwargs)

#             return wrapper_g

#         return wrapper_f

# def maybe_partial(n):

#     @wrapt_decorator
#     def wrapper(wrapped_f, instance, args, kwargs):

#         if len(args) < n:
#             return Partial(lambda s: wrapped_f(*(args + (s,)), **kwargs))
#         else:
#             return wrapped_f(*args, **kwargs)

#     return wrapper
