import functools
import traceback
from collections import namedtuple

TIMEOUT = 0.0001

class Namespace(object):
    pass

class Done(object): pass
DONE = Done()
def is_done(x): return isinstance(x, Done)

class Value(namedtuple("Value", "value")): pass
def is_value(x): return isinstance(x, Value)

class Continue(object): pass
CONTINUE = Continue()
def is_continue(x): return isinstance(x, Continue)


def chunks(n, l):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        if i + n <= len(l):
            yield l[i:i + n]


def print_error(f):

    @functools.wraps(f)
    def _lambda(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            print(traceback.format_exc())
            raise e

    return _lambda
