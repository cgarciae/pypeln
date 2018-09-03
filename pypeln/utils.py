
TIMEOUT = 0.0001

class Namespace(object):
    pass

class Done(object):
    pass

DONE = Done()

def is_done(x):
    return isinstance(x, Done)