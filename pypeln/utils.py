
TIMEOUT = 0.0001

class Done(object):
    pass

DONE = Done()

def is_done(x):
    return isinstance(x, Done)