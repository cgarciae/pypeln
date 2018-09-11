
TIMEOUT = 0.0001

class Namespace(object):
    pass

class Done(object):
    pass

DONE = Done()

def is_done(x):
    return isinstance(x, Done)


def chunks(n, l):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        if i + n <= len(l):
            yield l[i:i + n]