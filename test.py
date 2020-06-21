import pypeln as pl
from copy import copy, deepcopy


def generator():
    yield from [1, 2, 3]


# stage = lambda: generator()
stage = [1, 2, 3]
stage = pl.process.map(lambda x: x + 1, stage)

# stage0 = deepcopy(stage)
print(list(stage))
print(list(stage))
print(pl.Element)
