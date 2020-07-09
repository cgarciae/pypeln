"""
The `sync` module follows same API as the rest of the modules but runs the code synchronously using regular python generators. Use functions from this module when you don't need to perform heavy CPU or IO tasks but still want to retain element order information that certain functions like `pl.*.ordered` rely on. 

Common arguments such as `workers` and `maxsize` are accepted by this module's functions for API compatibility purposes but are ignored.

Most functions in this module return a `pl.sync.Stage` which is a regular  `Iterable` interface which enables you to combine it seamlessly with regular Python code.

### Iterable
You can iterate over any `p.sync.Stage` to get back the results of its computation:

```python
import pypeln as pl
import time
from random import random

def slow_add1(x):
    time.sleep(random()) # <= some slow computation
    return x + 1

def slow_gt3(x):
    time.sleep(random()) # <= some slow computation
    return x > 3

data = range(10) # [0, 1, 2, ..., 9] 

stage = pl.sync.map(slow_add1, data, workers=3, maxsize=4)
stage = pl.sync.filter(slow_gt3, stage, workers=2)

for x in stage:
    print(x) # e.g. 5, 6, 9, 4, 8, 10, 7
```
"""

from .api.concat import concat
from .api.each import each
from .api.filter import filter
from .api.flat_map import flat_map
from .api.from_iterable import from_iterable
from .api.map import map
from .api.run import run
from .api.to_iterable import to_iterable
from .api.ordered import ordered
from .stage import Stage
from .utils import Namespace
