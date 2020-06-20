"""
The `sync` module follows same API as the rest of the modules but runs the code synchronously using normal python generators. This module is intended to be used for debugging purposes as synchronous code tends to be easier to debug than concurrent code in Python (e.g. vscode's debugger doesn't work well (if at all) with the multiprocessing and threading modules).

Common arguments such as `workers` and `maxsize` are accepted by this module's 
functions for API compatibility purposes but are ignored.
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
from .utils import get_namespace
