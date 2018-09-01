from __future__ import absolute_import, print_function

import sys
from . import sync

if sys.version_info >= (3, 5):
    from . import async