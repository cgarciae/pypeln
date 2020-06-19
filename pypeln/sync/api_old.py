"""
The `sync` module follows same API as the rest of the modules but runs the code synchronously using normal python generators. This module is intended to be used for debugging purposes as synchronous code tends to be easier to debug than concurrent code in Python (e.g. vscode's debugger doesn't work well (if at all) with the multiprocessing and threading modules).

Common arguments such as `workers` and `maxsize` are accepted by this module's 
functions for API compatibility purposes but are ignored.
"""
import typing as tp

from pypeln import utils as pypeln_utils

from .stage import Stage



#############################################################
# run
#############################################################


#############################################################
# to_iterable
#############################################################


