import importlib

def singleton(cls):
    instances = {}
    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance

@singleton
class config:
    _impl = None
    _impl_queues = None
    
    def get_multiprocessing_impl(self):
        if self._impl is None:
            self._impl = importlib.import_module("multiprocessing")
            self._impl_queues = importlib.import_module("multiprocessing.queues")

        return self._impl, self._impl_queues

    def set_multiprocessing_impl(self, some_module, queues_module):
        self._impl = some_module
        self._impl_queues = queues_module