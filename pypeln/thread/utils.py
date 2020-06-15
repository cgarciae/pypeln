
import typing as tp



class _Namespace:
    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)


def Namespace(**kwargs) -> tp.Any:
    return _Namespace(**kwargs)
