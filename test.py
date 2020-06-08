import time


class Dummy:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        print("EXIT")


def generator():
    with Dummy():
        yield 1


g = generator()
i = iter(g)
x = next(i)

print("BEFORE DEL")
del i
del g
print("AFTER DEL")

time.sleep(10)
