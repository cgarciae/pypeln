from pypeln import asyncio_task as aio

list_acc = []

def batch(x, n):

    if len(list_acc) == n:
        list_out = list(list_acc)
        list_acc.clear()
        yield list_out
    else:
        list_acc.append(x)



print(
    range(100)
    | aio.from_iterable()
    | aio.flat_map(lambda x: batch(x, 10))
    | aio.map(sum)
    | list
)