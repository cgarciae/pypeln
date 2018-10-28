from pypeln import asyncio_task as aio

def batch(x, list_acc, n):

    if len(list_acc) == n:
        list_out = list(list_acc)
        list_acc.clear()
        yield list_out
    else:
        list_acc.append(x)



print(
    range(100)
    | aio.map(lambda x: x)
    | aio.flat_map(lambda x, list_acc: batch(x, list_acc, 10), on_start=lambda: [])
    | aio.map(sum)
    | list
)