import pypeln as pl


def batch(x, list_acc, n):

    if len(list_acc) == n:
        list_out = list(list_acc)
        list_acc.clear()
        yield list_out
    else:
        list_acc.append(x)


print(
    range(100)
    | pl.task.map(lambda x: x)
    | pl.task.flat_map(lambda x, list_acc: batch(x, list_acc, 10), on_start=lambda: [])
    | pl.task.map(sum)
    | list
)
