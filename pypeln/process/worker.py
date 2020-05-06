import multiprocessing as mp
import typing as tp

# ----------------------------------------------------------------
# create_daemon_workers
# ----------------------------------------------------------------
def start_workers(
    target: tp.Callable,
    n_workers: int = 1,
    args: tp.Tuple[tp.Any] = tuple(),
    kwargs: tp.Optional[tp.Dict[tp.Any, tp.Any]] = None,
) -> tp.List[mp.Process]:
    if kwargs is None:
        kwargs = {}

    workers: tp.List[mp.Process] = []

    for _ in range(n_workers):
        t = mp.Process(target=target, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        workers.append(t)

    return workers
