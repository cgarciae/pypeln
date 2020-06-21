import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A
from .to_stage import to_stage

from ..stage import Stage, ApplyProcess
from dataclasses import dataclass


class EachFn(tp.Protocol):
    def __call__(self, elem: A, **kwargs):
        ...


@dataclass
class Each(ApplyProcess):
    f: EachFn

    def apply(self, worker: Stage, elem: tp.Any, **kwargs) -> tp.Iterable:

        if "element_index" in worker.f_args:
            kwargs["element_index"] = elem.index

        self.f(elem.value, **kwargs)

        # empty iterable
        return ()


@tp.overload
def each(
    f: EachFn,
    stage: tp.Union[Stage[A], tp.Iterable[A]],
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
    run: bool = False,
) -> pypeln_utils.Partial[tp.Optional[Stage[None]]]:
    ...


@tp.overload
def each(
    f: EachFn,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
    run: bool = False,
) -> pypeln_utils.Partial[tp.Optional[Stage[None]]]:
    ...


def each(
    f: EachFn,
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    workers: int = 1,
    maxsize: int = 0,
    timeout: float = 0,
    on_start: tp.Callable = None,
    on_done: tp.Callable = None,
    run: bool = False,
) -> tp.Union[tp.Optional[Stage[None]], pypeln_utils.Partial[tp.Optional[Stage[None]]]]:
    """
    Creates a stage that runs the function `f` for each element in the data but the stage itself yields no elements. Its useful for sink stages that perform certain actions such as writting to disk, saving to a database, etc, and dont produce any results. For example:

    ```python
    import pypeln as pl

    def process_image(image_path):
        image = load_image(image_path)
        image = transform_image(image)
        save_image(image_path, image)

    files_paths = get_file_paths()
    stage = pl.sync.each(process_image, file_paths, workers=4)
    pl.sync.run(stage)

    ```
    or alternatively

    ```python
    files_paths = get_file_paths()
    pl.sync.each(process_image, file_paths, workers=4, run=True)
    ```

    !!! note
        Because of concurrency order is not guaranteed.

    Arguments:
        f: A function with signature `f(x) -> None`. `f` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        stage: A Stage or Iterable.
        workers: This parameter is not used and only kept for API compatibility with the other modules.
        maxsize: This parameter is not used and only kept for API compatibility with the other modules.
        timeout: Seconds before stoping the worker if its current task is not yet completed. Defaults to `0` which means its unbounded. 
        on_start: A function with signature `on_start(worker_info?) -> kwargs?`, where `kwargs` can be a `dict` of keyword arguments that can be consumed by `f` and `on_done`. `on_start` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        on_done: A function with signature `on_done(stage_status?)`. This function is executed once per worker when the worker finishes. `on_done` can accept additional arguments by name as described in [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
        run: Whether or not to execute the stage immediately.

    !!! warning
        To implement `timeout` we use `stopit.ThreadingTimeout` which has some limitations. 

    Returns:
        If the `stage` parameters is not given then this function returns a `Partial`, else if `run=False` (default) it return a new stage, if `run=True` then it runs the stage and returns `None`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: each(
                f,
                stage=stage,
                workers=workers,
                maxsize=maxsize,
                timeout=timeout,
                on_start=on_start,
                on_done=on_done,
            )
        )

    stage_ = to_stage(stage)

    stage_ = Stage(
        process_fn=Each(f),
        timeout=timeout,
        dependencies=[stage_],
        on_start=on_start,
        on_done=on_done,
        f_args=pypeln_utils.function_args(f),
    )

    if not run:
        return stage_

    for _ in stage_:
        pass
