# Changelog

## [0.4.1] - 2020-06-21
* Lower Python version requirement to `3.5`, however to use the `task` module will only be available for versions >= `3.7`.

## [0.4.0] - 2020-06-21
* Big internal refactor:
  * Reduces the risk of potential zombie workers
  * New internal Worker and Supervisor classes which make code more readable / maintainable.
  * Code is now split into individual files for each API function to make contribution easier and improve maintainability.
* API Reference docs are now shown per function and a new Overview page was created per module.

#### Breaking Changes
* `maxsize` arguement is removed from all `from_iterable` functions as it was not used.
* `worker_constructor` parameter was removed from all `from_iterable` functions in favor of the simpler `use_thread` argument.

## [0.3.0] - 2020-04-05
* `ordered` function in all modules, this orders output elements based on the order of creation on the source iterable.
* Additional options and rules for the depending injection mechanism. See [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#dependency-injection).
* All `pl.*.Stage` classes now inherit from `pl.BaseStage`.

## [0.2.7] - 2020-03-14
* `timeout` parameter to most funtions in all modules, this stops code execution after a given amount of time if the task has not been completed.

## [0.2.6] - 2020-03-04
* `sync` module which follows Pypeln's API but executes everything synchronously using python generators, meant for debugging purposes.

## [0.2.5] - 2020-03-03
* Fixed critical bug (#29) related to `**kwarg` arguments created in `on_start` not being passed to `on_done`.