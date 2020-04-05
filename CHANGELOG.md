# Changelog

## [0.3.0] - 2020-03-14
### Adds
* `ordered` funtion in all modules, this orders output elements based on the order of creation on the source iterable.
* 

## [0.2.7] - 2020-03-14
### Adds
* `timeout` parameter to most funtions in all modules, this stops code execution after a given amount of time if the task has not been completed.

## [0.2.6] - 2020-03-04
### Adds
* `sync` module which follows Pypeln's API but executes everything synchronously using python generators, meant for debugging purposes.

## [0.2.5] - 2020-03-03
### Fixes
* Fixed critical bug (#29) related to `**kwarg` arguments created in `on_start` not being passed to `on_done`.