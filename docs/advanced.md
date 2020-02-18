# Advanced Usage

## Architecture
A Pypeln pipeline has the following structure:

![diagram](images/diagram.png)

* Its composed of several concurrent **stages**
* At each stage it contains on or more **worker** entities that perform a task.
* Related stages are connected by a **queue**, workers from one stage *put* items into it, and workers from the other stage *get* items from it.
* Source stages consume iterables.
* Sink stages can be converted into iterables which 
consume them.

## Managing Resources

## Process and Thread

## Async Task