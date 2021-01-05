# Changelog

## [0.4.7](https://github.com/cgarciae/pypeln/tree/0.4.7) (2021-01-05)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.6...0.4.7)

**Fixed bugs:**

- \[Bug\] maxsize not being respected for thread.map [\#64](https://github.com/cgarciae/pypeln/issues/64)

**Closed issues:**

- maxsize not being respected for process.map [\#55](https://github.com/cgarciae/pypeln/issues/55)

**Merged pull requests:**

- lock-namespace [\#69](https://github.com/cgarciae/pypeln/pull/69) ([cgarciae](https://github.com/cgarciae))
- Fix maxsize in process, task and thread [\#66](https://github.com/cgarciae/pypeln/pull/66) ([charlielito](https://github.com/charlielito))
- Update bug\_report.md [\#65](https://github.com/cgarciae/pypeln/pull/65) ([charlielito](https://github.com/charlielito))
- fix/ci [\#62](https://github.com/cgarciae/pypeln/pull/62) ([cgarciae](https://github.com/cgarciae))
- Update advanced.md [\#57](https://github.com/cgarciae/pypeln/pull/57) ([isaacjoy](https://github.com/isaacjoy))

## [0.4.6](https://github.com/cgarciae/pypeln/tree/0.4.6) (2020-10-11)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.5...0.4.6)

**Closed issues:**

- ordered in pypeln.task is not always ordered [\#56](https://github.com/cgarciae/pypeln/issues/56)

**Merged pull requests:**

- fix/maxsize [\#59](https://github.com/cgarciae/pypeln/pull/59) ([cgarciae](https://github.com/cgarciae))

## [0.4.5](https://github.com/cgarciae/pypeln/tree/0.4.5) (2020-10-04)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.4...0.4.5)

## [0.4.4](https://github.com/cgarciae/pypeln/tree/0.4.4) (2020-07-09)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.3...0.4.4)

**Closed issues:**

- Update medium blog post [\#49](https://github.com/cgarciae/pypeln/issues/49)

**Merged pull requests:**

- feature/fix-manager [\#50](https://github.com/cgarciae/pypeln/pull/50) ([cgarciae](https://github.com/cgarciae))
- \[Feat\] Apply this to get a test Dockerfile. [\#44](https://github.com/cgarciae/pypeln/pull/44) ([Davidnet](https://github.com/Davidnet))

## [0.4.3](https://github.com/cgarciae/pypeln/tree/0.4.3) (2020-06-27)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.2...0.4.3)

**Closed issues:**

- TypeError: 'coroutine' object is not iterable in pypeln 0.4.2 [\#46](https://github.com/cgarciae/pypeln/issues/46)
- Version 0.4.0+ doesn't work on Python 3.6 [\#41](https://github.com/cgarciae/pypeln/issues/41)

**Merged pull requests:**

- Fix flat\_map [\#47](https://github.com/cgarciae/pypeln/pull/47) ([cgarciae](https://github.com/cgarciae))
- Fix typo in code example in docs [\#45](https://github.com/cgarciae/pypeln/pull/45) ([bryant1410](https://github.com/bryant1410))

## [0.4.2](https://github.com/cgarciae/pypeln/tree/0.4.2) (2020-06-23)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.1...0.4.2)

**Closed issues:**

- could this lib support python 3.6? [\#37](https://github.com/cgarciae/pypeln/issues/37)

**Merged pull requests:**

- Fix dependencies + imports based on Python version [\#42](https://github.com/cgarciae/pypeln/pull/42) ([cgarciae](https://github.com/cgarciae))

## [0.4.1](https://github.com/cgarciae/pypeln/tree/0.4.1) (2020-06-21)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.4.0...0.4.1)

## [0.4.0](https://github.com/cgarciae/pypeln/tree/0.4.0) (2020-06-21)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.3.3...0.4.0)

**Closed issues:**

- BrokenPipeError \[Errno 32\] when using process [\#39](https://github.com/cgarciae/pypeln/issues/39)
- RuntimeError: Timeout context manager should be used inside a task [\#38](https://github.com/cgarciae/pypeln/issues/38)

**Merged pull requests:**

- 0.4.0 [\#40](https://github.com/cgarciae/pypeln/pull/40) ([cgarciae](https://github.com/cgarciae))

## [0.3.3](https://github.com/cgarciae/pypeln/tree/0.3.3) (2020-05-31)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.3.0...0.3.3)

**Closed issues:**

- Showing progress with tqdm not working for pl.\*.each [\#33](https://github.com/cgarciae/pypeln/issues/33)

**Merged pull requests:**

- Re-export to\_iterator [\#35](https://github.com/cgarciae/pypeln/pull/35) ([PromyLOPh](https://github.com/PromyLOPh))

## [0.3.0](https://github.com/cgarciae/pypeln/tree/0.3.0) (2020-04-06)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.2.5...0.3.0)

**Closed issues:**

- on\_done is not called with on\_start args [\#29](https://github.com/cgarciae/pypeln/issues/29)
- Task timeout [\#24](https://github.com/cgarciae/pypeln/issues/24)
- Create sync module [\#16](https://github.com/cgarciae/pypeln/issues/16)

**Merged pull requests:**

- Adds ordered operation + minor updates to DI mechanism. [\#34](https://github.com/cgarciae/pypeln/pull/34) ([cgarciae](https://github.com/cgarciae))
- Timeout [\#32](https://github.com/cgarciae/pypeln/pull/32) ([cgarciae](https://github.com/cgarciae))
- Sync Module [\#31](https://github.com/cgarciae/pypeln/pull/31) ([cgarciae](https://github.com/cgarciae))

## [0.2.5](https://github.com/cgarciae/pypeln/tree/0.2.5) (2020-03-04)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.2.2...0.2.5)

**Merged pull requests:**

- Proper kwargs forwarding on run method [\#30](https://github.com/cgarciae/pypeln/pull/30) ([cgarciae](https://github.com/cgarciae))

## [0.2.2](https://github.com/cgarciae/pypeln/tree/0.2.2) (2020-02-22)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.2.0...0.2.2)

**Closed issues:**

- asyncio\_task example fails on Jupyter Notebook [\#26](https://github.com/cgarciae/pypeln/issues/26)
- \[documentation\] Shouldn't it be Pypeln instead of Pypeline? [\#25](https://github.com/cgarciae/pypeln/issues/25)
- tasks.put with a coroutine that returns a value [\#22](https://github.com/cgarciae/pypeln/issues/22)
- tdqm [\#21](https://github.com/cgarciae/pypeln/issues/21)
- \[documentation\] thread.py/process.py docstring mix-up [\#20](https://github.com/cgarciae/pypeln/issues/20)
- multiprocessing.Manager issue [\#19](https://github.com/cgarciae/pypeln/issues/19)
- Implement observe function [\#17](https://github.com/cgarciae/pypeln/issues/17)
- Create a buffering stage [\#15](https://github.com/cgarciae/pypeln/issues/15)
- Create cleanup mechanism [\#13](https://github.com/cgarciae/pypeln/issues/13)
- Complete API Refence / document code [\#8](https://github.com/cgarciae/pypeln/issues/8)
- Create guide [\#6](https://github.com/cgarciae/pypeln/issues/6)
- Create high cpu benchmark [\#5](https://github.com/cgarciae/pypeln/issues/5)
- Create high io benchmark [\#4](https://github.com/cgarciae/pypeln/issues/4)

**Merged pull requests:**

- aiter [\#28](https://github.com/cgarciae/pypeln/pull/28) ([cgarciae](https://github.com/cgarciae))

## [0.2.0](https://github.com/cgarciae/pypeln/tree/0.2.0) (2020-02-18)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.6...0.2.0)

**Closed issues:**

- Install requires should include 'six' [\#18](https://github.com/cgarciae/pypeln/issues/18)

**Merged pull requests:**

- refactor [\#27](https://github.com/cgarciae/pypeln/pull/27) ([cgarciae](https://github.com/cgarciae))

## [0.1.6](https://github.com/cgarciae/pypeln/tree/0.1.6) (2018-11-11)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.5...0.1.6)

## [0.1.5](https://github.com/cgarciae/pypeln/tree/0.1.5) (2018-10-28)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.4...0.1.5)

**Closed issues:**

- Improve error handling [\#12](https://github.com/cgarciae/pypeln/issues/12)

## [0.1.4](https://github.com/cgarciae/pypeln/tree/0.1.4) (2018-10-15)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.3...0.1.4)

## [0.1.3](https://github.com/cgarciae/pypeln/tree/0.1.3) (2018-10-15)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.2...0.1.3)

**Closed issues:**

- Add demo gif to README [\#14](https://github.com/cgarciae/pypeln/issues/14)

## [0.1.2](https://github.com/cgarciae/pypeln/tree/0.1.2) (2018-10-01)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.1...0.1.2)

**Closed issues:**

- worker start and end mechanism [\#10](https://github.com/cgarciae/pypeln/issues/10)

## [0.1.1](https://github.com/cgarciae/pypeln/tree/0.1.1) (2018-09-28)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.1.0...0.1.1)

**Merged pull requests:**

- fixed installation [\#11](https://github.com/cgarciae/pypeln/pull/11) ([0xflotus](https://github.com/0xflotus))

## [0.1.0](https://github.com/cgarciae/pypeln/tree/0.1.0) (2018-09-24)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.0.11...0.1.0)

**Closed issues:**

- optimize io module [\#9](https://github.com/cgarciae/pypeln/issues/9)
- Finish readme [\#7](https://github.com/cgarciae/pypeln/issues/7)

## [0.0.11](https://github.com/cgarciae/pypeln/tree/0.0.11) (2018-09-24)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.0.10...0.0.11)

## [0.0.10](https://github.com/cgarciae/pypeln/tree/0.0.10) (2018-09-24)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/0.0.9...0.0.10)

## [0.0.9](https://github.com/cgarciae/pypeln/tree/0.0.9) (2018-09-22)

[Full Changelog](https://github.com/cgarciae/pypeln/compare/b94703d9f886bc517398a874ca43fcdec0b8d4f2...0.0.9)

**Closed issues:**

- Create Diagram for the Readme + docs [\#3](https://github.com/cgarciae/pypeln/issues/3)
- Refactor Stream to Stage [\#2](https://github.com/cgarciae/pypeln/issues/2)
- Optimize pr.\_from\_iterable [\#1](https://github.com/cgarciae/pypeln/issues/1)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
