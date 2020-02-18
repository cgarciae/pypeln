import pypeln as pl
import time


def do_print(x):
    time.sleep(1)
    print(x)


stage = pl.process.map(do_print, range(1000), workers=5)

pl.process.run(stage)
