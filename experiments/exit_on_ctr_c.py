from pypeln import process as pr
import time

def do_print(x):
    time.sleep(1)
    print(x)

stage = pr.map(do_print, range(1000), workers=5)

pr.run(stage)