import pypeln as pl
from tqdm import tqdm
import time

total = 300_000


def f(x):
    i = 0
    for _ in range(500_000):
        i += 1

    return x * 2


stage = range(total)
stage = pl.process.map(f, stage, workers=8, maxsize=2000)
stage = tqdm(stage, total=total, desc="pipeline")

pl.process.run(stage)
