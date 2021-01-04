import pypeln as pl


stage = pl.process.map(lambda x: str(x), range(20_000), workers=4)
data = list(stage)