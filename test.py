import sys
import polars as pl

print(pl.read_parquet(sys.argv[1], low_memory=True))