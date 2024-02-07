import sys
import polars as pl
import time

start = time.time()
print(pl.read_parquet(sys.argv[1], low_memory=True))
end = time.time()
print("Time elapsed:", end-start)