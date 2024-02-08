import sys
import polars as pl
import time

start = time.time()
df = pl.scan_parquet(sys.argv[1], low_memory=True)

print(df.collect())
end = time.time()
print("Time elapsed:", end-start)