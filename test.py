import sys
import polars as pl
import time

start = time.time()
df = pl.scan_parquet(sys.argv[1], low_memory=True).filter(
    pl.col("name").str.ends_with(".xml")
).with_columns(
    body_str=pl.col("body").cast(pl.Utf8)
)

print(df.collect())
end = time.time()
print("Time elapsed:", end-start)