import pyarrow.parquet as pq
import pandas as pd
import numpy as np

df = pq.read_table(source="cards.parquet").to_pandas()
# print(df.head())
print(type(df.shape))
