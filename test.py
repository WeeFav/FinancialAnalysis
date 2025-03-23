import pandas as pd

df = pd.read_parquet("num.parquet")
print(len(df))
print(df.dtypes)
