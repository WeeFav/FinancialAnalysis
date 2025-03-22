import pandas as pd

df = pd.read_parquet("stock.parquet")
print(df.dtypes)
