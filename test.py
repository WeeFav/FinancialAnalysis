import pandas as pd

df = pd.read_parquet("./fs.parquet")
print(df.head())