import numpy as np
import pandas as pd

sub_df = pd.read_csv('./datasets/2024q2/sub.txt', sep='\t')
num_df = pd.read_csv('./datasets/2024q2/num.txt', sep='\t')
num_df = num_df[num_df.adsh=="0000320193-24-000069"]
print(num_df.head())
