import pandas as pd
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session

# get a list of S&P 500 companies
tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

news_df = spark.read.csv("./datasets/nasdaq_exteral_data.csv", header=True, inferSchema=True)
# news_df = news_df.drop('Unnamed: 0', 'Url', 'Publisher', 'Author', 'Lsa_summary', 'Luhn_summary', 'Textrank_summary', 'Lexrank_summary')
news_df = news_df.drop('Unnamed: 0', 'Publisher', 'Author', 'Luhn_summary', 'Textrank_summary', 'Lexrank_summary')
# news_df = news_df.filter(news_df['Stock_symbol'].isin(tickers['Symbol'].to_list()))
news_df = news_df.filter(news_df['Stock_symbol'] == 'NVDA')
# news_df = news_df.withColumn('Date', substring(col('Date'), 1, 10))
news_df.show()
news_df.coalesce(1).write.csv('./datasets/NVDA', header=True)