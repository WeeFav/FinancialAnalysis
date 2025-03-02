import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter
import yfinance as yf

# spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session
# num_df = spark.read.csv("./datasets/2024q2/num.txt", sep='\t', header=True, inferSchema=True) # read financial data
# # filter for AAPL
# num_df = num_df.filter((num_df.adsh == "0000320193-24-000069") & 
#               (num_df.tag == "RevenueFromContractWithCustomerExcludingAssessedTax") &
#               (num_df.segments.isNull()) &
#               (num_df.qtrs == 1))
# # drop irrevelant columns
# num_df = num_df.drop("adsh", "tag", "version", "qtrs", "uom", "segments", "coreg", "footnote")
# # extract revenue year
# num_df = num_df.withColumn("year", substring(col("ddate"), 1, 4)) \
#                .withColumn("metric", concat_ws(" ", col("year"), lit("Q2 Revenue"))) \
#                .withColumn("value", col("value") / 1000000)               
# num_df = num_df.select("metric", "value")
# num_df.show()
# num_df.write.parquet("./aapl_fs.parquet")

# appl = yf.Ticker("AAPL")
# stock_price = appl.history(start="2024-01-01", end="2024-12-31", interval="1d")
# stock_price = stock_price.drop(["Open", "High", "Low", "Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
# # convert datetime index into date column
# stock_price.index = stock_price.index.date
# stock_price = stock_price.reset_index()
# stock_price = stock_price.rename(columns={'index':'Date'})
# stock_price.to_parquet('./stock_price.parquet')

news = yf.Search("NVDA", news_count=1).news
print(news)

