import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter
import yfinance as yf
from sec_cik_mapper import StockMapper
import boto3

mapper = StockMapper()
ticker_to_cik = mapper.ticker_to_cik

spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session
sub_df = spark.read.csv(["./datasets/2024q2/sub.txt", "./datasets/2024q4/sub.txt"], sep='\t', header=True, inferSchema=True)
sub_df = sub_df.filter((sub_df.cik == ticker_to_cik['AAPL']) & ((sub_df.form == "10-Q") | (sub_df.form == "10-K")))
sub_df.show()

num_df = spark.read.csv(["./datasets/2024q2/num.txt", "./datasets/2024q4/num.txt"], sep='\t', header=True, inferSchema=True) # read financial data
num_df = num_df.join(sub_df, "adsh", "left_semi")
num_df = num_df.filter( 
              (num_df.tag.isin(["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]) ) &
              # (num_df.segments.isNull()) &
              (num_df.qtrs == 1) &
              (num_df.ddate.startswith("2024")))
# # drop irrevelant columns
num_df = num_df.drop("version", "qtrs", "uom", "segments", "coreg", "footnote")
# # extract revenue year
num_df = num_df.withColumn("value", col("value") / 1000000)
                              
num_df = num_df.select("adsh", "tag", "year", "value")
num_df.show()

# num_df.write.parquet("./fs.parquet")



# appl = yf.Ticker("AAPL")
# stock_price = appl.history(start="2024-01-01", end="2024-12-31", interval="1d")
# stock_price = stock_price.drop(["Open", "High", "Low", "Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
# # convert datetime index into date column
# stock_price.index = stock_price.index.date
# stock_price = stock_price.reset_index()
# stock_price = stock_price.rename(columns={'index':'Date'})
# stock_price.to_parquet('./stock_price.parquet')

# news = yf.Search("NFLX", news_count=5).news
# print(news)

