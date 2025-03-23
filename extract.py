import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, concat_ws, lit, quarter, to_date, year
import yfinance as yf
from sec_cik_mapper import StockMapper
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
import requests
import json
import os

# mapper = StockMapper()
# ticker_to_cik = mapper.ticker_to_cik
# tags = ["RevenueFromContractWithCustomerExcludingAssessedTax", "CostOfGoodsAndServicesSold", "GrossProfit", "ResearchAndDevelopmentExpense", "SellingGeneralAndAdministrativeExpense", "OperatingExpenses", "OperatingIncomeLoss", "NetIncomeLoss"]
# new_files = ["2024q1", "2024q2", "2024q3", "2024q4"]

# spark: SparkSession = SparkSession.builder.getOrCreate() # create spark session

# for file in new_files:
#     sub_file = os.path.join("./datasets/data", file, "sub.txt")
#     num_file = os.path.join("./datasets/data", file, "num.txt")
        
#     sub_df = spark.read.csv(sub_file, sep='\t', header=True, inferSchema=True)
#     sub_df = sub_df.filter((sub_df.cik.isin([int(ticker_to_cik['AAPL'])])) & (sub_df.form.isin(["10-Q", "10-K"])))
#     sub_df = sub_df.drop("cik", "sic", "zipba", "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma", "mas1", "mas2", "countryinc", "stprinc", "ein", "former", "changed", "afs", "wksi", "filed", "accepted", "prevrpt", "detail", "instance", "nciks", "aciks")
#     sub_df = sub_df.withColumn("period", to_date("period", 'yyyyMMdd'))
#     sub_df.show()

#     num_df = spark.read.csv(num_file, sep='\t', header=True, inferSchema=True) # read financial data
#     num_df = num_df.withColumn("ddate", substring("ddate", 0, 4)).join(sub_df.withColumn("ddate", year("period")), ["adsh", "ddate"], "left_semi")
#     num_df = num_df.filter((num_df.tag.isin(tags)))
#     num_df = num_df.drop("version", "ddate", "uom", "coreg", "footnote")
#     num_df = num_df.withColumn("value", col("value") / 1000000) # convert values with unit of millions 
#     num_df.show()
    
    # os.makedirs(f"./{file}", exist_ok=True)

    # num_df.toPandas().to_parquet(f"./{file}/num.parquet")
    # sub_df.toPandas().to_parquet(f"./{file}/sub.parquet")
    # print(f"Saved {file} parquet file")


appl = yf.Tickers(["AAPL", "GOOG", "MSFT"])
stock_price = appl.history(start="2024-01-01", end="2024-12-31", interval="1d")
stock_price = stock_price.drop(["Open", "High", "Low", "Dividends", "Stock Splits"], axis=1) # drop irrelevant columns
stock_price = stock_price.stack(level=1).reset_index()
stock_price["Date"] = stock_price["Date"].dt.date
# stock_price['Date'] = stock_price["Date"].date # convert datetime index into date
# stock_price = stock_price.reset_index() # reset index to default integer index and move existing date index into a column
# stock_price = stock_price.rename(columns={'index':'Date'})
stock_price['Volume'] = stock_price['Volume'].astype(np.int32)
print(stock_price["Date"].dtype)
# stock_price.to_parquet('./stock.parquet')

# header = {'Connection': 'keep-alive',
#           'Expires': '-1',
#           'Upgrade-Insecure-Requests': '1',
#           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
#         }

# def scrape(url):
#     content = ""
#     result = requests.get(url, headers=header)
#     doc = BeautifulSoup(result.text, "html.parser")
#     article = doc.find(class_="article yf-l7apfj")
#     tags = article.find_all("p", class_=["yf-1090901", "yf-1ba2ufg"])

#     for tag in tags:
#         content += tag.text + "\n"

#     return content    

# news_list = yf.Search("NFLX", news_count=5).news

# processed_news = []

# for news in news_list:
#     news_dict = {}
#     news_dict['title'] = news['title']
#     news_dict['date'] = datetime.fromtimestamp(news['providerPublishTime']).date().strftime('%Y-%m-%d')
#     news_dict['content'] = scrape(news['link'])
#     processed_news.append(news_dict)

# with open("./news.json", "w") as fp:
#     json.dump(processed_news, fp)

# # get a list of S&P 500 companies
# tickers = pd.read_html(
#     'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
# print(tickers)
