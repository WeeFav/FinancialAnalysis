from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import lit, to_date
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('extract_news') \
    .getOrCreate()

def extract():
    """Extract financial news from 2 datasets, transform it, and download to local"""
    
    # get S&P 500 companies symbol
    tickers = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    tickers = tickers['Symbol'].to_list()
    
    # define column type
    schema = types.StructType([
        types.StructField("Unnamed: 0", types.StringType(), True),
        types.StructField("Date", types.StringType(), True),
        types.StructField("Article_title", types.StringType(), True),
        types.StructField("Stock_symbol", types.StringType(), True),
        types.StructField("Url", types.StringType(), True),
        types.StructField("Publisher", types.StringType(), True),
        types.StructField("Author", types.StringType(), True),
        types.StructField("Article", types.StringType(), True),
        types.StructField("Lsa_summary", types.StringType(), True),
        types.StructField("Luhn_summary", types.StringType(), True),
        types.StructField("Textrank_summary", types.StringType(), True),
        types.StructField("Lexrank_summary", types.StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv("./datasets/news_data/nasdaq_exteral_data.csv")

    df = df.withColumn('Date', to_date(df.Date))
    df = df.withColumnsRenamed({'Date': 'date', 'Stock_symbol': 'symbol', 'Article_title': 'title'})
    df = df.filter((df.date.isNotNull()) & (df.symbol.isin(tickers)) & (df.date >= to_date(lit('2009-01-01')))) \
            .select([df.date, df.symbol, df.title])
    df.show()
    
    schema = types.StructType([
        types.StructField("", types.StringType(), True),
        types.StructField("title", types.StringType(), True),
        types.StructField("date", types.StringType(), True),
        types.StructField("stock", types.StringType(), True)
    ])    
    df2 = spark.read.option("header", "true").schema(schema).csv("./datasets/news_data/analyst_ratings_processed.csv")
    
    df2 = df2.withColumn("date", to_date(df2.date))
    df2 = df2.withColumnRenamed('stock', 'symbol')
    df2 = df2.filter((df2.date.isNotNull()) & (df2.symbol.isin(tickers)) & (df2.date >= to_date(lit('2009-01-01')))) \
        .select([df2.date, df2.symbol, df2.title])
    df2.show()
    
    # concatenate both datasets
    df_all = df.union(df2)
    df_all = df.sort("symbol", "date", ascending=[True, False])
    df_all.show()
    print(df.count(), df2.count(), df_all.count())
    
    # save combined dataset locally
    df_all.toPandas().to_csv("./datasets/news_data/combined_clean_news.csv", index=False)

def vis():
    """Visualize news distribution from 2009 to 2024"""
    
    schema = types.StructType([
        types.StructField("date", types.DateType(), True),
        types.StructField("symbol", types.StringType(), True),
        types.StructField("title", types.StringType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv("./datasets/news_data/combined_clean_news.csv")
    df = df.filter(df.symbol == lit('JNJ')) \
            .groupBy("date") \
            .count()
    df = df.toPandas()

    x = np.arange('2009-01-01', '2024-12-31', dtype='datetime64[D]')
    avaiable_dates = np.array([np.datetime64(d) for d in df['date'].to_numpy()])
    date_index = np.isin(x, avaiable_dates)
    y = np.zeros(len(x))
    y[date_index] = df['count'].to_numpy()

    plt.plot(x, y)
    plt.show()

if __name__ == '__main__':
    extract()
    vis()



