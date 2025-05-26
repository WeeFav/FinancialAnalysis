# Financial Data Analysis | End-to-End Data Project

### Overview
The goal of this project is to provide financial analysis of S&P 500 companies by analyzing their financial statements
and forecast stock prices using historical data and daily news. The projects aims to provide a simple and easy way to 
understand a company's financial performance using a data-driven approach.

### Data Architecture
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/pipeline.png?raw=true)
Financial statements, including income statement, balance sheet, and cash flow, comes from the U.S. Securities and Exchange Commission (SEC). Both stock prices and market news come from Yahoo Finance. ETL is done with PySpark through Airflow automation. Data are loaded to AWS S3 and then loaded to Redshfit data warehouse, which can be queried for downstream analysis tasks.

### Financial Statements Pipeline
Financal statements are downloaded from [this dataset](https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets)
from the SEC website and are uploaded to S3. The fs_pipeline dag in airflow extract the statements from S3, transform and load to postgres for local development or Redshift for cloud development.
You can specify which year and which quater to extract from in airflow. 
The sub table in the database contains metadata about quarterly and annual statements, and the num table contains all numerical data on these statements.   

Usage: run the pipeline docker compose and trigger the fs_pipeline dag. Choose database type and specify the folder name of the year/quater

### Stock Pipeline
Stocks prices of S&P 500 companies from 2009 to 2024 are extracted from yahoo finance using yfinance API, and loaded to postgres or Redshfit. 
The table in the database contains the closing and opening price each day, as well as highest and lowest price that day and total trade volumn.

Usage: run the pipeline docker compose and trigger the stock_pipeline dag. Choose database type.

### News Pipeline
News are sourced from [here](https://huggingface.co/datasets/Zihan1004/FNSPID) and [here](https://www.kaggle.com/datasets/miguelaenlle/massive-stock-news-analysis-db-for-nlpbacktests/data), 
which contains financial news from 2009 to 2023. The processed data includes the news title, stock symbol, and date. 

Usage: run the extract_news.py script. The proccesed, combined data will be saved locally.

### Exploratory Data Analysis
Based on the financial statements and stock price, we can conduct data analysis to analyze companies performance.
Metabase is used for visualizing financial analysis, such as year over year revenue growth, ROE, P/E ratio, and stock price performance.
SQL query are conducted against the database, and results are shown through line chart, bar chart, and number card.
Users can filter on specific company at a specific quater and get an simple and easy to understand visualization about important metrics for analysis.

![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/dashboard1.png?raw=true)
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/dashboard2.png?raw=true)
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/dashboard3.png?raw=true)

Usage: run the analytics docker compose and paste the SQL query into metabase. A full list of query can be found in sql/metabase.sql

### Stock Price Prediction
The [FinBert](https://huggingface.co/ProsusAI/finbert) pre-train model is used to classify news sentiment into positive, neutral, or negative.
We can then use the proccesed data, with sentiment analysis, to predict stock price.
Stock price prediction is done using classical statisical model (ARIMA), machine learning (Random Forest), and deep learning (LSTM)
Both ARIMA and Random Forest achieves high mean squared error, while LSTM performed the best, at a significantly lower MSE, which is expected.
Sentiment analysis and stock prediction are done in both google colab (local development) and AWS SageMaker (cloud development). 

![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/lstm.png?raw=true)
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/ARIMA.png?raw=true)
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/RF.png?raw=true)

Usage: First generate news sentiment by running the respective FinBert jupyter notebook for google colab or AWS sagemaker. Then run the Stock Predict notebook to forecast stock price 

### Future Work
- Web scrape latest news
- Compare different companies' finanaical performance

### Run 
Run pipeline
```
  docker compose -f .\docker-compose.pipeline.yaml up -d
  docker compose -f .\docker-compose.pipeline.yaml down 
```

Run dashboard
```
  docker compose -f .\docker-compose.analytics.yaml up -d
  docker compose -f .\docker-compose.analytics.yaml down  
```