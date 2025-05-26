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

### Stock Pipeline
Stocks prices of S&P 500 companies from 2009 to 2024 are extracted from yahoo finance using yfinance API, and loaded to postgres or Redshfit. 
The table in the database contains the closing and opening price each day, as well as highest and lowest price that day and total trade volumn.

### News Pipeline
News are sourced from [here](https://huggingface.co/datasets/Zihan1004/FNSPID) and [here](https://www.kaggle.com/datasets/miguelaenlle/massive-stock-news-analysis-db-for-nlpbacktests/data), 
which contains financial news from 2009 to 2023. The data includes the news title, stock symbol, and date. 

### Future Work

### Run 
Run pipeline docker
```
  docker compose -f .\docker-compose.pipeline.yaml up -d
  docker compose -f .\docker-compose.pipeline.yaml down 
```

Run dashboard docker
```
  docker compose -f .\docker-compose.analytics.yaml up -d
  docker compose -f .\docker-compose.analytics.yaml down  
```