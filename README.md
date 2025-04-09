# Financial Data Analysis | End-to-End Data Project

### Overview
The goal of this project is to provide a simple and easy way to understand a company's performance based on its financial statements, stock prices, and news. Users can also compare different companies to identify trends across different industries. This project aims to develop a quick, intuitive understanding of a company for people who are beginners in investing.

### Data Architecture
![alt text](https://github.com/WeeFav/FinancialAnalysis/blob/main/github_images/pipeline.png?raw=true)
Financial statements, including income statement, balance sheet, and cash flow, comes from the U.S. Securities and Exchange Commission (SEC). Both stock prices and market news come from Yahoo Finance. ETL is done with PySpark through Airflow automation. Data are loaded to AWS S3 and then loaded to Redshfit data warehouse, which can be queried for downstream analysis tasks.

## Run 
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