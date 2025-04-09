from sqlalchemy import create_engine
import pandas as pd

user = 'airflow'
password = 'airflow'
host = 'localhost'
port = '5432'
database = 'financial_statements'
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
tickers[['Symbol', 'CIK']].to_sql('ticker_cik_mapping', engine, if_exists='replace', index=False)