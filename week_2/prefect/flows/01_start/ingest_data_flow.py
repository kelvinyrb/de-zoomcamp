#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, tags=["extract"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
        print("csv name is yellow taxi")
    else:
        csv_name = 'output.csv'
        print("csv name is output")
    
    os.system(f"wget {url} -O {csv_name}")
    print("Got csv file!")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df

@task(log_prints=True, tags=["transform"])
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, tags=["load"], retries=3)
# def load_data(user, password, host, port, db, table_name, df):
def load_data(table_name, df):
    # postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    # engine = create_engine(postgres_url)
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
    print("Connection established...")
    
@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging subflow for: {table_name}")

@flow(name="Ingest data")
def main_flow(table_name: str = "yellow_taxi_trips"):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    print(csv_url)
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(table_name, data)
    
# When ingest_data.py is run as a script, the below runs because it's not imported and __name__ == '__main__' (it is the top-level code env)
if __name__ == '__main__':
    main_flow()
