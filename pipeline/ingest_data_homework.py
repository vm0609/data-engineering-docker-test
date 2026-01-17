#!/usr/bin/env python
# coding: utf-8




import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import numpy as np

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}


parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_pass', default='root', help='PostgreSQL password')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432,  help='PostgreSQL port')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, help='Year')
@click.option('--month', default=1, help='Month')
def run(pg_user,
        pg_pass,
        pg_host,
        pg_port,
        pg_db,target_table,year,month):



    #chunksize=100000

    
    
   
    file_path = '/workspaces/data-engineering-docker-test/pipeline/data/green_tripdata_2025-11.parquet'
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print('Engine acquired')
    df = pd.read_parquet(file_path)
    print('Read parquet file successfully')
    
    print(f'No records in df {len(df)} ')
    #print(df.head())
       # Load the data into PostgreSQL
    chunk_size = 500000
    num_chunks = int(np.ceil(len(df) / chunk_size))
   
    print(f'nm of chunks is {num_chunks}')

    first = True
    for i in range(num_chunks):
        start = i * chunk_size
        end = (i + 1) * chunk_size
        df_chunk = df.iloc[start:end]

        if first:
            # Create the table without data
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists='replace',
                index=False
            )
            print('Table replaced')
            first = False

        # Append the chunk to the table
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
        print(f'Loaded chunk {i + 1} of {num_chunks}')

    print(f'Data from {file_path} has been loaded into the {target_table} table.')
 

    # Load taxi_zone_lookup.csv into PostgreSQL
    csv_file_path = '/workspaces/data-engineering-docker-test/pipeline/data/taxi_zone_lookup.csv'
    print(f'Loading data from {csv_file_path}')
    
    # Read the CSV file
    df_csv = pd.read_csv(csv_file_path)
    print('Read CSV file successfully')
    print(f'No records in CSV: {len(df_csv)}')
    print(df_csv.head())

    # Load the CSV data into PostgreSQL
    csv_table_name = 'taxi_zone_lookup'
    df_csv.to_sql(name=csv_table_name, con=engine, if_exists='replace', index=False)
    print(f'Data from {csv_file_path} has been loaded into the {csv_table_name} table.')



if __name__ == '__main__':
   
    run()