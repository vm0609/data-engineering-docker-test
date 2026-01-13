#!/usr/bin/env python
# coding: utf-8




import pandas as pd
from sqlalchemy import create_engine

year=2021
month=1



prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = f'{prefix}'/yellow_tripdata_{year}-{month:02d}.csv.gz





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

df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)
df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)


pg_user = 'root'
pg_pass =  'root'
pg_host = 'localhost'
pg_port = 5423
pg_db   = 'ny_taxi'





engine = create_engine('postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')





# In[24]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[25]:


len(df)


# In[34]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)


# In[36]:


df_next= next(df_iter)


# In[37]:


df_next


# In[37]:


df_next


# In[40]:


get_ipython().system('uv add tqdm')


# In[41]:


from tqdm.auto import tqdm


# In[42]:


for df_chunk in tqdm(df_iter):
   df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[44]:


df_iter = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)


# In[45]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[46]:


for df_chunk in df_iter:
   df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




