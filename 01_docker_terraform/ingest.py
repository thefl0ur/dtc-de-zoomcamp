import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

CHUNK = 1000

load_dotenv()

engine = create_engine(f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@localhost:5433/{os.getenv("POSTGRES_DB")}')


df = pd.read_parquet("green_tripdata_2025-11.parquet")
print("Inserting data from PARQUET")
for i in range(0, len(df), CHUNK):
    df_chunk = df.iloc[i:i + CHUNK]
    df_chunk.to_sql(
        name="green_tripdata_2025_11",
        con=engine,
        if_exists='replace' if i == 0 else 'append'
    )

df_iter = pd.read_csv("taxi_zone_lookup.csv", iterator=True, chunksize=CHUNK)
print("Inserting data from CSV")
first = True
for df_chunk in df_iter:
    if first:
        df_chunk.head(0).to_sql(name="taxi_zone_lookup", con=engine, if_exists='replace')
        first = False
    df_chunk.to_sql(name="taxi_zone_lookup", con=engine, if_exists='append')