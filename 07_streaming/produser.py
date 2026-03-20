import json

from pathlib import Path
from time import time

import pandas as pd

from kafka import KafkaProducer

import dataclasses

from dataclasses import dataclass


@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    tip_amount: float
    trip_distance: float
    total_amount: float
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int


SERVER_ADDRESS = 'localhost:9092'
TOPIC_NAME = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[SERVER_ADDRESS],
    value_serializer=lambda rec: json.dumps(dataclasses.asdict(rec)).encode('utf-8'),
)

columns  =['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
file_path = Path('green_tripdata_2025-10.parquet')
df = pd.read_parquet(file_path, columns=columns)

start = time()
for _, row in df.iterrows():
    producer.send(
        TOPIC_NAME, 
        value= Ride(
            PULocationID=int(row['PULocationID']),
            DOLocationID=int(row['DOLocationID']),
            trip_distance=float(row['trip_distance']),
            total_amount=float(row['total_amount']),
            passenger_count=float(row['passenger_count']),
            tip_amount=float(row['tip_amount']),
            lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
            lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        ),
    )

producer.flush()
end = time()

print(f'took {(end - start):.2f} seconds')

