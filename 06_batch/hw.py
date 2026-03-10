from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName('test').getOrCreate()

# Q1
print(f"Q1: {spark.version}")

# Q2
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.mode("overwrite").parquet('data/taxi/yellow/2025/11/')

# Q3
from pyspark.sql import functions as F
df = spark.read.parquet('data/taxi/yellow/2025/11')
count = df.filter(F.to_date("tpep_pickup_datetime") == "2025-11-15").count()
print(f"Q3: {count}")

# Q4
duration = df.withColumn(
    "duration", 
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))/3600,
).select(F.max("duration")).collect()[0][0]
print(f"Q4: {round(duration, 1)}")

# Q6
df_zones = spark.read.option("header", "true").csv('taxi_zone_lookup.csv')
zone = (
    df.groupBy("PULocationID")
    .count()
    .join(
        df_zones,
        df.PULocationID == df_zones.LocationID,
        "inner",
    )
    .orderBy("count", ascending=True)
    .select("Zone")
    .collect()
    )[0][0]
print(f"Q6: {zone}")

spark.stop()
