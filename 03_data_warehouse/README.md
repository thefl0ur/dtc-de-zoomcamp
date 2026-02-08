creating tables

```sql
CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset_name.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://bucket_name/yellow_tripdata_2024-*.parquet'] 
);

CREATE OR REPLACE TABLE `project_id.dataset_name.regular_yellow_taxi` AS
SELECT * FROM `project_id.dataset_name.external_yellow_taxi`;
```

Q2

```sql
SELECT count(DISTINCT(PULocationID))
FROM `project_id.dataset_name.regular_yellow_taxi`;

SELECT count(DISTINCT(PULocationID))
FROM `project_id.dataset_name.external_yellow_taxi`;
```

Q3

```sql
SELECT PULocationID FROM `project_id.dataset_name.regular_yellow_taxi`;

SELECT PULocationID, DOLocationID FROM `project_id.dataset_name.regular_yellow_taxi`;
```

Q4

```sql
SELECT COUNT(*) FROM `project_id.dataset_name.regular_yellow_taxi` WHERE fare_amount = 0;
```

Q5

```sql
CREATE OR REPLACE TABLE `project_id.dataset_name.partitioned_clustered_yellow_taxi`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM  `project_id.dataset_name.regular_yellow_taxi`;
```

Q6

```sql
SELECT DISTINCT(VendorID)
FROM `project_id.dataset_name.partitioned_clustered_yellow_taxi`
where tpep_dropoff_datetime between '2024-03-01' and '2024-03-16';
```