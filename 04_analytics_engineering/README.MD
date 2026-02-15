## PRE

```bash
./duckdb taxi_rides_ny.duckdb
```

## Q3

```sql
SELECT COUNT(*)
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue;
```

## Q4

```sql
SELECT pickup_zone, AVG(revenue_monthly_total_amount) as avg_revenue_monthly_total_amount
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE YEAR(revenue_month) = '2020' AND service_type = 'Green'
GROUP BY pickup_zone
ORDER BY avg_revenue_monthly_total_amount DESC
LIMIT 1;
```

## Q5

```sql
SELECT count(*)
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE YEAR(revenue_month) = '2019' AND service_type = 'Green' and MONTH(revenue_month) = '10';
```

## Q6

```sql
SELECT COUNT(*)
FROM taxi_rides_ny.prod.stg_fhv_tripdata;
```

