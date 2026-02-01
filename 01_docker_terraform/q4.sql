select
	gt.lpep_pickup_datetime,
	gt.trip_distance
from
	green_tripdata_2025_11 gt
where
	trip_distance <= 100.0
order by
	gt.trip_distance desc
limit 1;