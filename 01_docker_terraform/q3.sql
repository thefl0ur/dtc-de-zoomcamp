select
	count(gt."index")
from
	green_tripdata_2025_11 gt
where
	trip_distance <= 1.0
	and
	lpep_pickup_datetime between '2025-11-01' and '2025-12-01';