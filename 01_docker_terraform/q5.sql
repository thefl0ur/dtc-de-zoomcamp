select
	tzl."Zone",
	sum(gt.total_amount) as total_sum
from
	green_tripdata_2025_11 gt
join
	taxi_zone_lookup tzl on
	tzl."LocationID" = gt."PULocationID"
where
	DATE(gt.lpep_pickup_datetime) = '2025-11-18'
group by
	tzl."Zone"
order by
	total_sum desc
limit 1;