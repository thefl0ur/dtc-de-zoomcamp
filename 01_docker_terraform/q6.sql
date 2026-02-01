select
	doz."Zone",
	max(gt.tip_amount) as tips
from
	green_tripdata_2025_11 gt
join
	taxi_zone_lookup puz on
	puz."LocationID" = gt."PULocationID"
join
	taxi_zone_lookup doz on
	doz."LocationID" = gt."DOLocationID"
where
	puz."Zone" = 'East Harlem North' and
	gt.lpep_pickup_datetime between '2025-11-01' and '2025-12-01'
group by
	doz."Zone"
order by
	tips desc
limit 1;