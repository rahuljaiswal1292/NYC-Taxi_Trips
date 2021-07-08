WITH filtered_dataset AS (
SELECT passenger_count, vendor_id, pickup_datetime, dropoff_datetime, 
pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude
FROM statueofliberty-318808.nyc_taxi_trips.tlc_yellow_trips_2016 
WHERE dropoff_longitude BETWEEN -180 AND 180
AND dropoff_latitude BETWEEN -90 AND 90
AND pickup_longitude BETWEEN -180 AND 180 
AND pickup_latitude BETWEEN -90 AND 90
AND passenger_count BETWEEN 1 AND 6
AND ST_DISTANCE(ST_GEOGPOINT(-74.0474287, 40.6895436), ST_GEOGPOINT(dropoff_longitude,dropoff_latitude)) <= 3500
),

trips_and_time AS (
SELECT T1.passenger_count AS passengerCount
,COUNT(DISTINCT T1.pickup_datetime) AS numberOfTrips
,SUM(DATETIME_DIFF(T1.dropoff_datetime,T1.pickup_datetime,MINUTE)) AS totalTimeInMinutes
FROM filtered_dataset T1
GROUP BY passenger_count
),

no_of_cabs AS (
SELECT A.passenger_count AS passengerCount, 
COUNT(DISTINCT A.pickup_datetime) AS total
FROM (
SELECT FD1.passenger_count, FD1.vendor_id, FD1.pickup_datetime, FD1.dropoff_datetime,
LAG(FD1.dropoff_datetime) 
	OVER (PARTITION BY FD1.passenger_count, FD1.pickup_datetime ORDER BY FD1.passenger_count, FD1.pickup_datetime ASC) 
	AS lag_dropoff
FROM filtered_dataset FD1 JOIN filtered_dataset FD2 
ON  FD1.dropoff_datetime = FD2.pickup_datetime
AND FD1.pickup_datetime < FD2.pickup_datetime
--AND FD1.dropoff_longitude = FD2.pickup_longitude
--AND FD1.dropoff_latitude = FD2.pickup_latitude
) A
WHERE A.lag_dropoff IS NULL
GROUP BY A.passenger_count
)

SELECT 
TT.passengerCount AS passengerCount, 
TT.numberOfTrips AS numberOfTrips,
TT.totalTimeInMinutes AS totalTimeInMinutes,
NOC.total AS numberOfCabsRequired
FROM trips_and_time TT JOIN no_of_cabs NOC
ON TT.passengerCount = NOC.passengerCount
ORDER BY TT.passengerCount ASC