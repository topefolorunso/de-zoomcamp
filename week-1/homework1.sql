
-- How many taxi trips were totally made on January 15?
SELECT COUNT(*)
FROM green_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-15'

-- Which was the day with the largest trip distance?
SELECT CAST(lpep_pickup_datetime AS DATE) trip_date, trip_distance
FROM green_trips
ORDER BY trip_distance DESC

-- In 2019-01-01 how many trips had 2 and 3 passengers?
SELECT passenger_count, COUNT(*)
FROM green_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' AND passenger_count IN (2, 3)
GROUP BY passenger_count


-- For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
SELECT tzl."Zone" zone, dol.tip tip
FROM
    (SELECT gt."DOLocationID", gt."tip_amount" tip
    FROM green_trips gt
        JOIN taxi_zone_lookup tzl ON gt."PULocationID"=tzl."LocationID"
    WHERE tzl."Zone"='Astoria') dol
        JOIN taxi_zone_lookup tzl ON dol."DOLocationID"=tzl."LocationID"
ORDER BY tip DESC