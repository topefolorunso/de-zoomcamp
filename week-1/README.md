
python ingest_green_trips.py \
    --file_name=green_tripdata_2019-01.csv \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5432 \
    --db=de-zoomcamp \
    --table_name=green_trips

python ingest_zone_lookup.py \
    --file_name=taxi+_zone_lookup.csv \
    --user=postgres \
    --password=postgres \
    --host=localhost \
    --port=5432 \
    --db=de-zoomcamp \
    --table_name=taxi_zone_lookup

