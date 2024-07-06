SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number,
    year(pickup_datetime) as year_ref
FROM {{ source('bronze','raw_fhv_trip_records') }}
WHERE 1=1
  AND YEAR(pickup_datetime) >= 2020
