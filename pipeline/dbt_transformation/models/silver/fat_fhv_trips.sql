SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid AS pulocation_id,
    dolocationid AS dolocation_id,
    sr_flag,
    affiliated_base_number,
    YEAR(pickup_datetime) as year_ref,
    CURRENT_TIMESTAMP AS created_at
FROM {{ source('bronze','raw_fhv_trip_records') }}
WHERE 1=1
  AND YEAR(pickup_datetime) >= 2020
