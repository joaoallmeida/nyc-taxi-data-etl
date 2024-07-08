SELECT
    vendorid AS vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COALESCE(passenger_count,1) AS passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid AS pulocation_id,
    dolocationid AS dolocation_id,
    payment_type AS payment_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    COALESCE(congestion_surcharge,0) AS congestion_surcharge,
    airport_fee,
    YEAR(tpep_pickup_datetime) AS year_ref,
    CURRENT_TIMESTAMP AS created_at
FROM {{ source('bronze','raw_yellow_taxi_trip_records') }}
WHERE 1=1
  AND YEAR(tpep_pickup_datetime) >= 2020
