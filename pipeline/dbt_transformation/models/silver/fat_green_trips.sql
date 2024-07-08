SELECT
      vendorid AS vendor_id
    , lpep_pickup_datetime
    , lpep_dropoff_datetime
    , store_and_fwd_flag
    , ratecodeid AS ratecode_id
    , pulocationid AS pulocation_id
    , dolocationid AS dolocation_id
    , COALESCE(passenger_count,1) AS passenger_count
    , trip_distance
    , fare_amount
    , extra
    , mta_tax
    , tip_amount
    , tolls_amount
    , improvement_surcharge
    , total_amount
    , ehail_fee
    , payment_type AS payment_id
    , trip_type AS trip_id
    , COALESCE(congestion_surcharge,0) AS congestion_surcharge
    , YEAR(lpep_pickup_datetime) as year_ref
    , CURRENT_TIMESTAMP AS created_at
FROM {{ source('bronze','raw_green_taxi_trip_records') }}
WHERE 1=1
  AND YEAR(lpep_pickup_datetime) >= 2020
