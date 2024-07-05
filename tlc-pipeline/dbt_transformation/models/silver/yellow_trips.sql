SELECT
    CASE
      WHEN vendorid = 1 THEN 'Creative Mobile Technologies'
      WHEN vendorid = 2 THEN 'VeriFone Inc'
      ELSE 'N/D'
    END AS vendor,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COALESCE(passenger_count,1) AS passenger_count,
    trip_distance,
    CASE
        WHEN ratecodeid = 1 THEN 'STANDARD RATE'
        WHEN ratecodeid = 2 THEN 'JFK'
        WHEN ratecodeid = 3 THEN 'NEWARK'
        WHEN ratecodeid = 4 THEN 'NASSAU OR WESTCHESTER'
        WHEN ratecodeid = 5 THEN 'NEGOTIATED FARE'
        WHEN ratecodeid = 6 THEN 'GROUP RIDE'
        ELSE 'N/D'
    END AS ratecode,
    CASE
      WHEN store_and_fwd_flag = 'Y' THEN 'STORE AND FORWARD TRIP'
      WHEN store_and_fwd_flag = 'N' THEN 'NOT A STORE AND FORWARD TRIP'
      ELSE 'N/D'
    END AS store_and_fwd_flag,
    pulocationid,
    dolocationid,
    CASE
        WHEN payment_type = 1 THEN 'CREDIT CARD'
        WHEN payment_type = 2 THEN 'CASH'
        WHEN payment_type = 3 THEN 'NO CHARGE'
        WHEN payment_type = 4 THEN 'DISPUTE'
        WHEN payment_type = 5 THEN 'UNKNOWN'
        WHEN payment_type = 6 THEN 'VOIDED TRIP'
        ELSE 'N/D'
    END AS payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    COALESCE(congestion_surcharge,0) AS congestion_surcharge,
    airport_fee,
    YEAR(tpep_pickup_datetime) AS year_ref
FROM {{ source('bronze','raw_yellow_taxi_trip_records') }}
WHERE 1=1
  AND YEAR(tpep_pickup_datetime) >= 2020
