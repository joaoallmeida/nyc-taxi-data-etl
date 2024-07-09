WITH stg_yellow_trips AS (
    SELECT
        vendorid AS vendor_id
        ,tpep_pickup_datetime AS pickup_datetime
        ,tpep_dropoff_datetime AS dropoff_datetime
        ,COALESCE(passenger_count,1) AS passenger_count
        ,trip_distance
        ,ratecodeid AS ratecode_id
        ,pulocationid AS pulocation_id
        ,dolocationid AS dolocation_id
        ,payment_type AS payment_id
        ,1 AS service_id
        ,fare_amount
        ,extra
        ,mta_tax
        ,tip_amount
        ,tolls_amount
        ,improvement_surcharge
        ,total_amount
        ,airport_fee as fee
        ,COALESCE(congestion_surcharge,0) AS congestion_surcharge
        ,YEAR(tpep_pickup_datetime) AS year_ref
        ,MONTH(tpep_pickup_datetime) AS month_ref
        ,CURRENT_TIMESTAMP AS created_at
    FROM {{ source('bronze','raw_yellow_taxi_trip_records') }}
    WHERE 1=1
    AND YEAR(tpep_pickup_datetime) >= 2020
),
stg_green_trips AS (
    SELECT
        vendorid AS vendor_id
        , lpep_pickup_datetime AS pickup_datetime
        , lpep_dropoff_datetime AS dropoff_datetime
        , COALESCE(passenger_count,1) AS passenger_count
        , trip_distance
        , ratecodeid AS ratecode_id
        , pulocationid AS pulocation_id
        , dolocationid AS dolocation_id
        , payment_type AS payment_id
        , 2 AS service_id
        , fare_amount
        , extra
        , mta_tax
        , tip_amount
        , tolls_amount
        , improvement_surcharge
        , total_amount
        , ehail_fee as fee
        , COALESCE(congestion_surcharge,0) AS congestion_surcharge
        , YEAR(lpep_pickup_datetime) as year_ref
        , MONTH(lpep_pickup_datetime) as month_ref
        , CURRENT_TIMESTAMP AS created_at
    FROM {{ source('bronze','raw_green_taxi_trip_records') }}
    WHERE 1=1
    AND YEAR(lpep_pickup_datetime) >= 2020
),
stg_fat_trips AS (
    SELECT *
    FROM stg_yellow_trips
    UNION ALL
    SELECT *
    FROM stg_green_trips
)
SELECT  {{ dbt_utils.surrogate_key(['service_id','vendor_id']) }} as fat_key
        , vendor_id
        , pickup_datetime
        , dropoff_datetime
        , passenger_count
        , trip_distance
        , {{ miles_to_km('trip_distance') }} AS trip_distance_km
        , ratecode_id
        , pulocation_id
        , dolocation_id
        , payment_id
        , service_id
        , fare_amount
        , extra
        , mta_tax
        , tip_amount
        , tolls_amount
        , improvement_surcharge
        , total_amount
        , fee
        , congestion_surcharge
        , year_ref
        , month_ref
        , created_at
FROM stg_fat_trips
