WITH stg_dim_do_zones AS (
    SELECT DISTINCT do_location_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    zone_key as do_zone_key
    , location_id as do_location_id
    , borough
    ,  zone
    , service_zone
    , loaded_at
    , CURRENT_TIMESTAMP AS created_at
FROM {{ ref('stg_taxi_zones') }} A
INNER JOIN stg_dim_do_zones B ON A.location_id  = B.do_location_id
ORDER BY do_location_id