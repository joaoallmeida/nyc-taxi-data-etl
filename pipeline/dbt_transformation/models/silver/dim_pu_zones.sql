WITH stg_dim_pu_zones AS (
    SELECT DISTINCT pu_location_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    zone_key as pu_zone_key
    , location_id AS pu_location_id
    , borough
    , zone
    , service_zone
    , loaded_at
    , CURRENT_TIMESTAMP AS created_at
FROM {{ ref('stg_taxi_zones') }} A
INNER JOIN stg_dim_pu_zones B ON A.location_id  = B.pu_location_id
ORDER BY pu_location_id