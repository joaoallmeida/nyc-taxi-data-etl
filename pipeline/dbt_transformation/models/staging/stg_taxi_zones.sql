SELECT
    {{ dbt_utils.surrogate_key(['LocationID','borough']) }} as zone_key
    , LocationID AS location_id
    , Borough AS borough
    , Zone AS zone
    , service_zone
    , CURRENT_TIMESTAMP AS loaded_at
FROM {{ source('bronze','raw_taxi_zones') }}
