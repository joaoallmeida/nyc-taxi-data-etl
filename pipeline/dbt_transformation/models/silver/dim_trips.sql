WITH stg_trips AS (
    SELECT *
    FROM {{ source('bronze','raw_trips') }}
)
SELECT
    id
    ,trip AS trip_name
    ,CURRENT_TIMESTAMP AS created_at
    ,YEAR(CURRENT_TIMESTAMP) AS year_ref
FROM stg_trips
