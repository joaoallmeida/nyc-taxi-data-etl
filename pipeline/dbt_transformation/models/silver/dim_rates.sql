WITH stg_ratecode AS (
    SELECT *
    FROM {{ source('bronze','raw_rates') }}
)
SELECT
    id
    ,rate AS rate_name
    ,CURRENT_TIMESTAMP AS created_at
    ,YEAR(CURRENT_TIMESTAMP) AS year_ref
FROM stg_ratecode
