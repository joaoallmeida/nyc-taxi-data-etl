WITH stg_vendor AS (
    SELECT *
    FROM {{ source('bronze','raw_vendors') }}
)
SELECT
    id
    ,vendor AS vendor_name
    ,CURRENT_TIMESTAMP AS created_at
    ,YEAR(CURRENT_TIMESTAMP) AS year_ref
FROM stg_vendor
