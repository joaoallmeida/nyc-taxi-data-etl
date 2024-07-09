WITH stg_ratecode AS (
    SELECT DISTINCT ratecode_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(['ratecode_id']) }} as ratecode_key
   , ratecode_id
   , {{ get_descriptions('ratecode', 'ratecode_id') }} as ratecode_desc
   ,CURRENT_TIMESTAMP AS created_at
FROM stg_ratecode
WHERE 1=1
    AND ratecode_id IS NOT NULL
ORDER BY ratecode_id
