WITH stg_vendors AS (
    SELECT DISTINCT vendor_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(['vendor_id']) }} as vendor_key
   , vendor_id
   , {{ get_descriptions('vendor', 'vendor_id') }} as vendor_desc
   ,CURRENT_TIMESTAMP AS created_at
FROM stg_vendors
