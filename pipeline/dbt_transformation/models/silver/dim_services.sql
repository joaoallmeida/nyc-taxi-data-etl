WITH stg_services AS (
    SELECT DISTINCT service_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(['service_id']) }} as service_key
   , service_id
   , {{ get_descriptions('service', 'service_id') }} as service_desc
   ,CURRENT_TIMESTAMP AS created_at
FROM stg_services
