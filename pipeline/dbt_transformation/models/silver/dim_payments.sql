WITH stg_payment AS (
    SELECT DISTINCT payment_id
    FROM {{ ref('stg_taxi_trips') }}
    WHERE vendor_id IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(['payment_id']) }} AS payment_key
   ,payment_id
   ,{{ get_descriptions('payment', 'payment_id') }} AS payment_desc
   ,CURRENT_TIMESTAMP AS created_at
FROM stg_payment
WHERE 1=1
    AND payment_id IS NOT NULL
ORDER BY payment_id
