WITH fat_green_trips AS (
    SELECT * 
    FROM {{ ref('fat_green_trips') }}
),
dim_payments AS (
    SELECT * 
    FROM {{ ref('dim_payments') }}
)
SELECT 
    SUM(total_amount) AS total_amount
    ,payment_name
    ,fat_green_trips.year_ref
FROM fat_green_trips
INNER JOIN dim_payments ON fat_green_trips.payment_id = dim_payments.id
GROUP BY 
    payment_name,
    fat_green_trips.year_ref