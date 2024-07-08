WITH stg_payment AS (
    SELECT *
    FROM {{ source('bronze','raw_payments') }}
)
SELECT
    id
   ,payment AS payment_name
   ,CURRENT_TIMESTAMP AS created_at
   ,YEAR(CURRENT_TIMESTAMP) AS year_ref
FROM stg_payment
