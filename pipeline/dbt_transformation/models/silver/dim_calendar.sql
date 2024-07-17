WITH stg_calendar AS (
    SELECT DISTINCT CAST(pickup_datetime AS DATE) AS date_key
    FROM {{ ref('stg_taxi_trips') }}
    WHERE pickup_datetime IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(['date_key']) }} AS date_key
    , date_key AS date
    , YEAR(date_key) AS year
    , QUARTER(date_key) AS quarter
    , MONTH(date_key) AS month
    , MONTHNAME(date_key) AS month_name
    , DAY(date_key) AS day
    , DAYOFWEEK(date_key) AS day_of_week
    , DAYNAME(date_key) AS day_of_week_name
    , CASE WHEN DAYOFWEEK(date_key) IN (6, 7) THEN true ELSE false END AS is_weekend
    , CASE WHEN DAYOFWEEK(date_key) NOT IN (6, 7) THEN true ELSE false END AS is_weekday
    , CURRENT_TIMESTAMP AS created_at
FROM stg_calendar
