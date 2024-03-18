{{
  config(
    options={
      "partition_by":"year_ref",
      "overwrite_or_ignore": True
    },
    format='parquet',
    location='s3://tlc-data-refined/fhv_trips'
  )
}}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number,
    year(pickup_datetime) as year_ref
FROM {{ source('minio','for-hire-vehicle-trip-records') }}
WHERE 1=1
  AND YEAR(pickup_datetime) >= 2021
