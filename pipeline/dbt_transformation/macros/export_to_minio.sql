{% macro export_to_minio(schema, table, datetime_col) %}
{% set minioBucket = env_var('MINIO_BUCKET_OUT')  %}

  COPY (SELECT *, YEAR({{datetime_col}}) AS year, MONTH({{datetime_col}}) AS month FROM {{ schema }}.{{ table }})
  TO '{{ minioBucket }}/{{ schema }}/{{ table }}/'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000, PARTITION_BY (year, month))

{% endmacro %}
