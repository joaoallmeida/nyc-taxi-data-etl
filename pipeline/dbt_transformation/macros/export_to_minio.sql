{% macro export_to_minio(schema, table) %}
{% set minioBucket = env_var('MINIO_BUCKET_OUT')  %}
  COPY {{ schema }}.{{ table }}
  TO '{{ minioBucket }}/{{ schema }}/{{ table }}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000, PARTITION_BY (year_ref))
{% endmacro %}
