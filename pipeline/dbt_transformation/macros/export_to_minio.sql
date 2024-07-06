{% macro export_to_minio(table) %}
{% set minioPath = env_var('MINIO_PATH_SILVER')  %}
  COPY {{ table }}
  TO '{{ minioPath }}/{{ table }}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000, PARTITION_BY (year_ref))
{% endmacro %}
