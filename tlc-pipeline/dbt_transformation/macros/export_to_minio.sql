{% macro export_to_minio(table) %}
{% set minioPath = 's3://tlc/refined' %}
  COPY {{ table }}
  TO '{{ minioPath }}/{{ table }}'
  (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000, PARTITION_BY (year_ref))
{% endmacro %}
