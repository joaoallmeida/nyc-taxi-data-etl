{% macro miles_to_km(col) %}
    CASE
        WHEN {{ col }} < 0 THEN ROUND({{ col }} * 0.1609344, 2)
        WHEN {{ col }} > 0 THEN ROUND({{ col }} * 1.609344, 2)
    END
{% endmacro %}
