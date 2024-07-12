{% macro get_descriptions(name, value)  %}

    {% if name == 'payment' %}
        CASE
            WHEN {{ value }} = 1 THEN 'CREDIT CARD'
            WHEN {{ value }} = 2 THEN 'CASH'
            WHEN {{ value }} = 3 THEN 'NO CHARGE'
            WHEN {{ value }} = 4 THEN 'DISPUTE'
            WHEN {{ value }} = 5 THEN 'UNKNOWN'
            WHEN {{ value }} = 6 THEN 'VOIDED TRIP'
            ELSE 'N/D'
        END
    {% elif name == 'ratecode' %}
        CASE
            WHEN {{ value }} = 1 THEN 'STANDARD RATE'
            WHEN {{ value }} = 2 THEN 'JFK'
            WHEN {{ value }} = 3 THEN 'NEWARK'
            WHEN {{ value }} = 4 THEN 'NASSAU OR WESTCHESTER'
            WHEN {{ value }} = 5 THEN 'NEGORIATED FARE'
            WHEN {{ value }} = 6 THEN 'GROUP RIDE'
            ELSE 'UNKNOW'
        END
    {% elif name == 'vendor' %}
        CASE
            WHEN {{ value }} = 1 THEN 'CREATIVE MOBILE TECHNOLOGIES, LLC'
            WHEN {{ value }} = 2 THEN 'VERIFONE INC'
            ELSE 'UNKNOW'
        END
    {% elif name == 'service' %}
        CASE
            WHEN {{ value }} = 1 THEN 'YELLOW'
            WHEN {{ value }} = 2 THEN 'GREEN'
        END
    {% endif %}

{% endmacro %}
