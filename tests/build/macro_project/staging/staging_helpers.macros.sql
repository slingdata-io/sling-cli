{% macro clean_string(column_name) %}
    TRIM(LOWER({{ column_name }}))
{% endmacro %}

{% macro null_if_empty(column_name) %}
    CASE WHEN {{ column_name }} = '' THEN NULL ELSE {{ column_name }} END
{% endmacro %}
