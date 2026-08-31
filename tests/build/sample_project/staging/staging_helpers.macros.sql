{% macro clean_string(column_name) %}
    TRIM(LOWER({{ column_name }}))
{% endmacro %}
