--{% macro safe_int(col) %}
    --safe_cast({{ col }} as int64)
--{% endmacro %}

{% macro dedupe(partition_col, order_col) %}
    row_number() over (
        partition by {{ partition_col }}
        order by {{ order_col }} desc
    ) as rn
{% endmacro %}