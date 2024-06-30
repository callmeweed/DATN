SELECT * FROM {{ table_name }}
{% if filter_query is not none -%}
WHERE
{{ filter_query }}
{%- endif -%}
;
