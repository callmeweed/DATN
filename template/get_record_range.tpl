SELECT * FROM {{ table_name }}
WHERE
    indexed_timestamp_ >= '{{ included_min_timestamp }}'
    AND indexed_timestamp_ <= '{{ included_max_timestamp }}'
    {% if target_symbols is not none -%}
    AND {{ symbol_column }} in ('{{ target_symbols | join('\',\'') }}')
    {%- endif -%}
    {% if filter_query is not none -%}
    {{ filter_query }}
    {%- endif -%}
;
