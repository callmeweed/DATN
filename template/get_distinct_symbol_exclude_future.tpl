SELECT DISTINCT {{ symbol_column }} FROM {{ table_name }} WHERE LENGTH({{ symbol_column }}) < 4;