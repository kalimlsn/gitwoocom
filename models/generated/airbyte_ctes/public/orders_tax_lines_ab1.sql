{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('orders_scd') }}
{{ unnest_cte(ref('orders_scd'), 'orders', 'tax_lines') }}
select
    _airbyte_orders_hashid,
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['label'], ['label']) }} as {{ adapter.quote('label') }},
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['rate_id'], ['rate_id']) }} as rate_id,
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['compound'], ['compound']) }} as compound,
    {{ json_extract_array(unnested_column_value('tax_lines'), ['meta_data'], ['meta_data']) }} as meta_data,
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['rate_code'], ['rate_code']) }} as rate_code,
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['tax_total'], ['tax_total']) }} as tax_total,
    {{ json_extract_scalar(unnested_column_value('tax_lines'), ['shipping_tax_total'], ['shipping_tax_total']) }} as shipping_tax_total,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_scd') }} as table_alias
-- tax_lines at orders/tax_lines
{{ cross_join_unnest('orders', 'tax_lines') }}
where 1 = 1
and tax_lines is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}
