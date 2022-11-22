{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('orders_fee_lines') }}
{{ unnest_cte(ref('orders_fee_lines'), 'fee_lines', 'taxes') }}
select
    _airbyte_fee_lines_hashid,
    {{ json_extract_scalar(unnested_column_value('taxes'), ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar(unnested_column_value('taxes'), ['label'], ['label']) }} as {{ adapter.quote('label') }},
    {{ json_extract_scalar(unnested_column_value('taxes'), ['rate_id'], ['rate_id']) }} as rate_id,
    {{ json_extract_scalar(unnested_column_value('taxes'), ['compound'], ['compound']) }} as compound,
    {{ json_extract_array(unnested_column_value('taxes'), ['meta_data'], ['meta_data']) }} as meta_data,
    {{ json_extract_scalar(unnested_column_value('taxes'), ['rate_code'], ['rate_code']) }} as rate_code,
    {{ json_extract_scalar(unnested_column_value('taxes'), ['tax_total'], ['tax_total']) }} as tax_total,
    {{ json_extract_scalar(unnested_column_value('taxes'), ['shipping_tax_total'], ['shipping_tax_total']) }} as shipping_tax_total,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_fee_lines') }} as table_alias
-- taxes at orders/fee_lines/taxes
{{ cross_join_unnest('fee_lines', 'taxes') }}
where 1 = 1
and taxes is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

