{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('orders_scd') }}
{{ unnest_cte(ref('orders_scd'), 'orders', 'coupon_lines') }}
select
    _airbyte_orders_hashid,
    {{ json_extract_scalar(unnested_column_value('coupon_lines'), ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar(unnested_column_value('coupon_lines'), ['code'], ['code']) }} as code,
    {{ json_extract_scalar(unnested_column_value('coupon_lines'), ['discount'], ['discount']) }} as discount,
    {{ json_extract_array(unnested_column_value('coupon_lines'), ['meta_data'], ['meta_data']) }} as meta_data,
    {{ json_extract_scalar(unnested_column_value('coupon_lines'), ['discount_tax'], ['discount_tax']) }} as discount_tax,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_scd') }} as table_alias
-- coupon_lines at orders/coupon_lines
{{ cross_join_unnest('orders', 'coupon_lines') }}
where 1 = 1
and coupon_lines is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

