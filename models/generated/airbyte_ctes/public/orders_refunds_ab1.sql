{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ ref('orders_scd') }}
{{ unnest_cte(ref('orders_scd'), 'orders', 'refunds') }}
select
    _airbyte_orders_hashid,
    {{ json_extract_scalar(unnested_column_value('refunds'), ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar(unnested_column_value('refunds'), ['total'], ['total']) }} as total,
    {{ json_extract_scalar(unnested_column_value('refunds'), ['reason'], ['reason']) }} as reason,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_scd') }} as table_alias
-- refunds at orders/refunds
{{ cross_join_unnest('orders', 'refunds') }}
where 1 = 1
and refunds is not null
{{ incremental_clause('_airbyte_emitted_at', this) }}

