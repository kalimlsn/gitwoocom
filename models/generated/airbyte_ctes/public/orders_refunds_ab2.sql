{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_refunds_ab1') }}
select
    _airbyte_orders_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(total as {{ dbt_utils.type_string() }}) as total,
    cast(reason as {{ dbt_utils.type_string() }}) as reason,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_refunds_ab1') }}
-- refunds at orders/refunds
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

