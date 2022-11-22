{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_shipping_lines_ab1') }}
select
    _airbyte_orders_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    taxes,
    cast(total as {{ dbt_utils.type_string() }}) as total,
    meta_data,
    cast(method_id as {{ dbt_utils.type_string() }}) as method_id,
    cast(total_tax as {{ dbt_utils.type_string() }}) as total_tax,
    cast(method_title as {{ dbt_utils.type_string() }}) as method_title,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_shipping_lines_ab1') }}
-- shipping_lines at orders/shipping_lines
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

