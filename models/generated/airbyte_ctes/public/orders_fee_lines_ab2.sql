{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_fee_lines_ab1') }}
select
    _airbyte_orders_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    taxes,
    cast(total as {{ dbt_utils.type_string() }}) as total,
    meta_data,
    cast(tax_class as {{ dbt_utils.type_string() }}) as tax_class,
    cast(total_tax as {{ dbt_utils.type_string() }}) as total_tax,
    cast(tax_status as {{ dbt_utils.type_string() }}) as tax_status,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_fee_lines_ab1') }}
-- fee_lines at orders/fee_lines
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

