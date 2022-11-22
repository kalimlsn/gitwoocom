{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_line_items_taxes_ab1') }}
select
    _airbyte_line_items_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast({{ adapter.quote('label') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('label') }},
    cast(rate_id as {{ dbt_utils.type_string() }}) as rate_id,
    {{ cast_to_boolean('compound') }} as compound,
    meta_data,
    cast(rate_code as {{ dbt_utils.type_string() }}) as rate_code,
    cast(tax_total as {{ dbt_utils.type_string() }}) as tax_total,
    cast(shipping_tax_total as {{ dbt_utils.type_string() }}) as shipping_tax_total,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_line_items_taxes_ab1') }}
-- taxes at orders/line_items/taxes
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

