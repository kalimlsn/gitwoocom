{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_line_items_ab1') }}
select
    _airbyte_orders_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(sku as {{ dbt_utils.type_string() }}) as sku,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast(price as {{ dbt_utils.type_float() }}) as price,
    taxes,
    cast(total as {{ dbt_utils.type_string() }}) as total,
    cast(quantity as {{ dbt_utils.type_float() }}) as quantity,
    cast(subtotal as {{ dbt_utils.type_string() }}) as subtotal,
    meta_data,
    cast(tax_class as {{ dbt_utils.type_string() }}) as tax_class,
    cast(total_tax as {{ dbt_utils.type_string() }}) as total_tax,
    cast(product_id as {{ dbt_utils.type_bigint() }}) as product_id,
    cast(subtotal_tax as {{ dbt_utils.type_string() }}) as subtotal_tax,
    cast(variation_id as {{ dbt_utils.type_bigint() }}) as variation_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_line_items_ab1') }}
-- line_items at orders/line_items
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

