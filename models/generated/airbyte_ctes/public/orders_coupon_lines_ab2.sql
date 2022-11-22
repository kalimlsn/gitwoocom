{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_coupon_lines_ab1') }}
select
    _airbyte_orders_hashid,
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(code as {{ dbt_utils.type_string() }}) as code,
    cast(discount as {{ dbt_utils.type_string() }}) as discount,
    meta_data,
    cast(discount_tax as {{ dbt_utils.type_string() }}) as discount_tax,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_coupon_lines_ab1') }}
-- coupon_lines at orders/coupon_lines
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

