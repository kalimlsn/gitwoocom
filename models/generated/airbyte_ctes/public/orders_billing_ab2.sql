{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    schema = "_airbyte_public",
    tags = [ "nested-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_billing_ab1') }}
select
    _airbyte_orders_hashid,
    cast(city as {{ dbt_utils.type_string() }}) as city,
    cast(email as {{ dbt_utils.type_string() }}) as email,
    cast(phone as {{ dbt_utils.type_string() }}) as phone,
    cast({{ adapter.quote('state') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('state') }},
    cast(company as {{ dbt_utils.type_string() }}) as company,
    cast(country as {{ dbt_utils.type_string() }}) as country,
    cast(postcode as {{ dbt_utils.type_string() }}) as postcode,
    cast(address_1 as {{ dbt_utils.type_string() }}) as address_1,
    cast(address_2 as {{ dbt_utils.type_string() }}) as address_2,
    cast(last_name as {{ dbt_utils.type_string() }}) as last_name,
    cast(first_name as {{ dbt_utils.type_string() }}) as first_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_billing_ab1') }}
-- billing at orders/billing
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

