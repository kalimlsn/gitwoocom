{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('customers_ab1') }}
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast({{ adapter.quote('role') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('role') }},
    cast(email as {{ dbt_utils.type_string() }}) as email,
    cast(_links as {{ type_json() }}) as _links,
    cast(billing as {{ type_json() }}) as billing,
    cast(shipping as {{ type_json() }}) as shipping,
    cast(shop_url as {{ dbt_utils.type_string() }}) as shop_url,
    cast(username as {{ dbt_utils.type_string() }}) as username,
    cast(last_name as {{ dbt_utils.type_string() }}) as last_name,
    meta_data,
    cast(avatar_url as {{ dbt_utils.type_string() }}) as avatar_url,
    cast(first_name as {{ dbt_utils.type_string() }}) as first_name,
    cast({{ empty_string_to_null('date_created') }} as {{ type_timestamp_with_timezone() }}) as date_created,
    cast({{ empty_string_to_null('date_modified') }} as {{ type_timestamp_with_timezone() }}) as date_modified,
    cast({{ empty_string_to_null('date_created_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_created_gmt,
    cast({{ empty_string_to_null('date_modified_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_modified_gmt,
    {{ cast_to_boolean('is_paying_customer') }} as is_paying_customer,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('customers_ab1') }}
-- customers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

