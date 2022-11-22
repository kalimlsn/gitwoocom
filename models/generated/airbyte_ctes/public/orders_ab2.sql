{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('orders_ab1') }}
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(total as {{ dbt_utils.type_string() }}) as total,
    cast(_links as {{ type_json() }}) as _links,
    cast({{ adapter.quote('number') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('number') }},
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(billing as {{ type_json() }}) as billing,
    refunds,
    cast({{ adapter.quote('version') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('version') }},
    cast(cart_tax as {{ dbt_utils.type_string() }}) as cart_tax,
    cast(currency as {{ dbt_utils.type_string() }}) as currency,
    {{ cast_to_boolean('set_paid') }} as set_paid,
    cast(shipping as {{ type_json() }}) as shipping,
    cast(shop_url as {{ dbt_utils.type_string() }}) as shop_url,
    cast(cart_hash as {{ dbt_utils.type_string() }}) as cart_hash,
    cast({{ empty_string_to_null('date_paid') }} as {{ type_timestamp_with_timezone() }}) as date_paid,
    fee_lines,
    meta_data,
    cast(order_key as {{ dbt_utils.type_string() }}) as order_key,
    cast(parent_id as {{ dbt_utils.type_bigint() }}) as parent_id,
    tax_lines,
    cast(total_tax as {{ dbt_utils.type_string() }}) as total_tax,
    line_items,
    cast(created_via as {{ dbt_utils.type_string() }}) as created_via,
    cast(customer_id as {{ dbt_utils.type_bigint() }}) as customer_id,
    coupon_lines,
    cast({{ empty_string_to_null('date_created') }} as {{ type_timestamp_with_timezone() }}) as date_created,
    cast(discount_tax as {{ dbt_utils.type_string() }}) as discount_tax,
    cast(shipping_tax as {{ dbt_utils.type_string() }}) as shipping_tax,
    cast(customer_note as {{ dbt_utils.type_string() }}) as customer_note,
    cast({{ empty_string_to_null('date_modified') }} as {{ type_timestamp_with_timezone() }}) as date_modified,
    cast({{ empty_string_to_null('date_paid_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_paid_gmt,
    cast({{ empty_string_to_null('date_completed') }} as {{ type_timestamp_with_timezone() }}) as date_completed,
    cast(discount_total as {{ dbt_utils.type_string() }}) as discount_total,
    cast(payment_method as {{ dbt_utils.type_string() }}) as payment_method,
    shipping_lines,
    cast(shipping_total as {{ dbt_utils.type_string() }}) as shipping_total,
    cast(transaction_id as {{ dbt_utils.type_string() }}) as transaction_id,
    cast({{ empty_string_to_null('date_created_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_created_gmt,
    cast({{ empty_string_to_null('date_modified_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_modified_gmt,
    cast({{ empty_string_to_null('date_completed_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_completed_gmt,
    {{ cast_to_boolean('prices_include_tax') }} as prices_include_tax,
    cast(customer_ip_address as {{ dbt_utils.type_string() }}) as customer_ip_address,
    cast(customer_user_agent as {{ dbt_utils.type_string() }}) as customer_user_agent,
    cast(payment_method_title as {{ dbt_utils.type_string() }}) as payment_method_title,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('orders_ab1') }}
-- orders
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

