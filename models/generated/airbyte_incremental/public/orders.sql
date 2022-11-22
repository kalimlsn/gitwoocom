{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('orders_scd') }}
select
    _airbyte_unique_key,
    {{ adapter.quote('id') }},
    total,
    _links,
    {{ adapter.quote('number') }},
    status,
    billing,
    refunds,
    {{ adapter.quote('version') }},
    cart_tax,
    currency,
    set_paid,
    shipping,
    shop_url,
    cart_hash,
    date_paid,
    fee_lines,
    meta_data,
    order_key,
    parent_id,
    tax_lines,
    total_tax,
    line_items,
    created_via,
    customer_id,
    coupon_lines,
    date_created,
    discount_tax,
    shipping_tax,
    customer_note,
    date_modified,
    date_paid_gmt,
    date_completed,
    discount_total,
    payment_method,
    shipping_lines,
    shipping_total,
    transaction_id,
    date_created_gmt,
    date_modified_gmt,
    date_completed_gmt,
    prices_include_tax,
    customer_ip_address,
    customer_user_agent,
    payment_method_title,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_orders_hashid
from {{ ref('orders_scd') }}
-- orders from {{ source('public', '_airbyte_raw_orders') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

